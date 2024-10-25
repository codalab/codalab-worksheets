# A script to migrate bundles from disk storage to Azure storage (UploadBundles, MakeBundles, RunBundles?)

"""
docker exec -ti codalab_rest-server_1 bash
cd /data/codalab0/migration
python codalab-worksheets/scripts/migrate-disk-to-blob.py
"""

import multiprocessing
from dataclasses import asdict
from functools import partial
import time
from collections import defaultdict
import json
import numpy as np
import traceback
import argparse
import os
import signal
import tarfile
import random
from apache_beam.io.filesystems import FileSystems
from codalab.common import (
    StorageType,
    StorageURLScheme,
)
from codalab.lib.print_util import FileTransferProgress
from codalab.lib import (
    path_util,
)
from codalab.worker.bundle_state import State

from codalab.worker import download_util
from codalab.worker.download_util import BundleTarget, PathException, _compute_target_info_blob
from codalab.lib.upload_manager import BlobStorageUploader
from codalab.lib.codalab_manager import CodaLabManager
from codalab.worker.file_util import (
    OpenFile,
    read_file_section,
    tar_gzip_directory,
)

from enum import Enum

from typing import Optional, List
from dataclasses import dataclass

import signal

@dataclass
class MigrationState:
    on_disk: bool
    on_azure: bool
    changed_db: bool
    verified: bool
    messages: List[str]

class Timer:
    """
    Class that uses signal to interrupt functions while they're running
    if they run for longer than timeout_seconds.
    Can also be used to time how long functions take within its context manager.
    Used for the timing tests.
    """

    def __init__(self, timeout_seconds:int =1, handle_timeouts:bool =True, uuid:Optional[str] =None):
        """
        A class that can be used as a context manager to ensure that code within that context manager times out
        after timeout_seconds time and which times the execution of code within the context manager.
        Parameters:
            timeout_seconds (float): Amount of time before execution in context manager is interrupted for timeout
            handle_timeouts (bool): If True, do not timeout, only return the time taken for execution in context manager.
            uuid (str): Uuid of bundles running within context manager.
        """
        self.handle_timeouts = handle_timeouts
        self.timeout_seconds = timeout_seconds
        self.uuid = uuid

    def handle_timeout(self, signum, frame):
        timeout_message = "Timeout ocurred"
        if self.uuid:
            timeout_message += " while waiting for %s to run" % self.uuid
        raise TimeoutError(timeout_message)

    def time_elapsed(self):
        return time.time() - self.start_time

    def __enter__(self):
        self.start_time = time.time()
        if self.handle_timeouts:
            signal.signal(signal.SIGALRM, self.handle_timeout)
            signal.alarm(self.timeout_seconds)

    def __exit__(self, type, value, traceback):
        self.time_elapsed = time.time() - self.start_time
        if self.handle_timeouts:
            signal.alarm(0)


class Migration:
    """
    Performs the migration.
    """

    def __init__(self, migration_states_path, target_store_name, upload, change_db, verify, delete, proc_id):
        self.migration_states_path = migration_states_path
        self.target_store_name = target_store_name
        self.upload = upload
        self.change_db = change_db
        self.verify = verify
        self.delete = delete
        self.proc_id = proc_id

        self.times = defaultdict(list)

        self.read_migration_states()

        self.codalab_manager = CodaLabManager()
        self.model = self.codalab_manager.model()
        self.bundle_store = self.codalab_manager.bundle_store()
        self.download_manager = self.codalab_manager.download_manager()
        self.upload_manager = self.codalab_manager.upload_manager()

        # Create a root user
        self.root_user_id = self.codalab_manager.root_user_id()  # root_user_id is 0.
        self.target_store = self.model.get_bundle_store(
            user_id=self.root_user_id, name=self.target_store_name
        )

        self.target_store_url = self.target_store['url']
        self.target_store_uuid = self.target_store['uuid']

        assert StorageType.AZURE_BLOB_STORAGE.value == self.target_store['storage_type']
        self.target_store_type = StorageType.AZURE_BLOB_STORAGE

    def read_migration_states(self):
        if not os.path.exists(self.migration_states_path):
            self.migration_states = {}
            return

        with open(self.migration_states_path) as f:
            raw_migration_states = json.load(f)
        self.migration_states = dict((uuid, MigrationState(**raw_state)) for uuid, raw_state in raw_migration_states.items())
        print(f"Read {len(self.migration_states)} migration states from {self.migration_states_path}")

    def write_migration_states(self):
        print(f"Writing {len(self.migration_states)} migration states to {self.migration_states_path}")
        with open(self.migration_states_path, "w") as f:
            print(json.dumps(dict((uuid, asdict(state)) for uuid, state in self.migration_states.items())), file=f)

    def get_bundle_uuids(self, worksheet_uuid, max_bundles):
        if worksheet_uuid is None:
            bundle_uuids = self.model.get_all_bundle_uuids(max_bundless=max_bundles)
        else:
            bundle_uuids = self.model.get_bundle_uuids(
                {'name': None, 'worksheet_uuid': worksheet_uuid, 'user_id': self.root_user_id},
                max_bundless=max_bundles,  # return all bundles in the worksheets
            )
        return list(set(bundle_uuids))

    def is_linked_bundle(self, bundle_uuid):
        bundle_link_url = self.model.get_bundle_metadata(
            [bundle_uuid], "link_url"
        ).get(bundle_uuid)
        if bundle_link_url:
            return True
        return False

    def get_bundle_location(self, bundle_uuid):
        # bundle_locations = self.model.get_bundle_locations(bundle_uuid)  # this is getting locations from database
        bundle_location_obj, bundle_location = self.bundle_store.get_bundle_location_full_info(bundle_uuid)
        bundle_location = self.bundle_store.get_bundle_location(bundle_uuid)
        return bundle_location

    def get_bundle_disk_location(self, bundle_uuid):
        # Get the original bundle location on disk.
        # NOTE: This is hacky, but it appears to work. That super() function
        # is in the _MultiDiskBundleStore class, and it basically searches through
        # all the partitions to find the bundle.
        # However, if it doesn't exist, it just returns a good path to store the bundle
        # at on disk, so we must check the path exists before deleting.
        location = super(
            type(self.bundle_store), self.bundle_store
        ).get_bundle_location(bundle_uuid)
        return location if FileSystems.exists(location) else None

    def get_bundle(self, bundle_uuid):
        return self.model.get_bundle(bundle_uuid)

    def get_bundle_info(self, bundle_uuid, bundle_location):
        target = BundleTarget(bundle_uuid, subpath='')
        try:
            info = download_util.get_target_info(bundle_location, target, depth=0)
        except Exception as e:
            print(f"Error: {str(e)}")
            raise e

        return info

    def blob_target_location(self, bundle_uuid, is_dir):
        file_name = "contents.tar.gz" if is_dir else "contents.gz"
        return f"{self.target_store_url}/{bundle_uuid}/{file_name}"

    def blob_index_location(self, bundle_uuid):
        return f"{self.target_store_url}/{bundle_uuid}/index.sqlite"

    def upload_to_azure_blob(self, bundle_uuid, bundle_location, is_dir=False):
        # generate target bundle path
        target_location = self.blob_target_location(bundle_uuid, is_dir)

        # TODO: delete?
        if FileSystems.exists(target_location):
            path_util.remove(target_location)

        uploader = BlobStorageUploader(
            bundle_model=self.model,
            bundle_store=self.bundle_store,
            destination_bundle_store=self.bundle_store,
            json_api_client=None
        )

        if is_dir:
            source_fileobj = tar_gzip_directory(bundle_location)
            source_ext = ".tar.gz"
            unpack = True
        else:
            # If it's a file, change it into GzipStream
            source_fileobj = open(bundle_location, 'rb')
            source_ext = ''
            unpack = False

        print(
            "Uploading from %s to Azure Blob Storage %s, uploaded file size is %s",
            bundle_location,
            target_location,
            path_util.get_path_size(bundle_location),
        )

        # Upload file content and generate index file
        # NOTE: We added a timeout (using the Timer class) since sometimes bundles just never uploaded
        # SET TO BE VERY AGGRESSIVE TIMEOUT RIGHT NOW so we can get through most bundles pretty quick.
        with Timer(90, uuid=bundle_uuid):
            uploader.write_fileobj(source_ext, source_fileobj, target_location, unpack_archive=unpack)

        assert FileSystems.exists(target_location)
        return target_location

    def modify_bundle_data(self, bundle, bundle_uuid, is_dir):
        """
        Change the bundle location in the database
        ATTENTION: this function will modify codalab
        """
        print(f"Modifying bundle info {bundle_uuid} in database")
        # add bundle location: Add bundle location to database
        self.model.add_bundle_location(bundle_uuid, self.target_store_uuid)

        new_location = self.get_bundle_location(bundle_uuid)
        assert new_location.startswith(StorageURLScheme.AZURE_BLOB_STORAGE.value)

        # Update metadata
        metadata = self.model.get_bundle_metadata(
            uuids=[bundle_uuid], metadata_key={'store'}
        )
        assert metadata.get('store', None) is None

        # storage_type is a legacy field, will still update this field because there is no side effect
        self.model.update_bundle(
            bundle,
            {
                'storage_type': self.target_store_type.value,
                'is_dir': is_dir,
                'metadata': {'store': self.target_store_name},
            },
        )

    def sanity_check(self, bundle_uuid, disk_location, is_dir, target_location, index_location):
        """
        Check whether the disk_location (disk) agrees with target_location (Azure).
        """
        if disk_location is None:
            return True, "No disk"

        # Check index
        try:
            info = _compute_target_info_blob(target_location, depth=10000)
            print(info)
        except Exception as e:
            return False, f"Unable to read info: {e}"

        if is_dir:
            # For dirs, check the folder contains same files
            with OpenFile(target_location, gzipped=True) as f:
                new_file_list = tarfile.open(fileobj=f, mode='r:gz').getnames()
                new_file_list.sort()

            (dirs, files) = path_util.recursive_ls(disk_location)
            files = [n.replace(disk_location, '.') for n in files]
            dirs = [n.replace(disk_location, '.') for n in dirs]
            old_file_list = files + dirs
            old_file_list.sort()
            if old_file_list != new_file_list:
                return False, "Directory file lists differ."

            return True, f"{len(new_file_list)} directories/files match"

        else:
            # For files, check the file has same contents
            old_content = read_file_section(disk_location, 5, 10)
            new_content = read_file_section(target_location, 5, 10)
            if old_content != new_content:
                return False, "First 5 bytes differ."

            old_file_size = path_util.get_path_size(disk_location)
            new_file_size = path_util.get_path_size(target_location)
            if old_file_size != new_file_size:
                return False, "File sizes differ"

            # check file contents of last 10 bytes
            if old_file_size < 10:
                if read_file_section(disk_location, 0, 10) != read_file_section(
                    target_location, 0, 10
                ):
                    return False, "First 10 bytes differ."
            else:
                if read_file_section(disk_location, old_file_size - 10, 10) != read_file_section(
                    target_location, old_file_size - 10, 10
                ):
                    return False, "Last 10 bytes differ."

            return True, "Checked file contents"

    def adjust_quota_and_upload_to_blob(self, bundle_uuid, disk_location, is_dir):
        # Get user info
        bundle_user_id = self.model.get_bundle_owner_ids([bundle_uuid])[bundle_uuid]
        user_info = self.model.get_user_info(bundle_user_id)

        # Update user disk quota, making sure quota doesn't go negative.
        deleted_size = path_util.get_path_size(disk_location)
        decrement = (
            deleted_size if user_info['disk_used'] > deleted_size else user_info['disk_used']
        )
        new_disk_used = user_info['disk_used'] - decrement
        self.model.update_user_info(
            {'user_id': bundle_user_id, 'disk_used': new_disk_used}
        )

        try:
            # If upload successfully, user's disk usage will change when uploading to Azure
            self.upload_to_azure_blob(bundle_uuid, disk_location, is_dir)
        except Exception as e:
            # If upload failed, add user's disk usage back
            user_info = self.model.get_user_info(bundle_user_id)
            new_disk_used = user_info['disk_used'] + decrement
            self.model.update_user_info(
                {'user_id': bundle_user_id, 'disk_used': new_disk_used}
            )
            raise e  # still raise the expcetion to outer try-catch wrapper

    def migrate_bundle(self, bundle_uuid):
        print(f"migrate_bundle({bundle_uuid})")
        try:
            # TEMPORARY: Wrap with timer.
            with Timer(300, uuid=bundle_uuid):
                total_start_time = time.time()

                # Get the observed state of this bundle
                bundle = self.get_bundle(bundle_uuid)
                bundle_location = self.get_bundle_location(bundle_uuid)

                disk_location = self.get_bundle_disk_location(bundle_uuid)
                on_disk = disk_location is not None

                is_dir = os.path.isdir(disk_location) if on_disk else None
                is_link = self.is_linked_bundle(bundle_uuid)

                changed_db = bundle_location.startswith(StorageURLScheme.AZURE_BLOB_STORAGE.value)

                target_location = self.blob_target_location(bundle_uuid, is_dir) if is_dir else None
                index_location = self.blob_index_location(bundle_uuid)
                on_azure = target_location and FileSystems.exists(target_location) and FileSystems.exists(index_location)
                #print("AAAAAAA", target_location, FileSystems.exists(target_location))
                #print("BBBBBBB", index_location, FileSystems.exists(index_location))

                # Create new migration state
                if bundle_uuid in self.migration_states:
                    state = self.migration_states[bundle_uuid]
                else:
                    state = MigrationState(
                        on_disk=on_disk,
                        on_azure=on_azure,
                        changed_db=changed_db,
                        verified=False,
                        messages=[],
                    )
                    self.migration_states[bundle_uuid] = state

                print(f"  {bundle_uuid}: {state}")
                print(f"  {bundle_uuid}: current location {bundle_location}")

                # Sanity check the state
                assert on_disk == state.on_disk
                assert on_azure == state.on_azure
                assert changed_db == state.changed_db

                should_upload = self.upload and state.on_disk and not state.on_azure
                should_verify = self.verify and state.on_azure and not state.verified
                should_change_db = self.change_db and state.on_azure and not state.changed_db
                should_delete = self.delete and state.changed_db and state.on_azure and state.on_disk

                # Upload to Azure
                if should_upload:
                    print(f"  PERFORM: upload {bundle_uuid}")
                    start_time = time.time()
                    self.adjust_quota_and_upload_to_blob(bundle_uuid, bundle_location, is_dir)
                    self.times["upload"].append(time.time() - start_time)
                    state.on_azure = True

                # Change bundle metadata in database to point to Azure
                if should_change_db:
                    print(f"  PERFORM: change db {bundle_uuid}")
                    start_time = time.time()
                    self.modify_bundle_data(bundle, bundle_uuid, is_dir)
                    self.times["change_db"].append(time.time() - start_time)
                    state.changed_db = True

                # Verify
                if should_verify:
                    print(f"  PERFORM: verify {bundle_uuid}")
                    start_time = time.time()
                    success, reason = self.sanity_check(bundle_uuid, disk_location, is_dir, target_location, index_location)
                    self.times["verify"].append(time.time() - start_time)
                    state.verified = success
                    state.messages.append(reason)

                # Delete from disk
                if should_delete:
                    print("  PERFORM: delete")
                    start_time = time.time()
                    path_util.remove(disk_location)
                    self.times["delete"].append(time.time() - start_time)
                    state.on_disk = False

        except Exception as e:
            print(f"Error for {bundle_uuid}: {traceback.format_exc()}")
            self.migration_states[bundle_uuid].messages.append(str(e))

    def log_times(self):
        output_dict = dict()
        for k, v in self.times.items():
            output_dict[k] = {
                "mean": np.mean(v),
                "std": np.std(v),
                "range": np.ptp(v),
                "median": np.median(v),
                "max": np.max(v),
                "min": np.min(v),
            }
        print("TIMES", json.dumps(output_dict, sort_keys=True, indent=4))

    def migrate_bundles(self, bundle_uuids, log_interval=100):
        total = len(bundle_uuids)
        for i, uuid in enumerate(bundle_uuids):
            self.migrate_bundle(uuid)
            if i > 0 and i % log_interval == 0 or i == len(bundle_uuids) - 1:
                self.log_times()
                self.write_migration_states()


def run_job(target_store_name, upload, change_db, verify, delete, bundle_uuids, max_bundles, num_processes, proc_id):
    """
    NOTE: I know this is bad styling since we re-create the Migration object and the
    bundle_uuids in each process. However, we cannot pass the same Migration object in as
    a parameter to the function given to each process by Pool because the Migration object
    is not Pickle-able (indeed, it is not even dill-able) due to one of its member objects
    (BundleManager, CodalabManager, etc.), and so this is the compromise we came up with.
    """
    print(f"[migration] Process {proc_id}/{num_processes}")
    migration_states_path = f"migration_states_{proc_id}_of_{num_processes}.json"
    migration = Migration(
        migration_states_path=migration_states_path,
        target_store_name=target_store_name,
        upload=upload,
        change_db=change_db,
        verify=verify,
        delete=delete,
        proc_id=proc_id,
    )

    # Get all bundle uuids (if not already provided)
    if not bundle_uuids:
        bundle_uuids = sorted(
            migration.get_bundle_uuids(worksheet_uuid=worksheet, max_bundles=max_bundles)
        )

    # Keep only the ones for this process
    selected_bundle_uuids = [uuid for uuid in bundle_uuids if hash(uuid) % num_processes == proc_id]

    migration.migrate_bundles(selected_bundle_uuids)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--max-bundles', type=int, help='Maximum number of bundles to migrate', default=1e9)
    parser.add_argument('-u', '--bundle-uuids', type=str, nargs='*', default=None, help='List of bundle UUIDs to migrate.')
    parser.add_argument('-t', '--target-store-name', type=str, help='The destination bundle store name', default="blob-prod")
    parser.add_argument('-U', '--upload', help='Upload', action='store_true')
    parser.add_argument('-C', '--change-db', help='Change the db', action='store_true')
    parser.add_argument('-V', '--verify', help='Verify contents', action='store_true')
    parser.add_argument('-D', '--delete', help='Delete the original database', action='store_true')
    parser.add_argument('-p', '--num-processes', type=int, help="Number of processes for multiprocessing", default=1)
    args = parser.parse_args()

    # Run the program with multiprocessing
    f = partial(
        run_job,
        args.target_store_name,
        args.upload,
        args.change_db,
        args.verify,
        args.delete,
        args.bundle_uuids,
        args.max_bundles,
        args.num_processes,
    )
    with multiprocessing.Pool(processes=args.num_processes) as pool:
        pool.map(f, list(range(args.num_processes)))
