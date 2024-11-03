# A script to migrate bundles from disk storage to Azure storage (UploadBundles, MakeBundles, RunBundles?)

"""
docker exec -ti codalab_rest-server_1 bash
cd /data/codalab0/migration
time python codalab-worksheets/scripts/migrate-disk-to-blob.py -p 4 -i 0 -TUVC
time python codalab-worksheets/scripts/migrate-disk-to-blob.py -p 4 -i 1 -TUVC
time python codalab-worksheets/scripts/migrate-disk-to-blob.py -p 4 -i 2 -TUVC
time python codalab-worksheets/scripts/migrate-disk-to-blob.py -p 4 -i 3 -TUVC

Took ~1 week.
"""

import multiprocessing
from dataclasses import asdict, replace
from functools import partial
import hashlib
import time
from collections import defaultdict
import json
import numpy as np
import traceback
import argparse
import shutil
import os
import signal
import sys
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
from datetime import datetime

import signal

def compute_hash(s: str):
    return int(hashlib.sha256(s.encode()).hexdigest(), 16)

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

@dataclass(unsafe_hash=True)
class MigrationState:
    on_disk: bool
    on_azure: bool
    changed_db: bool
    verified: bool
    success: Optional[bool]
    reason: Optional[str]

class Timer:
    """
    Class that uses signal to interrupt functions while they're running
    if they run for longer than timeout_seconds.
    Can also be used to time how long functions take within its context manager.
    Used for the timing tests.
    """

    def __init__(self, timeout_seconds: int = 1, handle_timeouts: bool = True, uuid: Optional[str] = None):
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

    def __init__(self, migration_states_path, target_store_name, upload, change_db, verify, delete_disk, delete_target, proc_id):
        self.migration_states_path = migration_states_path
        self.target_store_name = target_store_name
        self.upload = upload
        self.change_db = change_db
        self.verify = verify
        self.delete_disk = delete_disk
        self.delete_target = delete_target
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
        tmp_path = self.migration_states_path + ".tmp"
        with open(tmp_path, "w") as f:
            print(json.dumps(dict((uuid, asdict(state)) for uuid, state in self.migration_states.items())), file=f)
        shutil.move(tmp_path, self.migration_states_path)

    def get_bundle_uuids(self, max_bundles):
        bundle_uuids = self.model.get_all_bundle_uuids(max_results=1e9)
        bundle_uuids = sorted(list(set(bundle_uuids)))
        if max_bundles:
            bundle_uuids = bundle_uuids[:max_bundles]
        return bundle_uuids

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

    def upload_to_azure_blob(self, bundle_uuid, disk_location, is_dir=False):
        # generate target bundle path
        target_location = self.blob_target_location(bundle_uuid, is_dir)

        uploader = BlobStorageUploader(
            bundle_model=self.model,
            bundle_store=self.bundle_store,
            destination_bundle_store=self.bundle_store,
            json_api_client=None
        )

        if is_dir:
            source_fileobj = tar_gzip_directory(disk_location, exclude_patterns=None)
            source_ext = ".tar.gz"
            unpack = True
        else:
            # If it's a file, change it into GzipStream
            source_fileobj = open(disk_location, 'rb')
            source_ext = ''
            unpack = False

        size = path_util.get_path_size(disk_location)
        print(f"Uploading {disk_location} to {target_location} (size {size})")

        def callback(bytes_uploaded):
            print(f"\r{bytes_uploaded}/{size} ({round(bytes_uploaded/size*100)}%)", end="")
            sys.stdout.flush()
            return True
        #with Timer(60 * 60 * 100, uuid=bundle_uuid):
        uploader.write_fileobj(source_ext, source_fileobj, target_location, unpack_archive=unpack, progress_callback=callback)
        print("")

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
        except Exception as e:
            return False, f"Unable to read info: {e}"

        def normalize_file_list(files):
            return sorted(f.rstrip('/') for f in files)

        if is_dir:
            try:
                # For dirs, check the folder contains same files
                with OpenFile(target_location, gzipped=True) as f:
                    new_file_list = tarfile.open(fileobj=f, mode='r:gz').getnames()
            except Exception as e:
                return False, f"Unable to read: {e}"

            (dirs, files) = path_util.recursive_ls(disk_location)
            files = [n.replace(disk_location, '.') for n in files]
            dirs = [n.replace(disk_location, '.') for n in dirs]
            old_file_list = files + dirs

            old_file_list = normalize_file_list(old_file_list)
            new_file_list = normalize_file_list(new_file_list)
            if old_file_list != new_file_list:
                old_minus_new = [x for x in old_file_list if x not in new_file_list]
                new_minus_old = [x for x in new_file_list if x not in old_file_list]
                print("OLD - NEW", old_minus_new)
                print("NEW - OLD", new_minus_old)
                return False, f"Directory file lists differ: {len(old_file_list)} versus {len(new_file_list)}"

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
                old_content = read_file_section(disk_location, old_file_size - 10, 10)
                new_content = read_file_section(target_location, old_file_size - 10, 10)
                if old_content != new_content:
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

    def migrate_bundle(self, prefix, bundle_uuid):
        #print(f"migrate_bundle({bundle_uuid})")
        if bundle_uuid in self.migration_states:
            state = self.migration_states[bundle_uuid]
            if state.on_azure and state.changed_db and state.verified and state.success:
                # Already done
                #print(f"{prefix} {bundle_uuid}: {state} [DONE]")
                return
            if not state.on_disk and not state.on_azure:
                # Non-existent bundle, skip
                return

        try:
            # Get the observed state of this bundle
            bundle = self.get_bundle(bundle_uuid)
            bundle_location = self.get_bundle_location(bundle_uuid)

            disk_location = self.get_bundle_disk_location(bundle_uuid)
            on_disk = disk_location is not None

            if on_disk:
                is_dir = os.path.isdir(disk_location)
            else:
                is_dir = "contents.tar.gz" in bundle_location
            is_link = self.is_linked_bundle(bundle_uuid)

            changed_db = bundle_location.startswith(StorageURLScheme.AZURE_BLOB_STORAGE.value)

            target_location = self.blob_target_location(bundle_uuid, is_dir)
            index_location = self.blob_index_location(bundle_uuid)
            on_azure = FileSystems.exists(target_location) and FileSystems.exists(index_location)

            # Create new migration state
            if bundle_uuid in self.migration_states:
                state = self.migration_states[bundle_uuid]
            else:
                state = MigrationState(
                    on_disk=on_disk,
                    on_azure=on_azure,
                    changed_db=changed_db,
                    verified=False,
                    success=None,
                    reason=None,
                )
                self.migration_states[bundle_uuid] = state

            print(f"{now_str()} | {prefix} {bundle_uuid}: {state}")
            #print(f"  {bundle_uuid}: current location {bundle_location}")

            # Make sure we have the latest
            state.on_disk = on_disk
            state.on_azure = on_azure
            state.changed_db = changed_db

            # Upload to Azure
            def do_upload():
                should_upload = self.upload and state.on_disk and not state.on_azure
                if should_upload:
                    print(f"  PERFORM: upload {bundle_uuid}")
                    start_time = time.time()
                    self.adjust_quota_and_upload_to_blob(bundle_uuid, disk_location, is_dir)
                    self.times["upload"].append(time.time() - start_time)
                    state.on_azure = True
                    print(f"  PERFORM: upload {bundle_uuid} => {state}")

            # Verify
            def do_verify():
                should_verify = self.verify and state.on_azure and not state.verified
                if should_verify:
                    print(f"  PERFORM: verify {bundle_uuid}")
                    start_time = time.time()
                    success, reason = self.sanity_check(bundle_uuid, disk_location, is_dir, target_location, index_location)
                    self.times["verify"].append(time.time() - start_time)
                    state.verified = True
                    state.success = success
                    state.reason = reason
                    print(f"  PERFORM: verify {bundle_uuid} => {state}")

            # Change bundle metadata in database to point to Azure
            def do_change_db():
                should_change_db = self.change_db and state.on_azure and not state.changed_db
                if should_change_db:
                    print(f"  PERFORM: change db {bundle_uuid}")
                    start_time = time.time()
                    self.modify_bundle_data(bundle, bundle_uuid, is_dir)
                    self.times["change_db"].append(time.time() - start_time)
                    state.changed_db = True
                    print(f"  PERFORM: change db {bundle_uuid} => {state}")

            # Delete from disk
            def do_delete_disk():
                should_delete_disk = self.delete_disk and state.changed_db and state.on_azure and state.on_disk
                if should_delete_disk:
                    print(f"  PERFORM: delete disk {bundle_uuid}")
                    start_time = time.time()
                    path_util.remove(disk_location)
                    self.times["delete_disk"].append(time.time() - start_time)
                    state.on_disk = False
                    print(f"  PERFORM: delete disk {bundle_uuid} => {state}")

            # Delete from target (to undo failed uploads)
            def do_delete_target():
                should_delete_target = self.delete_target and state.on_azure and state.success == False
                if should_delete_target:
                    print(f"  PERFORM: delete target {bundle_uuid}")
                    start_time = time.time()
                    FileSystems.delete([target_location, index_location])
                    self.times["delete_target"].append(time.time() - start_time)
                    state.on_azure = False
                    state.verified = False
                    state.success = None
                    state.reason = None
                    print(f"  PERFORM: delete target {bundle_uuid} => {state}")

            do_verify()          # See where we are
            do_delete_target()   # Delete failed uploads
            do_upload()          # Upload
            do_verify()          # Check
            do_change_db()       # If good, change db
            do_delete_disk()     # Finally, delete the disk

        except Exception as e:
            print(f"ERROR for {bundle_uuid}: {traceback.format_exc()}")
            state.verified = True
            state.success = False
            state.reason = str(e)

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
        print("TIMES", json.dumps(output_dict, sort_keys=True, indent=2))

    def migrate_bundles(self, bundle_uuids):
        total = len(bundle_uuids)
        self.last_write_time = time.time()
        for i, uuid in enumerate(bundle_uuids):
            self.migrate_bundle(f"{i}/{total}", uuid)
            now = time.time()
            if i == len(bundle_uuids) - 1 or now - self.last_write_time > 20:  # every N seconds
                self.log_times()
                self.write_migration_states()
                self.last_write_time = now


def run_job(target_store_name, upload, change_db, verify, delete_disk, delete_target, bundle_uuids, max_bundles, num_processes, proc_id):
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
        delete_disk=delete_disk,
        delete_target=delete_target,
        proc_id=proc_id,
    )

    # Get all bundle uuids (if not already provided)
    if not bundle_uuids:
        bundle_uuids = migration.get_bundle_uuids(max_bundles=max_bundles)

    # Keep only the ones for this process
    selected_bundle_uuids = [uuid for uuid in bundle_uuids if compute_hash(uuid) % num_processes == proc_id]

    migration.migrate_bundles(selected_bundle_uuids)

    # Print out stats
    state_to_uuids = defaultdict(list)
    for uuid, state in migration.migration_states.items():
        state_to_uuids[replace(state, reason="*")].append(uuid)
    print("Breakdown")
    for state, uuids in state_to_uuids.items():
        print(f"  {state}: {len(uuids)} {' '.join(uuids[:1])}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--max-bundles', type=int, help='Maximum number of bundles to migrate')
    parser.add_argument('-u', '--bundle-uuids', type=str, nargs='*', default=None, help='List of bundle UUIDs to migrate.')
    parser.add_argument('-t', '--target-store-name', type=str, help='The destination bundle store name', default="blob-prod")
    parser.add_argument('-U', '--upload', help='Upload', action='store_true')
    parser.add_argument('-C', '--change-db', help='Change the db', action='store_true')
    parser.add_argument('-V', '--verify', help='Verify contents', action='store_true')
    parser.add_argument('-D', '--delete-disk', help='Delete the disk version', action='store_true')
    parser.add_argument('-T', '--delete-target', help='Delete the target version (to redo)', action='store_true')
    parser.add_argument('-p', '--num-processes', type=int, help="Number of processes for multiprocessing", required=True)
    parser.add_argument('-i', '--proc-id', type=int, help="Which process to run", required=True)
    args = parser.parse_args()

    # Run the program with multiprocessing
    f = partial(
        run_job,
        args.target_store_name,
        args.upload,
        args.change_db,
        args.verify,
        args.delete_disk,
        args.delete_target,
        args.bundle_uuids,
        args.max_bundles,
        args.num_processes,
    )
    if args.proc_id is not None:
        f(args.proc_id)
    else:
        with multiprocessing.Pool(processes=args.num_processes) as pool:
            pool.map(f, list(range(args.num_processes)))
