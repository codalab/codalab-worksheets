# A script to migrate bundles from disk storage to Azure storage (UploadBundles, MakeBundles, RunBundles?)

"""
python migration.py -c -t blob-prod
python migration.py -c -t blob-prod --disable_logging
python migration.py -c -t blob-prod --disable_logging -p 5

To run this on prod:
cd codalab-worksheets
wget https://raw.githubusercontent.com/codalab/codalab-worksheets/new-migration/codalab/migration.py -O codalab/migration.py
vim codalab/migration.py
docker cp codalab/migration.py codalab_rest-server_1:/opt/codalab-worksheets/codalab/migration.py && time docker exec -it codalab_rest-server_1 /bin/bash -c "python codalab/migration.py -t blob-prod"

docker cp codalab/migration.py codalab_rest-server_1:/opt/codalab-worksheets/codalab/migration.py && time docker exec -it codalab_rest-server_1 /bin/bash -c "python codalab/migration.py -c -t blob-prod"

docker exec codalab_rest-server_1 rm /opt/codalab-worksheets/migrated-bundles.txt


docker cp codalab_rest-server_1:/opt/codalab-worksheets/migrated-bundles.txt migrated-bundles.txt && cat migrated-bundles.txt 

"""

import multiprocessing
from functools import partial
import time
from collections import defaultdict
import json
import numpy as np
import pandas as pd
import traceback
import logging
import argparse
import os
import signal
import tarfile
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
from codalab.worker.download_util import BundleTarget, PathException
from codalab.server.bundle_manager import BundleManager
from codalab.lib.upload_manager import BlobStorageUploader
from codalab.lib.codalab_manager import CodaLabManager
from codalab.worker.file_util import (
    OpenFile,
    read_file_section,
    tar_gzip_directory,
)

from enum import Enum

from typing import Optional
from dataclasses import dataclass

import signal

class MigrationStatus(str, Enum):
    """An enum for tracking the migration status of bundles.

    """
    NOT_STARTED = "NOT_STARTED"
    UPLOADED_TO_AZURE = "UPLOADED_TO_AZURE"
    CHANGED_DB = "CHANGED_DB"
    FINISHED = "FINISHED"  # Meaning it is uploaded to Azure, DB updated, and deleted from disk.
    
    ERROR = "ERROR"
    SKIPPED_NOT_FINAL = "SKIPPED_NOT_FINAL"
    SKIPPED_LINKED = "SKIPPED_LINKED"


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



@dataclass
class BundleMigrationStatus:
    """Class for keeping track of an item in inventory."""
    uuid: str
    status: MigrationStatus = MigrationStatus.NOT_STARTED
    error_message: Optional[str] = None

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "status": self.status,
            "error_message": self.error_message
        }
    
    def uploaded_to_azure(self):
        return self.status == MigrationStatus.UPLOADED_TO_AZURE or self.status == MigrationStatus.CHANGED_DB or self.status == MigrationStatus.FINISHED

    def changed_db(self):
        return self.status == MigrationStatus.CHANGED_DB or self.status == MigrationStatus.FINISHED

    def finished(self):
        return self.status == MigrationStatus.FINISHED

class Migration:
    """
    Base class for BundleManager tests with a CodaLab Manager uses local database.
    ATTENTION: this class will modify real bundle database.
    """

    def __init__(self, target_store_name, change_db, delete, proc_id) -> None:
        self.target_store_name = target_store_name
        self.change_db = change_db
        self.delete = delete
        self.times = defaultdict(list)
        self.proc_id = proc_id

        self.setUp()

        self.bundle_migration_statuses = list()

        if os.path.exists(self.get_bundle_statuses_path()):
            self.existing_bundle_migration_statuses = pd.read_csv(self.get_bundle_statuses_path())
        else:
            self.existing_bundle_migration_statuses = None

    def setUp(self):
        self.codalab_manager = CodaLabManager()
        # self.codalab_manager.config['server']['class'] = 'SQLiteModel'
        self.bundle_manager = BundleManager(self.codalab_manager)
        self.download_manager = self.codalab_manager.download_manager()
        self.upload_manager = self.codalab_manager.upload_manager()

        # Create a root user
        self.root_user_id = self.codalab_manager.root_user_id()  # root_user_id is 0.

        self.target_store = self.bundle_manager._model.get_bundle_store(
            user_id=self.root_user_id, name=self.target_store_name
        )

        self.target_store_url = self.target_store['url']
        self.target_store_uuid = self.target_store['uuid']

        assert StorageType.AZURE_BLOB_STORAGE.value == self.target_store['storage_type']
        self.target_store_type = StorageType.AZURE_BLOB_STORAGE

        # This file is used to log those bundles's location that has been changed in database.
        self.logger = self.get_logger()
    
    def get_log_file_path(self):
        return os.path.join(self.codalab_manager.codalab_home, f"migration-{self.proc_id}.log")
    def get_bundle_statuses_path(self):
        return os.path.join(self.codalab_manager.codalab_home, f'bundle_statuses_proc_{self.proc_id}.csv')
    def get_bundle_ids_path(self):
        return os.path.join(self.codalab_manager.codalab_home, f'bundle_ids_{self.proc_id}.csv')


    def get_logger(self):
        """
        Create a logger to log the migration process.
        """

        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler(self.get_log_file_path())
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(f'%(asctime)s - %(name)s - %(levelname)s - [migration] [{self.proc_id}] %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        logger.propagate = False

        return logger

    def get_bundle_uuids(self, worksheet_uuid, max_result):
        if worksheet_uuid is None:
            bundle_uuids = self.bundle_manager._model.get_all_bundle_uuids(max_results=max_result)
        else:
            bundle_uuids = self.bundle_manager._model.get_bundle_uuids(
                {'name': None, 'worksheet_uuid': worksheet_uuid, 'user_id': self.root_user_id},
                max_results=max_result,  # return all bundles in the worksheets
            )
        return list(set(bundle_uuids))

    def is_linked_bundle(self, bundle_uuid):
        bundle_link_url = self.bundle_manager._model.get_bundle_metadata(
            [bundle_uuid], "link_url"
        ).get(bundle_uuid)
        if bundle_link_url:
            return True
        return False

    def get_bundle_location(self, bundle_uuid):
        # bundle_locations = self.bundle_manager._model.get_bundle_locations(bundle_uuid)  # this is getting locations from database
        bundle_location = self.bundle_manager._bundle_store.get_bundle_location(bundle_uuid)
        return bundle_location

    def get_bundle_disk_location(self, bundle_uuid):
        # Get the original bundle location on disk.
        # NOTE: This is hacky, but it appears to work. That super() function
        # is in the _MultiDiskBundleStore class, and it basically searches through
        # all the partitions to find the bundle.
        # However, if it doesn't exist, it just returns a good path to store the bundle
        # at on disk, so we must check the path exists before deleting.
        return super(
            type(self.bundle_manager._bundle_store), self.bundle_manager._bundle_store
        ).get_bundle_location(bundle_uuid)

    def get_bundle(self, bundle_uuid):
        return self.bundle_manager._model.get_bundle(bundle_uuid)

    def get_bundle_info(self, bundle_uuid, bundle_location):
        target = BundleTarget(bundle_uuid, subpath='')
        try:
            info = download_util.get_target_info(bundle_location, target, depth=0)
        except Exception as e:
            self.logger.info(f"Error: {str(e)}")
            raise e

        return info

    def blob_target_location(self, bundle_uuid, is_dir=False):
        file_name = "contents.tar.gz" if is_dir else "contents.gz"
        return f"{self.target_store_url}/{bundle_uuid}/{file_name}"

    def upload_to_azure_blob(self, bundle_uuid, bundle_location, is_dir=False):
        # generate target bundle path
        target_location = self.blob_target_location(bundle_uuid, is_dir)

        if FileSystems.exists(target_location):
            path_util.remove(target_location)
        
        uploader = BlobStorageUploader(
            bundle_model=self.bundle_manager._model,
            bundle_store=self.bundle_manager._bundle_store,
            destination_bundle_store=self.bundle_manager._bundle_store,
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

        self.logger.info(
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
        self.logger.info(f"Modifying bundle info {bundle_uuid} in database")
        # add bundle location: Add bundle location to database
        self.bundle_manager._model.add_bundle_location(bundle_uuid, self.target_store_uuid)

        new_location = self.get_bundle_location(bundle_uuid)
        assert new_location.startswith(StorageURLScheme.AZURE_BLOB_STORAGE.value)

        # Update metadata
        metadata = self.bundle_manager._model.get_bundle_metadata(
            uuids=[bundle_uuid], metadata_key={'store'}
        )
        assert metadata.get('store', None) is None

        # storage_type is a legacy field, will still update this field because there is no side effect
        self.bundle_manager._model.update_bundle(
            bundle,
            {
                'storage_type': self.target_store_type.value,
                'is_dir': is_dir,
                'metadata': {'store': self.target_store_name},
            },
        )

    def sanity_check(self, bundle_uuid, bundle_location, bundle_info, is_dir, new_location=None):
        if new_location is None:
            new_location = self.get_bundle_location(bundle_uuid)
        if is_dir:
            # For dirs, check the folder contains same files
            with OpenFile(new_location, gzipped=True) as f:
                new_file_list = tarfile.open(fileobj=f, mode='r:gz').getnames()
                new_file_list.sort()

            (files, dirs) = path_util.recursive_ls(bundle_location)
            old_file_list = files + dirs
            old_file_list = [n.replace(bundle_location, '.') for n in old_file_list]
            old_file_list.sort()
            if old_file_list != new_file_list:
                return False, "Directory file lists differ."

        else:
            # For files, check the file has same contents
            old_content = read_file_section(bundle_location, 5, 10)
            new_content = read_file_section(new_location, 5, 10)
            if old_content != new_content:
                return False, "First 5 bytes differ."

            old_file_size = path_util.get_path_size(bundle_location)
            new_file_size = path_util.get_path_size(new_location)
            if old_file_size != new_file_size:
                return False, "File sizes differ"

            # check file contents of last 10 bytes
            if old_file_size < 10:
                if read_file_section(bundle_location, 0, 10) != read_file_section(
                    new_location, 0, 10
                ):
                    return False, "First 10 bytes differ."
            else:
                if read_file_section(bundle_location, old_file_size - 10, 10) != read_file_section(
                    new_location, old_file_size - 10, 10
                ):
                    return False, "Last 10 bytes differ."
        return True, ""

    def delete_original_bundle(self, uuid):
        # Get the original bundle location.
        disk_bundle_location = self.get_bundle_disk_location(uuid)
        if not os.path.lexists(disk_bundle_location):
            return False

        # Now, delete the bundle.
        path_util.remove(disk_bundle_location)

    def adjust_quota_and_upload_to_blob(self, bundle_uuid, bundle_location, is_dir):
        # Get user info
        bundle_user_id = self.bundle_manager._model.get_bundle_owner_ids([bundle_uuid])[bundle_uuid]
        user_info = self.bundle_manager._model.get_user_info(bundle_user_id)

        # Update user disk quota, making sure quota doesn't go negative.
        deleted_size = path_util.get_path_size(bundle_location)
        decrement = (
            deleted_size if user_info['disk_used'] > deleted_size else user_info['disk_used']
        )
        new_disk_used = user_info['disk_used'] - decrement
        self.bundle_manager._model.update_user_info(
            {'user_id': bundle_user_id, 'disk_used': new_disk_used}
        )

        try:
            # If upload successfully, user's disk usage will change when uploading to Azure
            self.upload_to_azure_blob(bundle_uuid, bundle_location, is_dir)
        except Exception as e:
            # If upload failed, add user's disk usage back
            user_info = self.bundle_manager._model.get_user_info(bundle_user_id)
            new_disk_used = user_info['disk_used'] + decrement
            self.bundle_manager._model.update_user_info(
                {'user_id': bundle_user_id, 'disk_used': new_disk_used}
            )
            raise e  # still raise the expcetion to outer try-catch wrapper

    def migrate_bundle(self, bundle_uuid):
        try:
            # TEMPORARY: Wrap with timer.
            with Timer(300, uuid=bundle_uuid):
                total_start_time = time.time()

                # Create bundle migration status
                self.logger.info("Getting Bundle Migration Status")
                bundle_migration_status = BundleMigrationStatus(uuid=bundle_uuid)
                if self.existing_bundle_migration_statuses is not None: 
                    existing_bundle_migration_status = self.existing_bundle_migration_statuses[
                        self.existing_bundle_migration_statuses["uuid"] == bundle_uuid
                    ].to_dict('records')
                    if existing_bundle_migration_status:
                        bundle_migration_status = BundleMigrationStatus(**existing_bundle_migration_status[0])

                # Get bundle information
                self.logger.info("Getting Bundle info")
                bundle = self.get_bundle(bundle_uuid)
                bundle_location = self.get_bundle_location(bundle_uuid)
                bundle_info = self.get_bundle_info(bundle_uuid, bundle_location)
                is_dir = bundle_info['type'] == 'directory'
                target_location = self.blob_target_location(bundle_uuid, is_dir)
                disk_location = self.get_bundle_disk_location(bundle_uuid)

                # Don't migrate currently running bundles
                if bundle.state not in State.FINAL_STATES:
                    bundle_migration_status.status = MigrationStatus.SKIPPED_NOT_FINAL
                    return

                # Don't migrate linked bundles
                if self.is_linked_bundle(bundle_uuid):
                    bundle_migration_status.status = MigrationStatus.SKIPPED_LINKED
                    return

                # if db already changed
                # TODO: Check if bundle_location is azure (see other places in code base.)
                if bundle_migration_status.status == MigrationStatus.FINISHED:
                    return
                elif bundle_migration_status.changed_db() or bundle_location.startswith(StorageURLScheme.AZURE_BLOB_STORAGE.value):
                    bundle_migration_status.status = MigrationStatus.CHANGED_DB
                elif bundle_migration_status.uploaded_to_azure() or (FileSystems.exists(target_location) and self.sanity_check(
                        bundle_uuid, bundle_location, bundle_info, is_dir, target_location
                    )[0]):
                    bundle_migration_status.status = MigrationStatus.UPLOADED_TO_AZURE

                # Upload to Azure.
                if not bundle_migration_status.uploaded_to_azure() and os.path.lexists(disk_location):
                    self.logger.info("Uploading to Azure")
                    start_time = time.time()
                    self.adjust_quota_and_upload_to_blob(bundle_uuid, bundle_location, is_dir)
                    self.times["adjust_quota_and_upload_to_blob"].append(time.time() - start_time)
                    success, reason = self.sanity_check(
                        bundle_uuid, bundle_location, bundle_info, is_dir, target_location
                    )
                    if not success:
                        raise ValueError(f"SanityCheck failed with {reason}")
                    bundle_migration_status.status = MigrationStatus.UPLOADED_TO_AZURE
                    bundle_migration_status.error_message = None

                # Change bundle metadata in database to point to the Azure Blob location (not disk)
                if self.change_db and not bundle_migration_status.changed_db():
                    self.logger.info("Changing DB")
                    start_time = time.time()
                    self.modify_bundle_data(bundle, bundle_uuid, is_dir)
                    self.times["modify_bundle_data"].append(time.time() - start_time)
                    bundle_migration_status.status = MigrationStatus.CHANGED_DB

                # Delete the bundle from disk.
                if self.delete:
                    self.logger.info("Deleting from disk")
                    start_time = time.time()
                    if os.path.lexists(disk_location):
                        # Delete it.
                        path_util.remove(disk_bundle_location)
                    self.times["delete_original_bundle"].append(time.time() - start_time)
                    bundle_migration_status.status = MigrationStatus.FINISHED

                self.times["migrate_bundle"].append(time.time() - total_start_time)
        
        except Exception as e:
            self.logger.error(f"Error for {bundle_uuid}: {traceback.format_exc()}")
            bundle_migration_status.error_message = str(e)
            bundle_migration_status.status = MigrationStatus.ERROR
        
        finally:
            self.bundle_migration_statuses.append(bundle_migration_status)

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
        self.logger.info(json.dumps(output_dict, sort_keys=True, indent=4))
    
    def write_bundle_statuses(self):
        new_records = pd.DataFrame.from_records([b_m_s.to_dict() for b_m_s in self.bundle_migration_statuses])
        if self.existing_bundle_migration_statuses is None:
            self.existing_bundle_migration_statuses = new_records
        else:
            self.existing_bundle_migration_statuses = self.existing_bundle_migration_statuses.merge(new_records, how='outer')
            self.existing_bundle_migration_statuses = self.existing_bundle_migration_statuses.drop_duplicates('uuid', keep='last')
        self.existing_bundle_migration_statuses.to_csv(self.get_bundle_statuses_path(), index=False, mode='w')
        self.bundle_migration_statuses = list()

    def migrate_bundles(self, bundle_uuids, log_interval=100):
        total = len(bundle_uuids)
        for i, uuid in enumerate(bundle_uuids):
            self.logger.info(f"migrating {uuid}")
            self.migrate_bundle(uuid)
            self.logger.info("status: %d / %d", i, total)
            if i > 0 and i % log_interval == 0 or i == len(bundle_uuids) - 1:
                self.log_times()
                self.write_bundle_statuses()


def job(target_store_name, change_db, delete, worksheet, bundle_uuids, max_result, num_processes, proc_id):
    """A function for running the migration in parallel.

    NOTE: I know this is bad styling since we re-create the Migration object and the
    bundle_uuids in each process. However, we cannot pass the same Migration object in as
    a parameter to the function given to each process by Pool because the Migration object
    is not Pickle-able (indeed, it is not even dill-able) due to one of its member objects
    (BundleManager, CodalabManager, etc.), and so this is the compromise we came up with.
    """
    # Setup Migration.
    migration = Migration(target_store_name, change_db, delete, proc_id)

    # Get bundle uuids (if not already provided)
    if not bundle_uuids:
        bundle_uuids = sorted(
            migration.get_bundle_uuids(worksheet_uuid=worksheet, max_result=max_result)
        )
        bundle_uuids_df = pd.DataFrame(bundle_uuids)
        bundle_uuids_df.to_csv(migration.get_bundle_ids_path(), index=False, mode='w')
        print(f"[migration] Recorded all bundle ids to be migrated")

    # Sort according to what process you are.
    chunk_size = len(bundle_uuids) // num_processes
    start_idx = chunk_size * proc_id
    end_idx = len(bundle_uuids) if proc_id == num_processes - 1 else chunk_size * (proc_id + 1)
    print(f"[migration] ProcessID{proc_id}\tChunk: {chunk_size}\tstart:{start_idx}\tend:{end_idx}")
    bundle_uuids = bundle_uuids[start_idx:end_idx]

    # Do the migration.
    print(f"[migration] Start migrating {len(bundle_uuids)} bundles")
    migration.migrate_bundles(bundle_uuids)
    print(f"[migration] Finish migrating {len(bundle_uuids)} bundles")


if __name__ == '__main__':
    # Command line args.
    parser = argparse.ArgumentParser(
        description='Manages your local CodaLab Worksheets service deployment'
    )
    parser.add_argument(
        '-k', '--max-result', type=int, help='The worksheet uuid that needs migration', default=1e9
    )
    parser.add_argument(
        '-w', '--worksheet', type=str, help='The worksheet uuid that needs migration'
    )
    parser.add_argument(
        '-u', '--bundle-uuids', type=str, nargs='*', default=None, help='List of bundle UUIDs to migrate.'
    )
    parser.add_argument(
        '-t',
        '--target_store_name',
        type=str,
        help='The destination bundle store name',
        default="azure-store-default",
    )
    parser.add_argument(
        '-c', '--change_db', help='Change the bundle location in the database', action='store_true',
    )
    parser.add_argument(
        '-p',
        '--num_processes',
        type=int,
        help="Number of processes for multiprocessing",
        default=multiprocessing.cpu_count(),
    )
    parser.add_argument('-d', '--delete', help='Delete the original database', action='store_true')
    args = parser.parse_args()

    # Run the program with multiprocessing
    f = partial(
        job,
        args.target_store_name,
        args.change_db,
        args.delete,
        args.worksheet,
        args.bundle_uuids,
        args.max_result,
        args.num_processes,
    )
    with multiprocessing.Pool(processes=args.num_processes) as pool:
        pool.map(f, list(range(args.num_processes)))
