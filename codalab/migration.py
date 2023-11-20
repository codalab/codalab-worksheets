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

docker cp codalab/migration.py codalab_rest-server_1:/opt/codalab-worksheets/codalab/migration.py && time docker exec -it codalab_rest-server_1 /bin/bash -c "python codalab/migration.py -c -t blob-prod -k 1000000000"

docker exec codalab_rest-server_1 rm /opt/codalab-worksheets/migrated-bundles.txt


docker cp codalab_rest-server_1:/opt/codalab-worksheets/migrated-bundles.txt migrated-bundles.txt && cat migrated-bundles.txt 

"""

import multiprocessing
from functools import partial
import time
from collections import defaultdict
import json
import numpy as np
import traceback
import logging
import argparse
import os
import tarfile
from apache_beam.io.filesystems import FileSystems
from codalab.common import (
    StorageType,
    StorageURLScheme,
)
from codalab.lib.print_util import FileTransferProgress
from codalab.lib import (
    path_util,
    zip_util,
)
from codalab.worker.file_util import zip_directory
from codalab.worker.bundle_state import State

from codalab.worker import download_util
from codalab.worker.download_util import BundleTarget, PathException
from codalab.server.bundle_manager import BundleManager
from codalab.lib.upload_manager import BlobStorageUploader
from codalab.lib.codalab_manager import CodaLabManager
from codalab.worker.file_util import (
    OpenFile,
    read_file_section,
)

from scripts.test_util import Timer


class Migration:
    """
    Base class for BundleManager tests with a CodaLab Manager uses local database.
    ATTENTION: this class will modify real bundle database.
    """

    def __init__(self, target_store_name, change_db, delete, proc_id) -> None:
        self.target_store_name = target_store_name
        self.change_db = change_db
        self.delete = delete
        self.skipped_not_final = (
            self.skipped_link
        ) = (
            self.skipped_beam
        ) = (
            self.skipped_delete_path_dne
        ) = self.path_exception_cnt = self.error_cnt = self.success_cnt = 0
        self.times = defaultdict(list)
        self.exc_tracker = dict()
        self.proc_id = proc_id

        self.already_migrated_bundles = {bundle_uuid for bundle_uuid in open('migrated-bundles.txt', 'r').readlines()}

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

    def get_logger(self):
        """
        Create a logger to log the migration process.
        """

        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler(
            os.path.join(self.codalab_manager.codalab_home, f"migration-{self.proc_id}.log")
        )
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
                max_results=None,  # return all bundles in the worksheets
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
        # TODO: why after upload to Azure, this will automatically change to Azure url?
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
            self.logger.info(f"[migration] Error: {str(e)}")
            raise e

        return info

    def blob_target_location(self, bundle_uuid, is_dir=False):
        file_name = "contents.tar.gz" if is_dir else "contents.gz"
        return f"{self.target_store_url}/{bundle_uuid}/{file_name}"

    def upload_to_azure_blob(self, bundle_uuid, bundle_location, is_dir=False):
        # generate target bundle path
        target_location = self.blob_target_location(bundle_uuid, is_dir)

        # TODO: This step might cause repeated upload. Can not check by checking size (Azure blob storage is zipped).
        if FileSystems.exists(target_location):
            path_util.remove(target_location)
        
        uploader = BlobStorageUploader(
            bundle_model=self.bundle_manager._model,
            bundle_store=self.bundle_manager._bundle_store,
            destination_bundle_store=self.bundle_manager._bundle_store,
            json_api_client=None
        )

        if is_dir:
            source_fileobj = zip_directory(bundle_location)
            source_ext = ".zip"
            unpack = True
        else:
            # If it's a file, change it into GzipStream
            source_fileobj = open(bundle_location, 'rb')
            source_ext = ''
            unpack = False

        self.logger.info(
            "[migration] Uploading from %s to Azure Blob Storage %s, uploaded file size is %s",
            bundle_location,
            target_location,
            path_util.get_path_size(bundle_location),
        )
        # Upload file content and generate index file
        # NOTE: We added a timeout (using the Timer class) since sometimes bundles just never uploaded
        with FileTransferProgress(f'\t\tUploading {self.proc_id} ') as progress, Timer(3600):
            uploader.write_fileobj(source_ext, source_fileobj, target_location, unpack_archive=unpack, progress_callback=progress.update)

        assert FileSystems.exists(target_location)
        return target_location

    def modify_bundle_data(self, bundle, bundle_uuid, is_dir):
        """
        Change the bundle location in the database
        ATTENTION: this function will modify codalab
        """
        self.logger.info(f"[migration] Modifying bundle info {bundle_uuid} in database")
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
            # For dirs, check the folder contains same files.
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
            total_start_time = time.time()

            # Get bundle information
            bundle = self.get_bundle(bundle_uuid)
            bundle_location = self.get_bundle_location(bundle_uuid)
            bundle_info = self.get_bundle_info(bundle_uuid, bundle_location)
            is_dir = bundle_info['type'] == 'directory'

            # Don't migrate currently running bundles
            if bundle.state not in State.FINAL_STATES:
                self.skipped_not_final += 1
                return

            # Don't migrate linked bundles
            if self.is_linked_bundle(bundle_uuid):
                self.skipped_link += 1
                return

            # Migrate bundle. Only migrate if -c, -d not specified or sanity check FAILS
            target_location = self.blob_target_location(bundle_uuid, is_dir)
            disk_location = self.get_bundle_disk_location(bundle_uuid)
            ran_sanity_check = False
            if bundle_uuid not in self.already_migrated_bundles and os.path.lexists(disk_location) and (not FileSystems.exists(target_location)):
                start_time = time.time()
                self.adjust_quota_and_upload_to_blob(bundle_uuid, bundle_location, is_dir)
                self.times["adjust_quota_and_upload_to_blob"].append(time.time() - start_time)
                success, reason = self.sanity_check(
                    bundle_uuid, bundle_location, bundle_info, is_dir, target_location
                )
                ran_sanity_check = True
                if not success:
                    raise ValueError(f"SanityCheck failed with {reason}")
            self.success_cnt += 1

            # Change bundle metadata to point to the Azure Blob location (not disk)
            if self.change_db and bundle_uuid not in self.already_migrated_bundles:
                if not ran_sanity_check:
                    success, reason = self.sanity_check(
                        bundle_uuid, bundle_location, bundle_info, is_dir, target_location
                    )
                    ran_sanity_check = True
                    if not success:
                        raise ValueError(f"SanityCheck failed with {reason}")
                start_time = time.time()
                self.modify_bundle_data(bundle, bundle_uuid, is_dir)
                self.times["modify_bundle_data"].append(time.time() - start_time)
                with open('migrated-bundles.txt', 'a') as f:
                    f.write(bundle_uuid + "\n")

            # Delete the bundle from disk.
            if self.delete:
                start_time = time.time()
                deleted = self.delete_original_bundle(bundle_uuid)
                if not deleted:
                    self.skipped_delete_path_dne += 1
                self.times["delete_original_bundle"].append(time.time() - start_time)

            self.times["migrate_bundle"].append(time.time() - total_start_time)
        
        except PathException:
            self.path_exception_cnt += 1
        except Exception as e:
            self.error_cnt += 1
            tb = traceback.format_exc()
            self.logger.error(f"Error for {bundle_uuid}: {tb}")
            if str(e) in self.exc_tracker:
                self.exc_tracker[str(e)]["count"] += 1
                self.exc_tracker[str(e)]["uuid"].append(bundle_uuid)
            else:
                self.exc_tracker[str(e)] = {
                    "uuid": [bundle_uuid],
                    "traceback": tb,
                    "count": 1
                }

    def print_exc_tracker(self):
        self.logger.info(json.dumps(self.exc_tracker, indent=4))

    def print_times(self):
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
    
    def print_other_stats(self, total):
        self.logger.info(
            f"skipped {self.skipped_not_final}(ready) "
            f"{self.skipped_link}(linked bundle) "
            f"{self.skipped_beam}(on Azure) bundles, "
            f"skipped delete due to path DNE {self.skipped_delete_path_dne}, "
            f"PathException {self.path_exception_cnt}, "
            f"error {self.error_cnt} bundles. "
            f"Succeeed {self.success_cnt} bundles "
            f"Total: {total}"
        )

    def migrate_bundles(self, bundle_uuids, log_interval=100):
        total = len(bundle_uuids)
        for i, uuid in enumerate(bundle_uuids):
            self.migrate_bundle(uuid)
            self.logger.info("[migration] [process %d], status: %d / %d", self.proc_id, i, total)
            if i > 0 and i % log_interval == 0:
                self.print_exc_tracker()
                self.print_times()
                self.print_other_stats(len(bundle_uuids))
        self.print_exc_tracker()
        self.print_times()
        self.print_other_stats(len(bundle_uuids))


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
    migration.setUp()

    # Get bundle uuids (if not already provided)
    if not bundle_uuids:
        bundle_uuids = sorted(
            migration.get_bundle_uuids(worksheet_uuid=worksheet, max_result=max_result)
        )

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