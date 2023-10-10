# A script to migrate bundles from disk storage to Azure storage (UploadBundles, MakeBundles, RunBundles?)
import multiprocessing
from functools import partial
import time
from typing import Dict, List
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
from codalab.lib import (
    path_util,
    zip_util,
)

from codalab.worker import download_util
from codalab.worker.download_util import BundleTarget, PathException
from codalab.server.bundle_manager import BundleManager
from codalab.lib.upload_manager import BlobStorageUploader
from codalab.lib.codalab_manager import CodaLabManager
from codalab.worker.file_util import (
    OpenFile,
    read_file_section,
)


class Migration:
    """
    Base class for BundleManager tests with a CodaLab Manager uses local database.
    ATTENTION: this class will modify real bundle database.
    """

    def __init__(self, target_store_name, change_db, delete) -> None:
        self.target_store_name = target_store_name
        self.change_db = change_db
        self.delete = delete
        self.skipped_ready = (
            self.skipped_link
        ) = (
            self.skipped_beam
        ) = (
            self.skipped_delete_path_dne
        ) = self.path_exception_cnt = self.error_cnt = self.success_cnt = 0
        self.times: Dict[str, List[float]] = defaultdict(list)

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
            os.path.join(self.codalab_manager.codalab_home, "migration.log")
        )
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger

    def get_bundle_uuids(self, worksheet_uuid, max_result=1e9):
        if worksheet_uuid is None:
            bundle_uuids = self.bundle_manager._model.get_all_bundle_uuids(max_results=max_result)
        else:
            bundle_uuids = self.bundle_manager._model.get_bundle_uuids(
                {'name': None, 'worksheet_uuid': worksheet_uuid, 'user_id': self.root_user_id},
                max_results=None,  # return all bundles in the worksheets
            )
        return bundle_uuids

    def is_linked_bundle(self, bundle_uuid):
        bundle_link_url = self.bundle_manager._model.get_bundle_metadata(
            [bundle_uuid], "link_url"
        ).get(bundle_uuid)
        # logging.info(f"[migration] bundle {bundle_uuid} bundle_link_url: {bundle_link_url}")
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
            logging.info(f"[migration] Error: {str(e)}")
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
            json_api_client=None,
        )

        if is_dir:
            source_fileobj = zip_util.tar_gzip_directory(bundle_location)
            source_ext = ".tar.gz"
            unpack = True
        else:
            # If it's a file, change it into GzipStream
            source_fileobj = open(bundle_location, 'rb')
            source_ext = ''
            unpack = False

        logging.info(
            "[migration] Uploading from %s to Azure Blob Storage %s, uploaded file size is %s",
            bundle_location,
            target_location,
            path_util.get_path_size(bundle_location),
        )
        # Upload file content and generate index file
        uploader.write_fileobj(source_ext, source_fileobj, target_location, unpack_archive=unpack)

        assert FileSystems.exists(target_location)
        return target_location

    def modify_bundle_data(self, bundle, bundle_uuid, is_dir):
        """
        Change the bundle location in the database
        ATTENTION: this function will modify codalab
        """
        logging.info(f"[migration] Modifying bundle info {bundle_uuid} in database")
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
                return False

        else:
            # For files, check the file has same contents
            old_content = read_file_section(bundle_location, 5, 10)
            new_content = read_file_section(new_location, 5, 10)
            if old_content != new_content:
                return False

            old_file_size = path_util.get_path_size(bundle_location)
            new_file_size = path_util.get_path_size(new_location)
            if old_file_size != new_file_size:
                return False

            # check file contents of last 10 bytes
            if old_file_size < 10:
                if read_file_section(bundle_location, 0, 10) != read_file_section(
                    new_location, 0, 10
                ):
                    return False
            else:
                if read_file_section(bundle_location, old_file_size - 10, 10) != read_file_section(
                    new_location, old_file_size - 10, 10
                ):
                    return False
        return True

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
            if bundle.state != 'ready':
                self.skipped_ready += 1
                return

            # Don't migrate linked bundles
            if self.is_linked_bundle(bundle_uuid):
                self.skipped_link += 1
                return

            # Migrate bundle. Only migrate if -c, -d not specifid or sanity check FAILS
            target_location = self.blob_target_location(bundle_uuid, is_dir)
            disk_location = self.get_bundle_disk_location(bundle_uuid)
            if os.path.lexists(disk_location) and (
                not FileSystems.exists(target_location)
                or not self.sanity_check(
                    bundle_uuid, disk_location, bundle_info, is_dir, target_location
                )
            ):
                start_time = time.time()
                self.adjust_quota_and_upload_to_blob(bundle_uuid, bundle_location, is_dir)
                self.times["adjust_quota_and_upload_to_blob"].append(time.time() - start_time)
                if not self.sanity_check(
                    bundle_uuid, bundle_location, bundle_info, is_dir, target_location
                ):
                    raise ValueError("SanityCheck failed")
            self.success_cnt += 1

            # Change bundle metadata to point to the Azure Blob location (not disk)
            if self.change_db:
                start_time = time.time()
                self.modify_bundle_data(bundle, bundle_uuid, is_dir)
                self.times["modify_bundle_data"].append(time.time() - start_time)

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
        except Exception:
            self.error_cnt += 1
            print(traceback.format_exc())

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
        print(json.dumps(output_dict, sort_keys=True, indent=4))

    def migrate_bundles(self, bundle_uuids, log_interval=1000):
        for i, uuid in enumerate(bundle_uuids):
            self.migrate_bundle(uuid)
            if i > 0 and i % log_interval == 0:
                self.print_times()
        self.print_times()


def job(target_store_name, change_db, delete, worksheet, max_result, num_processes, proc_id):
    """A function for running the migration in parallel.

    NOTE: I know this is bad styling since we re-create the Migration object and the
    bundle_uuids in each process. However, we cannot pass the same Migration object in as
    a parameter to the function given to each process by Pool because the Migration object
    is not Pickle-able (indeed, it is not even dill-able) due to one of its member objects
    (BundleManager, CodalabManager, etc.), and so this is the compromise we came up with.
    """
    # Setup Migration.
    migration = Migration(target_store_name, change_db, delete)
    migration.setUp()

    # Get bundle uuids.
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
    total = len(bundle_uuids)
    print(f"[migration] Start migrating {total} bundles")
    migration.migrate_bundles(bundle_uuids)

    print(
        f"[migration] Migration finished, total {total} bundles migrated, "
        f"skipped {migration.skipped_ready}(ready) "
        f"{migration.skipped_link}(linked bundle) "
        f"{migration.skipped_beam}(on Azure) bundles, "
        f"skipped delete due to path DNE {migration.skipped_delete_path_dne}, "
        f"PathException {migration.path_exception_cnt}, "
        f"error {migration.error_cnt} bundles. "
        f"Succeeed {migration.success_cnt} bundles"
    )
    if change_db:
        print(
            "[migration][Change DB] Database migration finished, bundle location changed in database."
        )

    if delete:
        print("[migration][Deleted] Original bundles deleted from local disk.")


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
        '-t',
        '--target_store_name',
        type=str,
        help='The destination bundle store name',
        default="azure-store-default",
    )
    parser.add_argument(
        '-c', '--change_db', help='Change the bundle location in the database', action='store_true',
    )
    parser.add_argument('--disable_logging', help='If set, disable logging', action='store_true')
    parser.add_argument(
        '-p',
        '--num_processes',
        help="Number of processes for multiprocessing",
        default=multiprocessing.cpu_count(),
    )
    parser.add_argument('-d', '--delete', help='Delete the original database', action='store_true')
    args = parser.parse_args()

    # Configure logging
    logging.getLogger().setLevel(logging.INFO)
    if args.disable_logging:
        # Disables logging. Comment out if you want logging
        logging.disable(logging.CRITICAL)

    # Run the program with multiprocessing
    f = partial(
        job,
        args.target_store_name,
        args.change_db,
        args.delete,
        args.worksheet,
        args.max_result,
        args.num_processes,
    )
    with multiprocessing.Pool(processes=args.num_processes) as pool:
        pool.map(f, list(range(args.num_processes)))
