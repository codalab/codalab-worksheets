# A script to migrate bundles from disk storage to Azure storage (UploadBundles, MakeBundles, RunBundles?)
from multiprocessing import Pool
from functools import partial
import logging
import argparse
import os
import tarfile
from apache_beam.io.filesystems import FileSystems
from codalab.common import (
    StorageType,
    StorageURLScheme,
    parse_linked_bundle_url,
)
from codalab.lib import (
    path_util,
    spec_util,
    zip_util,
)

from codalab.worker import download_util
from codalab.worker.download_util import BundleTarget
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

        self.skipped_ready = self.skipped_link = self.skipped_beam = self.skipped_delete_path_dne = self.error_cnt = self.success_cnt = 0

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

        # Disables logging. Comment out if you want logging
        logging.disable(logging.CRITICAL)

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

    def upload_to_azure_blob(self, bundle_uuid, bundle_location, is_dir=False):
        # generate target bundle path
        file_name = "contents.tar.gz" if is_dir else "contents.gz"
        target_location = f"{self.target_store_url}/{bundle_uuid}/{file_name}"

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
            path_util.get_path_size(bundle_location)
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

        original_location = self.get_bundle_location(bundle_uuid)

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
            assert old_file_list == new_file_list

        else:
            # For files, check the file has same contents
            old_content = read_file_section(bundle_location, 5, 10)
            new_content = read_file_section(new_location, 5, 10)
            assert old_content == new_content

            old_file_size = path_util.get_path_size(bundle_location)
            new_file_size = path_util.get_path_size(new_location)
            assert old_file_size == new_file_size

            # check file contents of last 10 bytes
            if old_file_size < 10:
                assert read_file_section(bundle_location, 0, 10) == read_file_section(
                    new_location, 0, 10
                )
            else:
                assert read_file_section(
                    bundle_location, old_file_size - 10, 10
                ) == read_file_section(new_location, old_file_size - 10, 10)

    def delete_original_bundle(self, uuid):
        # Get the original bundle location.
        # NOTE: This is hacky, but it appears to work. That super() function
        # is in the _MultiDiskBundleStore class, and it basically searches through
        # all the partitions to find the bundle.
        # However, if it doesn't exist, it just returns a good path to store the bundle
        # at on disk, so we must check the path exists before deleting.
        disk_bundle_location = super(type(self.bundle_manager._bundle_store), self.bundle_manager._bundle_store).get_bundle_location(uuid)
        if not os.path.lexists(disk_bundle_location): return False

        # Now, delete the bundle.
        deleted_size = path_util.get_path_size(disk_bundle_location)
        bundle_user_id = self.bundle_manager._model.get_bundle_owner_ids([bundle_uuid])[
            bundle_uuid
        ]
        path_util.remove(disk_bundle_location)
        # update user's disk usage: reduce original bundle size
        user_info = self.bundle_manager._model.get_user_info(bundle_user_id)
        assert user_info['disk_used'] >= deleted_size
        new_disk_used = user_info['disk_used'] - deleted_size
        self.bundle_manager._model.update_user_info(
            {'user_id': bundle_user_id, 'disk_used': new_disk_used}
            )
        return True

    def migrate_bundle(args, bundle_uuid):
        try:
            bundle = migration.get_bundle(bundle_uuid)

            # TODO: change this to allow migration of run bundles
            if bundle.state != 'ready':
                # only migrate uploaded bundle, and the bundle state needs to be ready
                self.skipped_ready += 1
                continue

            # Uploaded bundles does not need has dependencies
            # logging.info(bundle.dependencies)
            # assert len(bundle.dependencies) == 0

            if migration.is_linked_bundle(bundle_uuid):
                # Do not migrate link bundle
                self.skipped_link += 1
                continue

            # bundle_location is the original bundle location
            bundle_location = migration.get_bundle_location(bundle_uuid)

            # Get bundle info
            bundle_info = migration.get_bundle_info(bundle_uuid, bundle_location)

            is_dir = bundle_info['type'] == 'directory'

            if parse_linked_bundle_url(bundle_location).uses_beam:
                self.skipped_beam += 1
            else:
                new_location = migration.upload_to_azure_blob(bundle_uuid, bundle_location, is_dir)
                self.success_cnt += 1
                migration.sanity_check(bundle_uuid, bundle_location, bundle_info, is_dir, new_location)

                if self.change_db:  # If need to change the database, continue to run
                    migration.modify_bundle_data(bundle, bundle_uuid, is_dir)

            if self.delete:
                deleted = migration.delete_original_bundle(bundle_uuid)
                if not deleted:
                    self.skipped_delete_path_dne += 1
        except Exception as e:
            self.error_cnt += 1
            print("Exception: {e}")

if __name__ == '__main__':
    # Command line parser, parse the worksheet id
    parser = argparse.ArgumentParser(
        description='Manages your local CodaLab Worksheets service deployment'
    )
    parser.add_argument(
        '-a', '--all', help='Run migration on all worksheets and all bundles', action='store_true',
    )
    parser.add_argument(
        '-k', '--max-result', type=int, help='The worksheet uuid that needs migration', default=1e9
    )
    parser.add_argument(
        '-w', '--worksheet', type=str, help='The worksheet uuid that needs migration'
    )
    parser.add_argument(
        '-t', '--target_store_name', type=str, help='The destination bundle store name', default = "azure-store-default"
    )
    parser.add_argument(
        '-c', '--change_db', help='Change the bundle location in the database', action='store_true',
    )
    parser.add_argument(
        '-p', '--num_processes', help='Number of multiprocessing pool to do', action='store_true', default = 10
    )
    parser.add_argument('-d', '--delete', help='Delete the original database', action='store_true')

    args = parser.parse_args()

    worksheet_uuid = args.worksheet

    migration = Migration(args.target_store_name, args.change_db, args.delete)
    migration.setUp()

    logging.getLogger().setLevel(logging.INFO)

    if args.all:
        bundle_uuids = migration.get_bundle_uuids(worksheet_uuid=None, max_result=args.max_result)
    else:
        # Must specify worksheet uuid
        if worksheet_uuid is not None and not spec_util.UUID_REGEX.match(worksheet_uuid):
            raise Exception("Input worksheet uuid has wrong format. ")
        bundle_uuids = migration.get_bundle_uuids(worksheet_uuid, max_result=args.max_result)

    total = len(bundle_uuids)
    logging.info(f"[migration] Start migrating {total} bundles")
    with Pool(processes=args.num_processes):
        pool.map(migration.migrate_bundle, bundle_uuids)

    print(
        f"[migration] Migration finished, total {total} bundles migrated, skipped {migration.skipped_ready}(ready) {migration.skipped_link}(linked bundle) {migration.skipped_beam}(on Azure) bundles, skipped delete due to path DNE {migration.skipped_delete_path_dne}, error {migration.error_cnt} bundles. Succeeed {migration.success_cnt} bundles"
    )
    if args.change_db:
        print(
            "[migration][Change DB] Database migration finished, bundle location changed in database."
        )

    if args.delete:
        print("[migration][Deleted] Original bundles deleted from local disk.")

