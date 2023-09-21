# A script to migrate bundles from disk storage to Azure storage (UploadBundles, MakeBundles, RunBundles?)
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


# Steps:
# 1. Create a database connection. Connecting to local database to get infomations
# 2. Get bundle locations in local filesystem using CodaLabManager()
#    Get bundle information using CodaLabManager
# 3. Find the proporate target bundle_url ([uuid]/contents.gz, [uuid]/contents.gz)
# 4. Upload all the bundles from local disk to Azure
# 5. Update database, pointing to new locations
# 6. Delete the original data


class Migration:
    """
    Base class for BundleManager tests with a CodaLab Manager uses local database.
    ATTENTION: this class will modify real bundle database.
    """

    def __init__(self, target_store_name) -> None:
        self.target_store_name = target_store_name

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
        self.change_db_records_file = "change_db_records.txt"
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
        return bundle_link_url is None

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
        logging.info(f"[migration] Uploading bundle {bundle_uuid} to Azure storage")
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
            "[migration] Uploading from %s to Azure Blob Storage %s",
            bundle_location,
            target_location,
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

        # write change database record to a file
        path = os.path.join(self.codalab_manager.codalab_home, self.change_db_records_file)
        with open(path, 'a') as f:
            f.write(f"{bundle_uuid},{original_location},{new_location}\n")

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

    def delete_original_bundle_by_uuid(self, bundle_uuid, bundle_location):
        """
        Delete original bundle from local disk
        """
        if os.path.exists(bundle_location):
            logging.info(
                f"[migration] Deleting original bundle {bundle_uuid} from local disk path {bundle_location}"
            )
            deleted_size = path_util.get_path_size(bundle_location)
            bundle_user_id = self.bundle_manager._model.get_bundle_owner_ids([bundle_uuid])[
                bundle_uuid
            ]
            path_util.remove(bundle_location)
            # update user's disk usage: reduce original bundle size
            user_info = self.bundle_manager._model.get_user_info(bundle_user_id)
            assert user_info['disk_used'] >= deleted_size
            new_disk_used = user_info['disk_used'] - deleted_size
            self.bundle_manager._model.update_user_info(
                {'user_id': bundle_user_id, 'disk_used': new_disk_used}
            )

    def delete_original_bundle(self):
        """
        Delete all the original bundle that has been uploaded to Azure Blob Storage and changed location in database
        """
        path = os.path.join(self.codalab_manager.codalab_home, self.change_db_records_file)
        if not os.path.exists(path):
            return
        with open(path, "r+") as f:
            lines = f.readlines()
            f.seek(0)
            f.truncate()
            for line in lines:
                bundle_uuid, origin_bundle_location, new_location = line.replace('\n', '').split(",")
                try:
                    is_dir = new_location.endswith("tar.gz")
                    migration.sanity_check(
                        bundle_uuid, origin_bundle_location, None, is_dir, new_location
                    )
                except FileNotFoundError:
                    logging.info(f"[migration] Bundle {bundle_uuid} already deleted from local disk")
                    continue
                except Exception as e:
                    logging.error(f"[migration] Sanity Check Error: {str(e)} for bundle {bundle_uuid}")
                    f.write(line)
                    continue

                if not self.get_bundle_location(bundle_uuid).startswith(
                    StorageURLScheme.AZURE_BLOB_STORAGE.value
                ):
                    logging.info(f"[migration] Bundle {bundle_uuid} info in database is not properly updated")
                    raise Exception(
                        f"Bundle {bundle_uuid} info in database is not properly updated"
                    )
                try:
                    self.delete_original_bundle_by_uuid(bundle_uuid, origin_bundle_location)
                except Exception as e:
                    # If the bundle is not deleted, save the information in the file
                    logging.error(f"[migration] Delete Original Bundle Error: {str(e)}")
                    f.write(line)
            


if __name__ == '__main__':
    # Command line parser, parse the worksheet id
    parser = argparse.ArgumentParser(
        description='Manages your local CodaLab Worksheets service deployment'
    )
    parser.add_argument(
        '-a', '--all', help='Run migration on all worksheets and all bundles', action='store_true',
    )
    parser.add_argument(
        '-w', '--worksheet', type=str, help='The worksheet uuid that needs migration'
    )
    parser.add_argument(
        '-t', '--target_store_name', type=str, help='The destination bundle store name'
    )
    parser.add_argument(
        '-c', '--change_db', help='Change the bundle location in the database', action='store_true',
    )
    parser.add_argument('-d', '--delete', help='Delete the original database', action='store_true')

    args = parser.parse_args()

    worksheet_uuid = args.worksheet
    target_store_name = (
        "azure-store-default" if args.target_store_name is None else args.target_store_name
    )

    migration = Migration(target_store_name)
    migration.setUp()

    if args.all:
        bundle_uuids = migration.get_bundle_uuids(worksheet_uuid=None)
    else:
        # Must specify worksheet uuid
        if worksheet_uuid is not None and not spec_util.UUID_REGEX.match(worksheet_uuid):
            raise Exception("Input worksheet uuid has wrong format. ")
        bundle_uuids = migration.get_bundle_uuids(worksheet_uuid)

    total = len(bundle_uuids)
    skipped, error_cnt, success_cnt = 0, 0, 0
    logging.info(f"[migration] Start migrating {total} bundles")
    for bundle_uuid in bundle_uuids:
        bundle = migration.get_bundle(bundle_uuid)

        # TODO: change this to allow migration of run bundles
        if bundle.state != 'ready':
            # only migrate uploaded bundle, and the bundle state needs to be ready
            skipped += 1
            continue

        # Uploaded bundles does not need has dependencies
        # logging.info(bundle.dependencies)
        # assert len(bundle.dependencies) == 0

        if migration.is_linked_bundle(bundle_uuid):
            # Do not migrate link bundle
            skipped += 1
            continue

        # bundle_location is the original bundle location
        bundle_location = migration.get_bundle_location(bundle_uuid)

        if parse_linked_bundle_url(bundle_location).uses_beam:
            # Do not migrate Azure / GCP bundles
            skipped += 1
            continue

        # TODO: Add try-catch wrapper, cuz some bulde will generate "path not found error"
        try:
            bundle_info = migration.get_bundle_info(bundle_uuid, bundle_location)
        except Exception as e:
            error_cnt += 1
            continue

        is_dir = bundle_info['type'] == 'directory'

        new_location = migration.upload_to_azure_blob(bundle_uuid, bundle_location, is_dir)
        success_cnt += 1
        migration.sanity_check(bundle_uuid, bundle_location, bundle_info, is_dir, new_location)

        if args.change_db:  # If need to change the database, continue to run
            migration.modify_bundle_data(bundle, bundle_uuid, is_dir)
            migration.sanity_check(bundle_uuid, bundle_location, bundle_info, is_dir)

    logging.info(f"[migration] Migration finished, total {total} bundles migrated, skipped {skipped} bundles, error {error_cnt} bundles. Succeeed {success_cnt} bundles")
    if args.change_db:
        logging.info(f"[migration][Change DB] Database migration finished, bundle location changed in database.")
    
    if args.delete:
        migration.delete_original_bundle()
        logging.info(f"[migration][Deleted] Original bundles deleted from local disk.")
