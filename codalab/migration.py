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

    def get_bundle_uuids(self, worksheet_uuid, max_result=100):
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
        # bundle_locations = self.bundle_manager._model.get_bundle_locations(bundle_uuid)  # this is getting locations from
        bundle_location = self.bundle_manager._bundle_store.get_bundle_location(bundle_uuid)
        return bundle_location

    def get_bundle(self, bundle_uuid):
        return self.bundle_manager._model.get_bundle(bundle_uuid)

    def get_bundle_info(self, bundle_uuid, bundle_location):
        target = BundleTarget(bundle_uuid, subpath='')
        logging.info(f"[migration] {target}")
        try:
            info = download_util.get_target_info(bundle_location, target, depth=0)
            logging.info(f"[migration] {info}")
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
            destination_bundle_store=None,
            json_api_client=None,
        )

        if is_dir:
            source_fileobj = zip_util.tar_gzip_directory(bundle_location, exclude_patterns=None)
            source_ext = ".tar.gz"
            unpack = True
        else:
            # If it's a file, change it into GzipStream
            source_fileobj = open(bundle_location, 'rb')
            source_ext = ''
            unpack = False

        # Upload file content and generate index file
        uploader.write_fileobj(source_ext, source_fileobj, target_location, unpack_archive=unpack)

        assert FileSystems.exists(target_location)

    def modify_bundle_data(self, bundle, bundle_uuid, is_dir):
        """
        Change the bundle location in the database
        ATTENTION: this function will modify codalab
        """

        # add bundle location
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

    def sanity_check(self, bundle_uuid, bundle_location, bundle_info, is_dir):
        new_location = self.get_bundle_location(bundle_uuid)
        if is_dir:
            # For dirs, check the folder contains same files
            with OpenFile(new_location, gzipped=True, exclude_patterns=None) as f:
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

    def delete_original_bundle(self, bundle_uuid, bundle_location):
        # Delete data from orginal bundle store
        if os.path.exists(bundle_location):
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
    parser.add_argument('--target_store_name', type=str, help='The destination bundle store name')
    parser.add_argument(
        '-c', '--change_db', help='Change the bundle location in the database', action='store_true',
    )
    parser.add_argument('-d', '--delete', help='Delete the original database', action='store_true')

    args = parser.parse_args()

    worksheet_uuid = args.worksheet
    target_store_name = (
        "azure-store-default" if args.target_store_name is None else args.target_store_name
    )

    # TODO: write output to log / log files
    migration = Migration(target_store_name)
    migration.setUp()

    if args.all:
        bundle_uuids = migration.get_bundle_uuids(worksheet_uuid=None)
    else:
        # Must specify worksheet uuid
        if worksheet_uuid is not None and not spec_util.UUID_REGEX.match(worksheet_uuid):
            raise Exception("Input worksheet uuid has wrong format. ")
        bundle_uuids = migration.get_bundle_uuids(worksheet_uuid)

    for bundle_uuid in bundle_uuids:
        logging.info(bundle_uuid)

        bundle = migration.get_bundle(bundle_uuid)
        if bundle.bundle_type != 'dataset' or bundle.state != 'ready':
            # only migrate uploaded bundle, and the bundle state needs to be ready
            continue

        # Uploaded bundles does not need has dependencies
        assert len(bundle.dependencies) == 0

        if migration.is_linked_bundle(bundle_uuid):
            # Do not migrate link bundle
            continue

        bundle_location = migration.get_bundle_location(bundle_uuid)

        if parse_linked_bundle_url(bundle_location).uses_beam:
            # Do not migrate Azure / GCP bundles
            continue

        bundle_info = migration.get_bundle_info(bundle_uuid, bundle_location)

        is_dir = bundle_info['type'] == 'directory'
        migration.upload_to_azure_blob(bundle_uuid, bundle_location, is_dir)

        if args.change_db:  # If need to change the database, continue to run
            migration.modify_bundle_data(bundle, bundle_uuid, is_dir)
            migration.sanity_check(bundle_uuid, bundle_location, bundle_info, is_dir)
            if args.delete:
                migration.delete_original_bundle(bundle_uuid, bundle_location)
