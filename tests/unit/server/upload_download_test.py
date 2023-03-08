import tests.unit.azure_blob_mock  # noqa: F401

import gzip
import os
import tarfile
import tempfile
import unittest

from io import BytesIO

from codalab.common import NotFoundError, StorageType
from codalab.lib.spec_util import generate_uuid
from codalab.worker.download_util import BundleTarget
from codalab.worker.file_util import tar_gzip_directory
from tests.unit.server.bundle_manager import TestBase
from codalab.lib.beam.filesystems import get_azure_bypass_conn_str


class BaseUploadDownloadBundleTest(TestBase):
    """Base class for UploadDownload tests.
    All subclasses must implement the upload_folder
    and upload_file methods.
    """

    DEFAULT_PERM_FILE = 0  # Should be overridden by subclasses

    DEFAULT_PERM_DIR = 0o777

    @property
    def use_azure_blob_beta(self):
        """Whether to use Azure Blob Storage for uploads."""
        raise NotImplementedError

    @property
    def storage_type(self):
        """Returns storage type. Must be one of the values of the StorageType enum."""
        raise NotImplementedError

    def upload_folder(self, bundle, contents):
        with tempfile.TemporaryDirectory() as tmpdir:
            for item in contents:
                path, contents = item
                file_path = os.path.join(tmpdir, path)
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                with open(file_path, "wb+") as f:
                    f.write(contents)
                os.chmod(file_path, self.DEFAULT_PERM_FILE)
                os.chmod(os.path.dirname(file_path), self.DEFAULT_PERM_DIR)
            self.upload_manager.upload_to_bundle_store(
                bundle,
                source=["contents.tar.gz", tar_gzip_directory(tmpdir)],
                git=False,
                unpack=True,
                use_azure_blob_beta=self.use_azure_blob_beta,
            )

    def upload_file(self, bundle, contents):
        self.upload_manager.upload_to_bundle_store(
            bundle,
            source=["contents", BytesIO(contents)],
            git=False,
            unpack=False,
            use_azure_blob_beta=self.use_azure_blob_beta,
        )

    def test_not_found(self):
        """Running get_target_info for a nonexistent bundle should raise an error."""
        with self.assertRaises(NotFoundError):
            target = BundleTarget(generate_uuid(), "")
            self.download_manager.get_target_info(target, 0)

    def check_file_target_contents(self, target):
        """Checks to make sure that the specified file has the contents 'hello world'."""
        # This can not be checked, Since
        with self.download_manager.stream_file(target, gzipped=False) as f:
            self.assertEqual(f.read(), b"hello world")

        with gzip.GzipFile(fileobj=self.download_manager.stream_file(target, gzipped=True)) as f:
            self.assertEqual(f.read(), b"hello world")

        with BytesIO(
            self.download_manager.read_file_section(target, offset=3, length=4, gzipped=False)
        ) as f:
            self.assertEqual(f.read(), b"lo w")

        with gzip.GzipFile(
            fileobj=BytesIO(
                self.download_manager.read_file_section(target, offset=3, length=4, gzipped=True)
            )
        ) as f:
            self.assertEqual(f.read(), b"lo w")

        with BytesIO(
            self.download_manager.summarize_file(
                target,
                num_head_lines=1,
                num_tail_lines=1,
                max_line_length=3,
                truncation_text="....",
                gzipped=False,
            )
        ) as f:
            self.assertEqual(f.read(), b"....")

        with BytesIO(
            self.download_manager.summarize_file(
                target,
                num_head_lines=50,
                num_tail_lines=0,
                max_line_length=128,
                truncation_text="....",
                gzipped=False,
            )
        ) as f:
            self.assertEqual(f.read(), b"hello world\n")

        with BytesIO(
            self.download_manager.summarize_file(
                target,
                num_head_lines=50,
                num_tail_lines=0,
                max_line_length=128,
                truncation_text="....",
                gzipped=True,
            )
        ) as f:
            self.assertEqual(gzip.decompress(f.read()), b"hello world\n")

    def check_folder_target_contents(self, target, expected_members=[]):
        """Checks to make sure that the specified folder has the expected contents and can be streamed, etc."""
        with self.assertRaises(IOError):
            with self.download_manager.stream_file(target, gzipped=False) as f:
                pass

        with self.assertRaises(IOError):
            self.download_manager.summarize_file(
                target,
                num_head_lines=1,
                num_tail_lines=1,
                max_line_length=3,
                truncation_text="....",
                gzipped=False,
            )

        with tarfile.open(
            fileobj=self.download_manager.stream_tarred_gzipped_directory(target), mode='r:gz'
        ) as f:
            self.assertEqual(sorted(f.getnames()), sorted(expected_members))

    def test_bundle_single_file(self):
        """Running get_target_info for a bundle with a single file."""
        bundle = self.create_run_bundle()
        self.save_bundle(bundle)
        self.upload_file(bundle, b"hello world")
        target = BundleTarget(bundle.uuid, "")
        self.assertEqual(bundle.is_dir, False)
        self.assertEqual(bundle.storage_type, self.storage_type)

        info = self.download_manager.get_target_info(target, 0)
        print("info: ", info)
        self.assertEqual(info["name"], bundle.uuid)
        self.assertEqual(info["size"], 11)  # the size is size after compress
        self.assertEqual(info["perm"], self.DEFAULT_PERM_FILE)
        self.assertEqual(info["type"], "file")
        self.assertEqual(str(info["resolved_target"]), f"{bundle.uuid}:")
        self.check_file_target_contents(target)

    def test_bundle_folder(self):
        """Running get_target_info for a bundle with a folder, and with subpaths."""
        bundle = self.create_run_bundle()
        self.save_bundle(bundle)
        self.upload_folder(
            bundle, [("item.txt", b"hello world"), ("src/item2.txt", b"hello world")]
        )
        self.assertEqual(bundle.is_dir, True)
        self.assertEqual(bundle.storage_type, self.storage_type)

        target = BundleTarget(bundle.uuid, "")
        info = self.download_manager.get_target_info(target, 2)
        self.assertEqual(info["name"], bundle.uuid)
        self.assertEqual(info["type"], "directory")
        self.assertEqual(str(info["resolved_target"]), f"{bundle.uuid}:")
        # Directory size can vary based on platform, so removing it before checking equality.
        info["contents"][0].pop("size")
        info["contents"][1].pop("size")
        self.assertEqual(
            sorted(info["contents"], key=lambda x: x["name"]),
            sorted(
                [
                    {'name': 'item.txt', 'perm': self.DEFAULT_PERM_FILE, 'type': 'file'},
                    {
                        'name': 'src',
                        'perm': self.DEFAULT_PERM_DIR,
                        'type': 'directory',
                        'contents': [
                            {
                                'name': 'item2.txt',
                                'size': 11,
                                'perm': self.DEFAULT_PERM_FILE,
                                'type': 'file',
                            }
                        ],
                    },
                ],
                key=lambda x: x["name"],
            ),
        )
        self.check_folder_target_contents(
            target, expected_members=['.', './item.txt', './src', './src/item2.txt']
        )

        target = BundleTarget(bundle.uuid, "item.txt")
        info = self.download_manager.get_target_info(target, 0)
        self.assertEqual(info["name"], "item.txt")
        self.assertEqual(info["type"], "file")
        self.assertEqual(str(info["resolved_target"]), f"{bundle.uuid}:item.txt")
        self.check_file_target_contents(target)

        target = BundleTarget(bundle.uuid, "src")
        info = self.download_manager.get_target_info(target, 1)
        self.assertEqual(info["name"], "src")
        self.assertEqual(info["type"], "directory")
        self.assertEqual(str(info["resolved_target"]), f"{bundle.uuid}:src")
        self.assertEqual(
            info["contents"],
            [{'name': 'item2.txt', 'size': 11, 'perm': self.DEFAULT_PERM_FILE, 'type': 'file'}],
        )
        self.check_folder_target_contents(target, expected_members=['.', './item2.txt'])

        target = BundleTarget(bundle.uuid, "src/item2.txt")
        info = self.download_manager.get_target_info(target, 0)
        self.assertEqual(info["name"], "item2.txt")
        self.assertEqual(info["type"], "file")
        self.assertEqual(str(info["resolved_target"]), f"{bundle.uuid}:src/item2.txt")
        self.check_file_target_contents(target)


class RegularBundleStoreTest(BaseUploadDownloadBundleTest, unittest.TestCase):
    """Test uploading and downloading from / to a regular, file-based bundle store."""

    DEFAULT_PERM_FILE = 0o644

    @property
    def use_azure_blob_beta(self):
        return False

    @property
    def storage_type(self):
        return StorageType.DISK_STORAGE.value


class AzureBlobBundleStoreTest(BaseUploadDownloadBundleTest, unittest.TestCase):
    """Test uploading and downloading from / to Azure Blob storage."""

    DEFAULT_PERM_FILE = 0o755

    @property
    def use_azure_blob_beta(self):
        return True

    @property
    def storage_type(self):
        return StorageType.AZURE_BLOB_STORAGE.value


class AzureBypassConnStrTest(unittest.TestCase):
    def test_azure_bypass_conn_str(self):
        bypass_conn_str = get_azure_bypass_conn_str()
        self.assertEqual("AccountKey" in bypass_conn_str, False)
