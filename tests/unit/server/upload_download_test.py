import tests.unit.azure_blob_mock  # noqa: F401
from codalab.lib.spec_util import generate_uuid
from codalab.worker.download_util import BundleTarget
from codalab.common import NotFoundError, StorageType
from tests.unit.server.bundle_manager import TestBase
from io import BytesIO
import gzip
import tarfile
import unittest


class BaseUploadDownloadBundleTest(TestBase):
    DEFAULT_PERM = 420

    def upload_folder(self, bundle, contents):
        raise NotImplementedError

    def upload_file(self, bundle, contents):
        raise NotImplementedError

    def test_not_found(self):
        """Running get_target_info for a nonexistent bundle should raise an error."""
        with self.assertRaises(NotFoundError):
            target = BundleTarget(generate_uuid(), "")
            self.download_manager.get_target_info(target, 0)

    def check_file_target_contents(self, target):
        """Checks to make sure that the specified file has the contents 'hello world'."""
        with self.download_manager.stream_file(target, gzipped=False) as f:
            self.assertEqual(f.read(), b"hello world")

        with gzip.GzipFile(fileobj=self.download_manager.stream_file(target, gzipped=True)) as f:
            self.assertEqual(f.read(), b"hello world")

        with self.assertRaises(tarfile.ReadError):
            with tarfile.open(
                fileobj=self.download_manager.stream_tarred_gzipped_directory(target), mode='r:gz'
            ) as f:
                pass

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

    def check_folder_target_contents(self, target, expected_members=[]):
        """Checks to make sure that the specified folder has the expected contents and can be streamed, etc."""
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
        self.assertEqual(bundle.storage_type, StorageType.DISK_STORAGE.value)

        info = self.download_manager.get_target_info(target, 0)
        self.assertEqual(info["name"], bundle.uuid)
        self.assertEqual(info["size"], 11)

        self.assertEqual(info["perm"], 420)
        self.assertEqual(info["type"], "file")
        self.assertEqual(str(info["resolved_target"]), f"{bundle.uuid}:")
        self.check_file_target_contents(target)

    def test_bundle_folder(self):
        """Running get_target_info for a bundle with a folder, and with subpaths."""
        bundle = self.create_run_bundle()
        self.save_bundle(bundle)

        f = BytesIO()

        def writestr(tf, name, contents):
            tinfo = tarfile.TarInfo(name)
            tinfo.size = len(contents)
            tf.addfile(tinfo, BytesIO(contents.encode()))

        def writedir(tf, name):
            tinfo = tarfile.TarInfo(name)
            tinfo.type = tarfile.DIRTYPE
            tf.addfile(tinfo, BytesIO())

        f.seek(0)
        with tarfile.open(fileobj=f, mode="w:gz") as tf:
            writestr(tf, "./item.txt", "hello world")
            writestr(tf, "./src/item2.txt", "hello world")
        f.seek(0)
        self.upload_folder(bundle, f)
        self.assertEqual(bundle.is_dir, True)
        self.assertEqual(bundle.storage_type, StorageType.DISK_STORAGE.value)

        target = BundleTarget(bundle.uuid, "")
        info = self.download_manager.get_target_info(target, 2)
        self.assertEqual(info["name"], bundle.uuid)
        self.assertEqual(info["perm"], 511)
        self.assertEqual(info["type"], "directory")
        self.assertEqual(str(info["resolved_target"]), f"{bundle.uuid}:")
        # Directory size can vary based on platform, so removing it before checking equality.
        info["contents"][0].pop("size")
        info["contents"][1].pop("size")
        self.assertEqual(
            sorted(info["contents"], key=lambda x: x["name"]),
            sorted(
                [
                    {'name': 'item.txt', 'perm': self.DEFAULT_PERM, 'type': 'file'},
                    {
                        'name': 'src',
                        'perm': 493,
                        'type': 'directory',
                        'contents': [
                            {
                                'name': 'item2.txt',
                                'size': 11,
                                'perm': self.DEFAULT_PERM,
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
            [{'name': 'item2.txt', 'size': 11, 'perm': self.DEFAULT_PERM, 'type': 'file'}],
        )
        self.check_folder_target_contents(target, expected_members=['.', './item2.txt'])

        target = BundleTarget(bundle.uuid, "src/item2.txt")
        info = self.download_manager.get_target_info(target, 0)
        self.assertEqual(info["name"], "item2.txt")
        self.assertEqual(info["type"], "file")
        self.assertEqual(str(info["resolved_target"]), f"{bundle.uuid}:src/item2.txt")
        self.check_file_target_contents(target)


class RegularBundleStoreTest(BaseUploadDownloadBundleTest):
    """Test uploading and downloading from / to a regular, file-based bundle store."""

    def upload_folder(self, bundle, f):
        sources = [["contents.tar.gz", f]]
        self.upload_manager.upload_to_bundle_store(
            bundle,
            sources,
            follow_symlinks=False,
            exclude_patterns=None,
            remove_sources=False,
            git=False,
            unpack=True,
            simplify_archives=True,
            use_azure_blob_beta=False,
        )

    def upload_file(self, bundle, contents):
        sources = [["contents", BytesIO(contents)]]
        self.upload_manager.upload_to_bundle_store(
            bundle,
            sources,
            follow_symlinks=False,
            exclude_patterns=None,
            remove_sources=False,
            git=False,
            unpack=False,
            simplify_archives=True,
            use_azure_blob_beta=False,
        )


class AzureBlobBundleStoreTest(BaseUploadDownloadBundleTest, unittest.TestCase):
    """Test uploading and downloading from / to Azure Blob storage."""

    def upload_folder(self, bundle, f):
        sources = [["contents.tar.gz", f]]
        self.upload_manager.upload_to_bundle_store(
            bundle,
            sources,
            follow_symlinks=False,
            exclude_patterns=None,
            remove_sources=False,
            git=False,
            unpack=True,
            simplify_archives=True,
            use_azure_blob_beta=True,
        )

    def upload_file(self, bundle, contents):
        sources = [["contents", BytesIO(contents)]]
        self.upload_manager.upload_to_bundle_store(
            bundle,
            sources,
            follow_symlinks=False,
            exclude_patterns=None,
            remove_sources=False,
            git=False,
            unpack=False,
            simplify_archives=True,
            use_azure_blob_beta=True,
        )
