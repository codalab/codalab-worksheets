import tests.unit.azure_blob_mock  # noqa: F401
from codalab.lib.spec_util import generate_uuid
from codalab.worker.download_util import BundleTarget
from codalab.common import NotFoundError, StorageType
from tests.unit.server.bundle_manager import TestBase
from io import BytesIO
import gzip
import tarfile
import unittest
from unittest.mock import patch, MagicMock
from urllib.response import addinfourl
import urllib


class NotFoundTest(TestBase):
    def test_not_found(self):
        """Running get_target_info for a nonexistent bundle should raise an error."""
        with self.assertRaises(NotFoundError):
            target = BundleTarget(generate_uuid(), "")
            self.download_manager.get_target_info(target, 0)


class BaseUploadDownloadBundleTest(TestBase):
    """Base class for UploadDownload tests.
    """

    def create_fileobj_to_upload(self):
        """Create the fileobj that is to be uploaded."""
        raise NotImplementedError

    def get_sources(self, fileobj):
        """Construct a sources array from the given fileobj that can be sent to upload_to_bundle_store."""
        raise NotImplementedError

    def do_upload(self, bundle, sources):
        """Perform the upload (call upload_to_bundle_store) given the specified bundle and sources."""
        raise NotImplementedError

    def check_contents(self, bundle):
        """Check contents of a given bundle."""
        raise NotImplementedError

    @property
    def ext(self):
        """Source extension."""
        raise NotImplementedError

    def test_main(self):
        """Main test -- create file object, bundle, sources, do the upload, then check the contents."""
        f = self.create_fileobj_to_upload()
        bundle = self.create_bundle()
        sources = self.get_sources(f)
        self.do_upload(bundle, sources)
        self.check_contents(bundle)

    def create_bundle(self):
        bundle = self.create_run_bundle()
        self.save_bundle(bundle)
        return bundle

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


class FolderBase(TestBase):
    """Upload a folder. Subclasses must define `ext` and `mode`.
    """

    @property
    def mode(self):
        """Mode (gz, bz2, etc.)"""
        raise NotImplementedError

    def create_fileobj_to_upload(self):
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
        with tarfile.open(fileobj=f, mode=f"w:{self.mode}") as tf:
            writestr(tf, "./item.txt", "hello world")
            writestr(tf, "./src/item2.txt", "hello world")
        f.seek(0)
        return f

    def check_contents(self, bundle):
        """Running get_target_info for a bundle with a folder, and with subpaths."""
        self.assertEqual(bundle.is_dir, True)
        self.assertEqual(bundle.storage_type, self.expected_storage_type)

        target = BundleTarget(bundle.uuid, "")
        info = self.download_manager.get_target_info(target, 2)
        self.assertEqual(info["name"], bundle.uuid)
        self.assertEqual(info["perm"], 0o755)
        self.assertEqual(info["type"], "directory")
        self.assertEqual(str(info["resolved_target"]), f"{bundle.uuid}:")
        # Directory size can vary based on platform, so removing it before checking equality.
        info["contents"][0].pop("size")
        info["contents"][1].pop("size")
        self.assertEqual(
            sorted(info["contents"], key=lambda x: x["name"]),
            sorted(
                [
                    {'name': 'item.txt', 'perm': 0o644, 'type': 'file'},
                    {
                        'name': 'src',
                        'perm': 0o755,
                        'type': 'directory',
                        'contents': [
                            {'name': 'item2.txt', 'size': 11, 'perm': 0o644, 'type': 'file',}
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
            info["contents"], [{'name': 'item2.txt', 'size': 11, 'perm': 0o644, 'type': 'file'}],
        )
        self.check_folder_target_contents(target, expected_members=['.', './item2.txt'])

        target = BundleTarget(bundle.uuid, "src/item2.txt")
        info = self.download_manager.get_target_info(target, 0)
        self.assertEqual(info["name"], "item2.txt")
        self.assertEqual(info["type"], "file")
        self.assertEqual(str(info["resolved_target"]), f"{bundle.uuid}:src/item2.txt")
        self.check_file_target_contents(target)


class TarGzFolderBase(FolderBase):
    """Upload a .tar.gz folder"""

    ext = ".tar.gz"
    mode = "gz"


class TarBz2FolderBase(FolderBase):
    """Upload a .tar.bz2 folder."""

    ext = ".tar.bz2"
    mode = "bz2"


class FileBase(TestBase):
    """Upload a file.
    """

    ext = ""

    def create_fileobj_to_upload(self):
        return BytesIO(b"hello world")

    def check_contents(self, bundle):
        """Running get_target_info for a bundle with a single file."""
        target = BundleTarget(bundle.uuid, "")
        self.assertEqual(bundle.is_dir, False)
        self.assertEqual(bundle.storage_type, self.expected_storage_type)

        info = self.download_manager.get_target_info(target, 0)
        self.assertEqual(info["name"], bundle.uuid)
        self.assertEqual(info["size"], 11)

        self.assertEqual(info["perm"], 0o644)
        self.assertEqual(info["type"], "file")
        self.assertEqual(str(info["resolved_target"]), f"{bundle.uuid}:")
        self.check_file_target_contents(target)


class DiskBundleStoreBase(TestBase):
    """Upload sources to disk bundle store."""

    expected_storage_type = StorageType.DISK_STORAGE.value

    def do_upload(self, bundle, sources):
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


class BlobBundleStoreBase(TestBase):
    """Upload sources to Blob bundle store."""

    expected_storage_type = StorageType.AZURE_BLOB_STORAGE.value

    def do_upload(self, bundle, sources):
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


class FileObjUploadBase(TestBase):
    """Upload sources as a file object."""

    def get_sources(self, fileobj):
        return [[f"contents{self.ext}", fileobj]]


class URLUploadBase(TestBase):
    """Upload sources as a URL."""

    def get_sources(self, fileobj):
        url = f"https://codalab/contents{self.ext}"
        size = len(fileobj.read())
        fileobj.seek(0)
        urllib.request.urlopen = MagicMock()
        urllib.request.urlopen.return_value = addinfourl(fileobj, {"content-length": size}, url)
        return [url]


class DiskUploadFileTest(
    FileBase,
    DiskBundleStoreBase,
    FileObjUploadBase,
    BaseUploadDownloadBundleTest,
    TestBase,
    unittest.TestCase,
):
    """Upload a file as a fileobj to the disk bundle store."""

    pass


class DiskUploadFolderTest(
    TarGzFolderBase,
    DiskBundleStoreBase,
    FileObjUploadBase,
    BaseUploadDownloadBundleTest,
    TestBase,
    unittest.TestCase,
):
    """Upload a folder as a fileobj to the disk bundle store."""

    pass


class BlobUploadFileTest(
    FileBase,
    BlobBundleStoreBase,
    FileObjUploadBase,
    BaseUploadDownloadBundleTest,
    TestBase,
    unittest.TestCase,
):
    """Upload a file as a fileobj to the blob bundle store."""

    pass


class BlobUploadFolderTest(
    TarGzFolderBase,
    BlobBundleStoreBase,
    FileObjUploadBase,
    BaseUploadDownloadBundleTest,
    TestBase,
    unittest.TestCase,
):
    """Upload a folder as a fileobj to the blob bundle store."""

    pass


class DiskUploadFileURLTest(
    FileBase,
    DiskBundleStoreBase,
    URLUploadBase,
    BaseUploadDownloadBundleTest,
    TestBase,
    unittest.TestCase,
):
    """Upload a file as a URL to the disk bundle store."""

    pass


class DiskUploadFolderURLTest(
    TarGzFolderBase,
    DiskBundleStoreBase,
    URLUploadBase,
    BaseUploadDownloadBundleTest,
    TestBase,
    unittest.TestCase,
):
    """Upload a folder as a URL to the disk bundle store."""

    pass


class BlobUploadFileURLTest(
    FileBase,
    BlobBundleStoreBase,
    URLUploadBase,
    BaseUploadDownloadBundleTest,
    TestBase,
    unittest.TestCase,
):
    """Upload a file as a URL to the blob bundle store."""

    pass


class BlobUploadFolderURLTest(
    TarGzFolderBase,
    BlobBundleStoreBase,
    URLUploadBase,
    BaseUploadDownloadBundleTest,
    TestBase,
    unittest.TestCase,
):
    """Upload a folder as a URL to the blob bundle store."""

    pass


class BlobUploadTarBz2FolderURLTest(
    TarBz2FolderBase,
    BlobBundleStoreBase,
    URLUploadBase,
    BaseUploadDownloadBundleTest,
    TestBase,
    unittest.TestCase,
):
    """Upload a .tar.bz2 folder as a URL to the blob bundle store."""

    pass
