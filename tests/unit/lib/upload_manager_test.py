import tests.unit.azure_blob_mock  # noqa: F401

import gzip
import os
import tarfile
import tempfile
import unittest
import urllib

from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from io import BytesIO
from memory_profiler import memory_usage
from typing import IO, cast
from unittest.mock import MagicMock
from urllib.response import addinfourl

from codalab.worker.file_util import gzip_bytestring, remove_path, tar_gzip_directory
from tests.unit.server.bundle_manager import TestBase

urlopen_real = urllib.request.urlopen
LARGE_FILE_SIZE = 16777216 #16MB
EXTRA_LARGE_FILE_SIZE = 134217728 #128MB for Memory Profiling Only

class UploadManagerTestBase(TestBase):
    """A class that contains the base for an UploadManager test. Subclasses
    can inherit from this class and unittest.TestCase and provide implementations
    for the unimplemented methods in order to test different types of uploading.
    """

    @property
    def use_azure_blob_beta(self):
        """Whether uploads use Azure Blob Storage."""
        raise NotImplementedError

    def check_file_equals_string(self, file_subpath: str, expected_contents: str):
        """Check that a file in the current bundle location has the specified string as its contents.
        Args:
            file_subpath (str): Subpath within the bundle. Set to an empty string if the bundle is just a single file and you want to specify just that file.
            expected_contents (str): Expected string.
        """
        raise NotImplementedError

    def listdir(self):
        """List the files in the current bundle location."""
        raise NotImplementedError
    
    def check_file_size(self):
        """Check the file sizes in the current bundle location"""
        with FileSystems.open(
            self.bundle_location, compression_type=CompressionTypes.UNCOMPRESSED
        ) as f, tarfile.open(fileobj=f, mode='r:gz') as tf:
            return [tarinfo.size for tarinfo in tf.getmembers()]

    @property
    def bundle_location(self):
        """Get bundle location of the currently created bundle."""
        return self.codalab_manager.bundle_store().get_bundle_location(self.bundle.uuid)

    def setUp(self):
        super().setUp()

        bundle = self.create_run_bundle()
        self.save_bundle(bundle)
        self.bundle = bundle

        self.temp_dir = tempfile.mkdtemp()

        urllib.request.urlopen = urlopen_real

    def tearDown(self):
        remove_path(self.temp_dir)

    def do_upload(
        self, source, git=False, unpack=True,
    ):

        self.upload_manager.upload_to_bundle_store(
            self.bundle, source, git, unpack, use_azure_blob_beta=self.use_azure_blob_beta,
        )

    def test_fileobj(self):
        self.do_upload(('source', BytesIO(b'testing')))
        self.check_file_equals_string('', 'testing')

    def test_fileobj_gz(self):
        self.do_upload(('source.gz', BytesIO(gzip_bytestring(b'testing'))))
        self.check_file_equals_string('', 'testing')

    def test_fileobj_tar_gz(self):
        source = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source)
        self.write_file_of_size(10, os.path.join(source, 'file'))
        self.do_upload(('source.tar.gz', tar_gzip_directory(source)))
        self.assertEqual(['file'], sorted(self.listdir()))
        self.assertEqual([0, 10], self.check_file_size())

    def test_large_fileobj_tar_gz(self):
        """
        Large bundles should not cause issues
        """
        source = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source)
        self.write_file_of_size(LARGE_FILE_SIZE, os.path.join(source, 'bigfile'))
        self.write_string_to_file('testing', os.path.join(source, 'README'))
        self.do_upload(('source.tar.gz', tar_gzip_directory(source)))
        self.assertEqual(['README', 'bigfile'], sorted(self.listdir()))

    def test_large_fileobj_tar_gz2(self):
        """
        Large bundles should not cause issues
        """
        source = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source)
        self.write_file_of_size(LARGE_FILE_SIZE, os.path.join(source, 'bigfile'))
        self.write_file_of_size(LARGE_FILE_SIZE, os.path.join(source, 'bigfile2'))
        self.do_upload(('source.tar.gz', tar_gzip_directory(source)))
        self.assertEqual(['bigfile', 'bigfile2'], sorted(self.listdir()))
        self.assertEqual([0, LARGE_FILE_SIZE, LARGE_FILE_SIZE], self.check_file_size())

    def test_fileobj_tar_gz_should_not_simplify_archives(self):
        source = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source)
        self.write_string_to_file('testing', os.path.join(source, 'filename'))
        self.do_upload(('source.tar.gz', tar_gzip_directory(source)))
        self.assertEqual(['filename'], self.listdir())
        self.check_file_equals_string('filename', 'testing')

    def test_fileobj_tar_gz_with_dsstore_should_not_simplify_archive(self):
        """If the user included two files, README and .DS_Store, in the archive,
        the archive should not be simplified because we have more than one file in the archive.
        """
        source = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source)
        self.write_string_to_file('testing', os.path.join(source, 'README'))
        self.write_string_to_file('testing', os.path.join(source, '.DS_Store'))
        self.do_upload(('source.tar.gz', tar_gzip_directory(source)))
        self.assertEqual(['.DS_Store', 'README'], sorted(self.listdir()))

    def test_fileobj_tar_gz_with_dsstore_should_not_simplify_archive_2(self):
        """If the user included three files, README, README2, and .DS_Store, in the archive,
        the archive should not be simplified because we have more than one file in the archive.
        """
        source = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source)
        self.write_string_to_file('testing', os.path.join(source, 'README'))
        self.write_string_to_file('testing', os.path.join(source, 'README2'))
        self.write_string_to_file('testing', os.path.join(source, '.DS_Store'))
        self.do_upload(('source.tar.gz', tar_gzip_directory(source)))
        self.assertEqual(['.DS_Store', 'README', 'README2'], sorted(self.listdir()))
    
    def mock_url_source(self, fileobj, ext=""):
        """Returns a URL that is mocked to return the contents of fileobj.
        The URL will end in the extension "ext", if given.
        """
        url = f"https://codalab/contents{ext}"
        size = len(fileobj.read())
        fileobj.seek(0)
        urllib.request.urlopen = MagicMock()
        urllib.request.urlopen.return_value = addinfourl(fileobj, {"content-length": size}, url)
        return url

    def test_url(self):
        self.do_upload(self.mock_url_source(BytesIO(b'hello world')))
        self.check_file_equals_string('', 'hello world')

    def test_url_tar_gz(self):
        source = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source)
        self.write_string_to_file('testing', os.path.join(source, 'file1'))
        self.write_string_to_file('testing', os.path.join(source, 'file2'))
        self.do_upload(
            self.mock_url_source(BytesIO(tar_gzip_directory(source).read()), ext=".tar.gz")
        )
        self.assertIn('file2', self.listdir())

    def test_url_tar_gz_should_not_simplify_archives(self):
        source = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source)
        self.write_string_to_file('testing', os.path.join(source, 'filename'))
        self.do_upload(
            self.mock_url_source(BytesIO(tar_gzip_directory(source).read()), ext=".tar.gz")
        )
        self.check_file_equals_string('filename', 'testing')

    def test_url_git(self):
        self.do_upload('https://github.com/codalab/test', git=True)
        # This test hits the real GitHub repository. If the contents of README.md at https://github.com/codalab/test
        # change, then update this test.
        self.check_file_equals_string('testfile.md', '# test\nUsed for testing\n')

    def test_upload_memory(self):
        self.write_file_of_size(LARGE_FILE_SIZE, os.path.join(self.temp_dir, 'bigfile'))
        mem_usage = memory_usage(
            (self.do_upload(('bigfile', os.path.join(self.temp_dir, 'bigfile'))), ),
            interval=0.1,
            timeout=1
        )
        self.assertEqual(max(memory_usage) < 100000000, True)

    def write_string_to_file(self, string, file_path):
        with open(file_path, 'w') as f:
            f.write(string)

    def write_file_of_size(self, size: int, file_path: str):
        with open(file_path, "wb") as f:
            f.seek(size - 1)
            f.write(b"\0")

class UploadManagerDiskStorageTest(UploadManagerTestBase, unittest.TestCase):
    """Tests for UploadManager that upload files to disk storage."""

    @property
    def use_azure_blob_beta(self):
        return False

    def check_file_equals_string(self, file_subpath: str, expected_contents: str):
        file_path = (
            os.path.join(self.bundle_location, file_subpath)
            if file_subpath
            else self.bundle_location
        )
        self.assertTrue(os.path.isfile(file_path))
        with open(file_path, 'r') as f:
            self.assertEqual(f.read(), expected_contents)

    def listdir(self):
        return os.listdir(self.bundle_location)


class UploadManagerBlobStorageTest(UploadManagerTestBase, unittest.TestCase):
    """Tests for UploadManager that upload files to Blob Storage."""

    @property
    def use_azure_blob_beta(self):
        return True

    def check_file_equals_string(self, file_subpath: str, expected_contents: str):
        with FileSystems.open(
            self.bundle_location, compression_type=CompressionTypes.UNCOMPRESSED
        ) as f:
            if not file_subpath:
                # Should be a .gz file
                self.assertTrue(self.bundle_location.endswith("contents.gz"))
                self.assertEqual(gzip.decompress(f.read()).decode(), expected_contents)
            else:
                # Should be a .tar.gz file
                self.assertTrue(self.bundle_location.endswith("contents.tar.gz"))
                with tarfile.open(fileobj=f, mode='r:gz') as tf:
                    # Prepend "./" to the file subpath so that it corresponds with a file in the archive.
                    self.assertEqual(
                        cast(IO[bytes], tf.extractfile("./" + file_subpath)).read().decode(),
                        expected_contents,
                    )

    def listdir(self):
        with FileSystems.open(
            self.bundle_location, compression_type=CompressionTypes.UNCOMPRESSED
        ) as f, tarfile.open(fileobj=f, mode='r:gz') as tf:
            # Remove "." entry and "./" prefixes to make the file listing similar to that
            # in an ordinary directory on disk.
            return [i.replace("./", "") for i in tf.getnames() if i != "."]
