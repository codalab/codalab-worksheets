import os
from io import BytesIO
import tempfile
import unittest

from codalab.lib.upload_manager import UploadManager
from codalab.worker.file_util import gzip_bytestring, remove_path, tar_gzip_directory

from unittest.mock import MagicMock
from urllib.response import addinfourl
import urllib

urlopen_real = urllib.request.urlopen


class UploadManagerTest(unittest.TestCase):
    def setUp(self):
        class MockBundleStore(object):
            def __init__(self, bundle_location):
                self.bundle_location = bundle_location

            def get_bundle_location(self, uuid):
                return self.bundle_location

        class MockBundleModel(object):
            def update_bundle(self, *args, **kwargs):
                return

        self.temp_dir = tempfile.mkdtemp()
        self.bundle_location = os.path.join(self.temp_dir, 'bundle')
        self.manager = UploadManager(MockBundleModel(), MockBundleStore(self.bundle_location))
        urllib.request.urlopen = urlopen_real

    def tearDown(self):
        remove_path(self.temp_dir)

    def do_upload(
        self, sources, git=False, unpack=True, simplify_archives=True, use_azure_blob_beta=False,
    ):
        class FakeBundle(object):
            def __init__(self):
                self.uuid = 'fake'
                self.metadata = object()

        self.manager.upload_to_bundle_store(
            FakeBundle(), sources, git, unpack, simplify_archives, use_azure_blob_beta,
        )

    def test_fileobj_single(self):
        self.do_upload([('source', BytesIO(b'testing'))])
        self.check_file_contains_string(self.bundle_location, 'testing')

    def test_fileobj_single_gz(self):
        self.do_upload([('source.gz', BytesIO(gzip_bytestring(b'testing')))])
        self.check_file_contains_string(self.bundle_location, 'testing')

    def test_fileobj_single_tar_gz_simplify_archives(self):
        source = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source)
        self.write_string_to_file('testing', os.path.join(source, 'filename'))
        self.do_upload([('source.tar.gz', tar_gzip_directory(source))])
        self.check_file_contains_string(self.bundle_location, 'testing')

    def test_fileobj_single_tar_gz_no_simplify_archives(self):
        source = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source)
        self.write_string_to_file('testing', os.path.join(source, 'filename'))
        self.do_upload([('source.tar.gz', tar_gzip_directory(source))], simplify_archives=False)
        self.assertEqual(['filename'], os.listdir(self.bundle_location))
        self.check_file_contains_string(os.path.join(self.bundle_location, 'filename'), 'testing')

    def test_fileobj_single_tar_gz_with_dsstore_should_not_simplify_archive(self):
        """If the user included two files, README and .DS_Store, in the archive,
        the archive should not be simplified because we have more than one file in the archive.
        """
        source = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source)
        self.write_string_to_file('testing', os.path.join(source, 'README'))
        self.write_string_to_file('testing', os.path.join(source, '.DS_Store'))
        self.do_upload([('source.tar.gz', tar_gzip_directory(source))])
        self.assertEqual(['.DS_Store', 'README'], sorted(os.listdir(self.bundle_location)))

    def test_fileobj_single_tar_gz_with_dsstore_should_not_simplify_archive_2(self):
        """If the user included three files, README, README2, and .DS_Store, in the archive,
        the archive should not be simplified because we have more than one file in the archive.
        """
        source = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source)
        self.write_string_to_file('testing', os.path.join(source, 'README'))
        self.write_string_to_file('testing', os.path.join(source, 'README2'))
        self.write_string_to_file('testing', os.path.join(source, '.DS_Store'))
        self.do_upload([('source.tar.gz', tar_gzip_directory(source))])
        self.assertEqual(
            ['.DS_Store', 'README', 'README2'], sorted(os.listdir(self.bundle_location))
        )

    def mock_url_sources(self, fileobj, ext=""):
        """Returns a URL that is mocked to return the contents of fileobj.
        The URL will end in the extension "ext", if given.
        """
        url = f"https://codalab/contents{ext}"
        size = len(fileobj.read())
        fileobj.seek(0)
        urllib.request.urlopen = MagicMock()
        urllib.request.urlopen.return_value = addinfourl(fileobj, {"content-length": size}, url)
        return [url]

    def test_url_single(self):
        self.do_upload(self.mock_url_sources(BytesIO(b'hello world')))
        self.check_file_contains_string(self.bundle_location, 'hello world')

    def test_url_tar_gz(self):
        source = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source)
        self.write_string_to_file('testing', os.path.join(source, 'file1'))
        self.write_string_to_file('testing', os.path.join(source, 'file2'))
        self.do_upload(
            self.mock_url_sources(BytesIO(tar_gzip_directory(source).read()), ext=".tar.gz")
        )
        self.assertIn('file2', os.listdir(self.bundle_location))

    def test_multiple_sources(self):
        self.do_upload(
            [
                ('source1', BytesIO(b'testing1')),
                ('source2', BytesIO(b'testing2')),
                'http://alpha.gnu.org/gnu/bc/bc-1.06.95.tar.bz2',
            ]
        )
        self.assertEqual(
            ['bc-1.06.95', 'source1', 'source2'], sorted(os.listdir(self.bundle_location))
        )
        self.check_file_contains_string(os.path.join(self.bundle_location, 'source1'), 'testing1')
        self.check_file_contains_string(os.path.join(self.bundle_location, 'source2'), 'testing2')
        self.assertIn('README', os.listdir(os.path.join(self.bundle_location, 'bc-1.06.95')))

    def test_url_git(self):
        self.do_upload(['https://github.com/codalab/test'], git=True)

    def write_string_to_file(self, string, file_path):
        with open(file_path, 'w') as f:
            f.write(string)

    def write_bytes_to_file(self, bytes_, file_path):
        with open(file_path, 'wb') as f:
            f.write(bytes_)

    def check_file_contains_string(self, file_path, string):
        self.assertTrue(os.path.isfile(file_path))
        with open(file_path, 'r') as f:
            self.assertEqual(f.read(), string)
