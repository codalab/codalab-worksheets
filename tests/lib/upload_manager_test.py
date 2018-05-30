import os
from cStringIO import StringIO
import tempfile
import unittest

from codalab.lib.upload_manager import UploadManager
from codalabworker.file_util import gzip_string, remove_path, tar_gzip_directory

class UploadManagerTest(unittest.TestCase):
    def setUp(self):
        class MockBundleStore(object):
            def __init__(self, bundle_location):
                self.bundle_location = bundle_location

            def get_bundle_location(self, uuid):
                return self.bundle_location

        self.temp_dir = tempfile.mkdtemp()
        self.bundle_location = os.path.join(self.temp_dir, 'bundle')
        self.manager = UploadManager(None, MockBundleStore(self.bundle_location))

    def tearDown(self):
        remove_path(self.temp_dir)

    def do_upload(self, sources,
                  follow_symlinks=False, exclude_patterns=[], remove_sources=False,
                  git=False, unpack=True, simplify_archives=True):
        class FakeBundle(object):
            def __init__(self):
                self.uuid = 'fake'
        self.manager.upload_to_bundle_store(
            FakeBundle(), sources,
            follow_symlinks, exclude_patterns, remove_sources,
            git, unpack, simplify_archives)

    def test_single_local_path(self):
        source = os.path.join(self.temp_dir, 'filename')
        self.write_string_to_file('testing', source)
        self.do_upload([source])
        self.assertTrue(os.path.exists(source))
        self.check_file_contains_string(self.bundle_location, 'testing')

    def test_ignored_files(self):
        dsstore_file = os.path.join(self.temp_dir, '.DS_Store')
        macosx_file = os.path.join(self.temp_dir, '__MACOSX')
        self.write_string_to_file('testing', dsstore_file)
        source = os.path.join(self.temp_dir, 'filename')
        self.write_string_to_file('testing', source)
        self.do_upload([self.temp_dir])
        self.assertTrue(os.path.exists(os.path.join(self.bundle_location, 'filename')))
        self.assertFalse(os.path.exists(os.path.join(self.bundle_location, '.DS_Store')))
        self.assertFalse(os.path.exists(os.path.join(self.bundle_location, '__MACOSX')))
        self.check_file_contains_string(os.path.join(self.bundle_location, 'filename'), 'testing')

    def test_single_local_gzip_path(self):
        source = os.path.join(self.temp_dir, 'filename.gz')
        self.write_string_to_file(gzip_string('testing'), source)
        self.do_upload([source], unpack=True)
        self.assertTrue(os.path.exists(source))
        self.check_file_contains_string(self.bundle_location, 'testing')

    def test_single_local_tar_gz_path_simplify_archives(self):
        source_dir = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source_dir)
        self.write_string_to_file('testing', os.path.join(source_dir, 'filename'))
        source = os.path.join(self.temp_dir, 'source.tar.gz')
        with open(source, 'wb') as f:
            f.write(tar_gzip_directory(source_dir).read())
        self.do_upload([source], simplify_archives=True)
        self.assertTrue(os.path.exists(source))
        self.check_file_contains_string(self.bundle_location, 'testing')

    def test_single_local_path_remove_sources(self):
        source = os.path.join(self.temp_dir, 'filename')
        self.write_string_to_file('testing', source)
        self.do_upload([source], remove_sources=True)
        self.assertFalse(os.path.exists(source))

    def test_single_local_gzip_path_remove_sources(self):
        source = os.path.join(self.temp_dir, 'filename.gz')
        self.write_string_to_file(gzip_string('testing'), source)
        self.do_upload([source], remove_sources=True)
        self.assertFalse(os.path.exists(source))

    def test_single_fileobj(self):
        self.do_upload([('source', StringIO('testing'))])
        self.check_file_contains_string(self.bundle_location, 'testing')

    def test_single_fileobj_tar_gz_simplify_archives(self):
        source = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source)
        self.write_string_to_file('testing', os.path.join(source, 'filename'))
        self.do_upload([('source.tar.gz', tar_gzip_directory(source))])
        self.check_file_contains_string(self.bundle_location, 'testing')

    def test_single_fileobj_tar_gz_no_simplify_archives(self):
        source = os.path.join(self.temp_dir, 'source_dir')
        os.mkdir(source)
        self.write_string_to_file('testing', os.path.join(source, 'filename'))
        self.do_upload([('source.tar.gz', tar_gzip_directory(source))],
                       simplify_archives=False)
        self.assertEqual(['filename'], os.listdir(self.bundle_location))
        self.check_file_contains_string(os.path.join(self.bundle_location, 'filename'), 'testing')

    def test_multiple_sources(self):
        self.do_upload([('source1', StringIO('testing1')),
                        ('source2', StringIO('testing2'))])
        self.assertItemsEqual(['source1', 'source2'], os.listdir(self.bundle_location))
        self.check_file_contains_string(os.path.join(self.bundle_location, 'source1'), 'testing1')
        self.check_file_contains_string(os.path.join(self.bundle_location, 'source2'), 'testing2')

    def write_string_to_file(self, string, file_path):
        with open(file_path, 'wb') as f:
            f.write(string)

    def check_file_contains_string(self, file_path, string):
        self.assertTrue(os.path.isfile(file_path))
        with open(file_path, 'rb') as f:
            self.assertEqual(f.read(), string)
