import errno
import mock
import os
import sys
import unittest

from codalab.lib.bundle_store import SingleDiskBundleStore

class BundleStoreTest(unittest.TestCase):
    @mock.patch('codalab.lib.bundle_store.path_util')
    @mock.patch('codalab.lib.bundle_store.os', new_callable=mock.Mock)
    def test_upload(self, mock_os, mock_path_util):
        '''
        Tries to upload a bundle: should copy bundle into temp, hash and move
        it in to the data directory.
        '''
        global_paths = set()  # Paths that exist on the mock file system

        ### BundleStore

        class MockBundleStore(SingleDiskBundleStore):
          def __init__(self, root):
            self.root = root
            self.data = os.path.join(root, SingleDiskBundleStore.DATA_SUBDIRECTORY)
            self.temp = os.path.join(root, SingleDiskBundleStore.TEMP_SUBDIRECTORY)

        bundle_store = MockBundleStore('mock_root')

        ### os.path

        mock_os.path = mock.Mock()
        mock_os.path.join = os.path.join
        mock_os.path.basename = os.path.basename
        mock_os.path.realpath = lambda x : x

        def exists(path):
            return path in global_paths
        mock_os.path.exists = exists

        ### path_util

        mock_path_util.normalize = lambda x : x
        mock_path_util.recursive_ls = lambda x : []
        mock_path_util.path_is_url = lambda x : False

        def copy(source_path, dest_path, follow_symlinks, exclude_patterns):
            print 'copy', source_path, dest_path
            self.assertIn(source_path, global_paths)
            self.assertNotIn(dest_path, global_paths)
            global_paths.add(dest_path)
        mock_path_util.copy = copy

        def rename(source_path, dest_path):
            print 'rename', source_path, dest_path
            self.assertIn(source_path, global_paths)
            self.assertNotIn(dest_path, global_paths)
            global_paths.remove(source_path)
            global_paths.add(dest_path)
        mock_path_util.rename = rename

        def remove(path):
            print 'remove', path
            self.assertIn(path, global_paths)
            global_paths.remove(path)
        mock_path_util.remove = remove

        def hash_directory(path, dirs_and_files=None):
            self.assertIn(path, global_paths)
            return '12345'
        mock_path_util.hash_directory = hash_directory

        ### Main: upload!

        contents = 'contents'
        test_uuid1 = '0xdeadbeef'
        test_uuid2 = '0xaaaaaaaa'
        final_path1 = 'mock_root/bundles/%s' % test_uuid1
        final_path2 = 'mock_root/bundles/%s' % test_uuid2
        global_paths.add(contents)

        bundle_store.upload(sources=[contents], follow_symlinks=False, exclude_patterns=None, git=False, unpack=False, remove_sources=False, uuid=test_uuid1)
        self.assertEquals(global_paths, set([contents, final_path1]))

        bundle_store.upload(sources=[contents], follow_symlinks=False, exclude_patterns=None, git=False, unpack=False, remove_sources=True, uuid=test_uuid2)
        self.assertNotIn(contents, global_paths)  # File is not there
        self.assertEquals(global_paths, set([final_path1, final_path2]))
