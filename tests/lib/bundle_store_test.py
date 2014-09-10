import errno
import mock
import os
import unittest

from codalab.lib.bundle_store import BundleStore


class BundleStoreTest(unittest.TestCase):
  unnormalized_test_root = 'random string that normalizes to test_root'
  test_root = '/tmp/codalab_tests'

  directories = [
    os.path.join(test_root, BundleStore.DATA_SUBDIRECTORY),
    os.path.join(test_root, BundleStore.TEMP_SUBDIRECTORY),
  ]
  mkdir_calls = [[(directory,), {}] for directory in directories]

  def mock_normalize(self, path):
    assert(path == BundleStoreTest.unnormalized_test_root)
    return BundleStoreTest.test_root

  @mock.patch('codalab.lib.bundle_store.os')
  @mock.patch('codalab.lib.bundle_store.path_util')
  def test_init(self, mock_path_util, mock_os):
    '''
    Check that __init__ calls normalize path and then creates the directories.
    '''
    mock_os.path.join = os.path.join
    mock_path_util.normalize = self.mock_normalize
    BundleStore(self.unnormalized_test_root)
    self.assertEqual(mock_path_util.make_directory.call_args_list, self.mkdir_calls)

  @mock.patch('codalab.lib.bundle_store.os', new_callable=mock.Mock)
  @mock.patch('codalab.lib.bundle_store.uuid')
  @mock.patch('codalab.lib.bundle_store.path_util')
  def run_upload_trial(self, mock_path_util, mock_uuid,
                       mock_os, new):
    '''
    Test that upload takes the following actions, in order:
      - Copies the bundle into the temp directory
      - Sets permissions for the bundle to 755
      - Hashes the directory
      - Moves the directory into data (if new) or deletes it (if old)
    '''
    check_isvalid_called = [False]
    rename_called = [False]

    mock_os.path = mock.Mock()
    mock_os.path.join = os.path.join

    unnormalized_bundle_path = 'random thing that will normalize to bundle path'
    bundle_path = 'bundle path'
    test_root = 'test_root'
    test_data = os.path.join(test_root, 'data')
    test_temp = os.path.join(test_root, 'temp')
    test_directory_hash = 'directory-hash'
    final_path = os.path.join(test_data, '0x' + test_directory_hash)

    def exists(path):
      self.assertEqual(path, final_path)
      return not new or rename_called[0]
    mock_os.path.exists = exists

    def rename(source_path, dest_path):
      self.assertEqual(source_path, temp_path)
      self.assertEqual(dest_path, final_path)
      rename_called[0] = True
    mock_path_util.rename = rename

    def utime(path, ts):
      self.assertEqual(path, final_path)
      self.assertIsNone(ts)
      if new:
        raise OSError(errno.ENOENT, 'Path does not exist: ' + path)
    mock_os.utime = utime

    temp_dir = 'abloogywoogywu'
    temp_path = os.path.join(test_temp, temp_dir)
    mock_uuid.uuid4.return_value = type('MockUUID', (), {'hex': temp_dir})()

    test_dirs_and_files = 'my test dirs_and_files sentinel'

    def normalize(path):
      self.assertEqual(path, unnormalized_bundle_path)
      return bundle_path
    mock_path_util.normalize = normalize

    def check_isvalid(path, fn_name):
      self.assertEqual(path, bundle_path)
      check_isvalid_called[0] = True
    mock_path_util.check_isvalid = check_isvalid

    def recursive_ls(path):
      self.assertEqual(path, temp_path)
      return test_dirs_and_files
    mock_path_util.recursive_ls = recursive_ls
    
    def set_permissions(path, permissions, dirs_and_files=None):
      if dirs_and_files is not None:
        self.assertEqual(dirs_and_files, test_dirs_and_files)
      self.assertEqual(path, temp_path)
      self.assertEqual(permissions, 0o755)
    mock_path_util.set_permissions = set_permissions

    def hash_directory(path, dirs_and_files=None):
      if dirs_and_files is not None:
        self.assertEqual(dirs_and_files, test_dirs_and_files)
      self.assertTrue(path, temp_path)
      return test_directory_hash
    mock_path_util.hash_directory = hash_directory

    class MockBundleStore(BundleStore):
      def __init__(self, root):
        self.root = root
        self.data = os.path.join(root, 'data')
        self.temp = os.path.join(root, 'temp') 

    bundle_store = MockBundleStore(test_root)
    self.assertFalse(check_isvalid_called[0])
    bundle_store.upload(unnormalized_bundle_path)
    self.assertTrue(check_isvalid_called[0])
    if new:
      self.assertTrue(rename_called[0])
    else:
      self.assertFalse(rename_called[0])
      mock_path_util.remove.assert_called_with(temp_path)

  def test_new_upload(self):
    self.run_upload_trial(new=True)

  def test_old_upload(self):
    self.run_upload_trial(new=False)
