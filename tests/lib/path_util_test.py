import errno
import hashlib
import mock
import os
import unittest

from codalab.common import PreconditionViolation
from codalab.lib import path_util


class PathUtilTest(unittest.TestCase):
  test_path = '/my/test/path'
  mkdir_calls = [[(test_path,), {}]]

  @mock.patch('codalab.lib.path_util.os', new_callable=lambda: None)
  def test_get_relative_path(self, mock_os):
    '''
    Test that get_relative_path checks if the root is a prefix of the path,
    and if so, returns the path's suffix.
    '''
    # The path is a prefix, so get_relative_path should return the suffix.
    self.assertEqual(path_util.get_relative_path('asdf', 'asdfblah'), 'blah')
    self.assertRaises(
      PreconditionViolation,
      lambda: path_util.get_relative_path('asdfg', 'asdfblah'),
    )

  @mock.patch('codalab.lib.path_util.os', new_callable=lambda: None)
  def test_hash_directory(self, mock_os):
    '''
    Test the two-level hashing scheme, mocking out all filesystem operations.
    '''
    tester = self
    directories = ['asdf', 'blah', 'this', 'is', 'not', 'sorted']
    files = ['foo', 'bar']
    relative_prefix = 'relative-'
    contents_hash_prefix = 'contents-hash-'

    # Compute the result of the the two-level hashing scheme on this bundle.
    directory_hash = hashlib.sha1()
    for directory in sorted(directories):
      path_hash = hashlib.sha1(relative_prefix + directory).hexdigest()
      directory_hash.update(path_hash)
    file_hash = hashlib.sha1()
    for file_name in sorted(files):
      name_hash = hashlib.sha1(relative_prefix + file_name).hexdigest()
      file_hash.update(name_hash)
      file_hash.update(contents_hash_prefix + file_name)
    overall_hash = hashlib.sha1()
    overall_hash.update(directory_hash.hexdigest())
    overall_hash.update(file_hash.hexdigest())
    expected_hash = overall_hash.hexdigest()

    # Mock the recursive-listing and file-hashing operations in path_util.
    def mock_recursive_ls(path):
      tester.assertEqual(path, self.test_path)
      return (directories, files)

    def mock_get_relative_path(root, path):
      tester.assertEqual(root, self.test_path)
      tester.assertIn(path, directories + files)
      return relative_prefix + path

    def mock_hash_file_contents(path):
      tester.assertIn(path, files)
      return contents_hash_prefix + path

    with mock.patch('codalab.lib.path_util.recursive_ls', mock_recursive_ls):
      with mock.patch('codalab.lib.path_util.get_relative_path', mock_get_relative_path):
        with mock.patch('codalab.lib.path_util.hash_file_contents', mock_hash_file_contents):
          actual_hash = path_util.hash_directory(self.test_path) 
    self.assertEqual(actual_hash, expected_hash)

  @mock.patch('codalab.lib.path_util.os')
  def test_make_directory(self, mock_os):
    '''
    Check that make_directory calls normalize and then creates the directory.
    '''
    mock_os.path.join = os.path.join
    path_util.make_directory(self.test_path)
    self.assertEqual(mock_os.mkdir.call_args_list, self.mkdir_calls)

  @mock.patch('codalab.lib.path_util.os')
  def test_make_directory_if_exists(self, mock_os):
    '''
    Check that make_directory still works if the directory exists.
    '''
    mock_os.path.join = os.path.join
    failures = [0]
    def mkdir_when_directory_exists(path):
      failures[0] += 1
      error = OSError()
      error.errno = errno.EEXIST
      raise error
    mock_os.mkdir.side_effect = mkdir_when_directory_exists
    path_util.make_directory(self.test_path)
    self.assertEqual(mock_os.mkdir.call_args_list, self.mkdir_calls)
    self.assertEqual(failures[0], 1)

  @mock.patch('codalab.lib.path_util.os')
  def test_make_directory_with_failures(self, mock_os):
    '''
    Check that make_directory still works if the directory exists.
    '''
    mock_os.path.join = os.path.join
    def mkdir_with_other_failure(path):
      raise OSError()
    mock_os.mkdir.reset_mock()
    mock_os.mkdir.side_effect = mkdir_with_other_failure
    self.assertRaises(OSError, lambda: path_util.make_directory(self.test_path))
    self.assertEqual(mock_os.mkdir.call_args_list, self.mkdir_calls)
