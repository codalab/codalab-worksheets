import hashlib
import os
import shutil
import stat
import tempfile
import unittest

from codalab.lib import path_util


class PathUtilFSTest(unittest.TestCase):
  contents = 'random file contents'

  def setUp(self):
    self.temp_directory = tempfile.mkdtemp()
    assert(os.path.isabs(self.temp_directory)), \
      'tempfile.mkdtemp returned relative root: %s' % (self.temp_directory,)

    self.bundle_path = os.path.join(self.temp_directory, 'test_bundle')
    self.bundle_directories = [
      self.bundle_path,
      os.path.join(self.bundle_path, 'asdf'),
      os.path.join(self.bundle_path, 'asdf', 'craw'),
      os.path.join(self.bundle_path, 'blah'),
    ]
    self.bundle_files = [
      os.path.join(self.bundle_path, 'foo'),
      os.path.join(self.bundle_path, 'asdf', 'bar'),
      os.path.join(self.bundle_path, 'asdf', 'baz'),
    ]

    for directory in self.bundle_directories:
      os.mkdir(directory)
    for file_name in self.bundle_files:
      with open(file_name, 'w') as fd:
        fd.write(self.contents)

  def tearDown(self):
    shutil.rmtree(self.temp_directory)

  def test_normalize(self):
    test_pairs = [
      ('~', os.path.expanduser('~')),
      (os.curdir, os.getcwd()),
      (os.pardir, os.path.abspath(os.path.join(os.getcwd(), os.pardir))),
    ]
    for (test_path, expected_result) in test_pairs:
      actual_result = path_util.normalize(test_path)
      self.assertTrue(os.path.isabs(actual_result))
      self.assertEqual(actual_result, expected_result)
      # Test idempotency. An absolute path be a fixed point of normalize.
      self.assertEqual(path_util.normalize(actual_result), actual_result)

  def test_recursive_ls(self):
    '''
    Test that recursive_ls lists all absolute paths within a directory.
    '''
    (directories, files) = path_util.recursive_ls(self.bundle_path)
    self.assertEqual(set(directories), set(self.bundle_directories))
    self.assertEqual(set(files), set(self.bundle_files))

  def test_hash_file_contests(self):
    '''
    Test that hash_file_contents reads a file and hashes its contents.
    '''
    self.assertNotEqual(path_util.FILE_PREFIX, path_util.LINK_PREFIX)
    # Check that files are hashed with a file prefix.
    # TODO(skishore): Try this test with a much larger file.
    expected_hash = hashlib.sha1(path_util.FILE_PREFIX + self.contents).hexdigest()
    for path in self.bundle_files:
      file_hash = path_util.hash_file_contents(path)
      self.assertEqual(file_hash, expected_hash)
    # Check that links are hashed with a link prefix.
    link_target = '../some/random/thing/to/symlink/to'
    expected_hash = hashlib.sha1(path_util.LINK_PREFIX + link_target).hexdigest()
    symlink_path = os.path.join(self.bundle_directories[-1], 'my_symlink')
    os.symlink(link_target, symlink_path)
    link_hash = path_util.hash_file_contents(symlink_path)
    self.assertEqual(link_hash, expected_hash)
