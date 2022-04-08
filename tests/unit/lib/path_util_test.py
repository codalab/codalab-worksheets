import errno
import hashlib
from unittest import mock
import os
import unittest

from codalab.common import PreconditionViolation, StorageType, parse_linked_bundle_url
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
            PreconditionViolation, lambda: path_util.get_relative_path('asdfg', 'asdfblah')
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
            path_hash = hashlib.sha1((relative_prefix + directory).encode()).hexdigest()
            directory_hash.update(path_hash.encode())
        file_hash = hashlib.sha1()
        for file_name in sorted(files):
            name_hash = hashlib.sha1((relative_prefix + file_name).encode()).hexdigest()
            file_hash.update(name_hash.encode())
            file_hash.update((contents_hash_prefix + file_name).encode())
        overall_hash = hashlib.sha1()
        overall_hash.update(directory_hash.hexdigest().encode())
        overall_hash.update(file_hash.hexdigest().encode())
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
                with mock.patch(
                    'codalab.lib.path_util.hash_file_contents', mock_hash_file_contents
                ):
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


class ParseBundleUrl(unittest.TestCase):
    def test_single_file(self):
        """Parse a URL referring to a single file on Blob."""
        linked_bundle_path = parse_linked_bundle_url(
            "azfs://storageclwsdev0/bundles/uuid/contents.gz"
        )
        self.assertEqual(linked_bundle_path.storage_type, StorageType.AZURE_BLOB_STORAGE.value)
        self.assertEqual(linked_bundle_path.uses_beam, True)
        self.assertEqual(
            linked_bundle_path.bundle_path, "azfs://storageclwsdev0/bundles/uuid/contents.gz"
        )
        self.assertEqual(linked_bundle_path.is_archive, True)
        self.assertEqual(linked_bundle_path.is_archive_dir, False)
        self.assertEqual(
            linked_bundle_path.index_path, "azfs://storageclwsdev0/bundles/uuid/index.sqlite"
        )
        self.assertEqual(linked_bundle_path.archive_subpath, None)
        self.assertEqual(linked_bundle_path.bundle_uuid, "uuid")

        linked_bundle_path = parse_linked_bundle_url("gs://codalabbucket1/uuid/contents.gz")
        self.assertEqual(linked_bundle_path.storage_type, StorageType.GCS_STORAGE.value)
        self.assertEqual(linked_bundle_path.uses_beam, True)
        self.assertEqual(linked_bundle_path.bundle_path, "gs://codalabbucket1/uuid/contents.gz")
        self.assertEqual(linked_bundle_path.is_archive, True)
        self.assertEqual(linked_bundle_path.is_archive_dir, False)
        self.assertEqual(linked_bundle_path.index_path, "gs://codalabbucket1/uuid/index.sqlite")
        self.assertEqual(linked_bundle_path.archive_subpath, None)
        self.assertEqual(linked_bundle_path.bundle_uuid, "uuid")

    def test_directory(self):
        """Parse a URL referring to an archived directory."""
        linked_bundle_path = parse_linked_bundle_url(
            "azfs://storageclwsdev0/bundles/uuid/contents.tar.gz"
        )
        self.assertEqual(linked_bundle_path.storage_type, StorageType.AZURE_BLOB_STORAGE.value)
        self.assertEqual(
            linked_bundle_path.bundle_path, "azfs://storageclwsdev0/bundles/uuid/contents.tar.gz"
        )
        self.assertEqual(linked_bundle_path.is_archive, True)
        self.assertEqual(linked_bundle_path.is_archive_dir, True)
        self.assertEqual(
            linked_bundle_path.index_path, "azfs://storageclwsdev0/bundles/uuid/index.sqlite"
        )
        self.assertEqual(linked_bundle_path.archive_subpath, None)
        self.assertEqual(linked_bundle_path.bundle_uuid, "uuid")

    def test_directory_with_subpath(self):
        """Parse a URL referring to a subpath within an archived directory."""
        linked_bundle_path = parse_linked_bundle_url(
            "azfs://storageclwsdev0/bundles/uuid/contents.tar.gz/a/b.txt"
        )
        self.assertEqual(linked_bundle_path.storage_type, StorageType.AZURE_BLOB_STORAGE.value)
        self.assertEqual(
            linked_bundle_path.bundle_path, "azfs://storageclwsdev0/bundles/uuid/contents.tar.gz"
        )
        self.assertEqual(linked_bundle_path.is_archive, True)
        self.assertEqual(linked_bundle_path.is_archive_dir, True)
        self.assertEqual(
            linked_bundle_path.index_path, "azfs://storageclwsdev0/bundles/uuid/index.sqlite"
        )
        self.assertEqual(linked_bundle_path.archive_subpath, "a/b.txt")
        self.assertEqual(linked_bundle_path.bundle_uuid, "uuid")

    def test_non_azure_file(self):
        """Should parse a non-Azure URL properly."""
        linked_bundle_path = parse_linked_bundle_url(
            "/tmp/storageclwsdev0/bundles/uuid/contents.txt"
        )
        self.assertEqual(linked_bundle_path.storage_type, StorageType.DISK_STORAGE.value)
        self.assertEqual(linked_bundle_path.uses_beam, False)
        self.assertEqual(
            linked_bundle_path.bundle_path, "/tmp/storageclwsdev0/bundles/uuid/contents.txt"
        )
        self.assertEqual(linked_bundle_path.is_archive, False)

    def test_container(self):
        """Parse a URL referring to a container or bucket."""
        linked_bundle_path = parse_linked_bundle_url("gs://codalab-test")
        self.assertEqual(linked_bundle_path.storage_type, StorageType.GCS_STORAGE.value)
        linked_bundle_path = parse_linked_bundle_url("azfs://devstoreaccount1/bundles")
        self.assertEqual(linked_bundle_path.storage_type, StorageType.AZURE_BLOB_STORAGE.value)
