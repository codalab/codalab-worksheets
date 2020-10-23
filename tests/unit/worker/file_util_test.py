import os
import tempfile
import unittest
import bz2

from codalab.worker.file_util import (
    gzip_file,
    get_file_size,
    gzip_bytestring,
    remove_path,
    tar_gzip_directory,
    un_gzip_stream,
    un_bz2_file,
    un_gzip_bytestring,
    un_tar_directory,
    read_file_section,
    zip_directory,
    unzip_directory,
)

FILES_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'cli', 'files')
IGNORE_TEST_DIR = os.path.join(FILES_DIR, 'ignore_test')


class FileUtilTest(unittest.TestCase):
    def test_get_file_size(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"hello world")
        self.assertEqual(get_file_size(f.name), 11)

    def test_read_file_section(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"hello world")
        self.assertEqual(read_file_section(f.name, 2, 4), b"llo ")

    def test_gzip_stream(self):
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            self.addCleanup(lambda: os.remove(temp_file.name))
            temp_file.write(b'contents')
            name = temp_file.name

        self.assertEqual(un_gzip_stream(gzip_file(name)).read(), b'contents')

    def test_bz2_file(self):
        source_write = tempfile.NamedTemporaryFile(delete=False)
        self.addCleanup(lambda: os.remove(source_write.name))
        source_write.write(bz2.compress(b'contents'))
        source_write.flush()
        source_read = open(source_write.name, 'rb')
        destination = tempfile.NamedTemporaryFile(delete=False)
        self.addCleanup(lambda: os.remove(destination.name))
        un_bz2_file(source_read, destination.name)
        self.assertEqual(destination.read(), b'contents')
        source_write.close()
        source_read.close()
        destination.close()

    def test_gzip_bytestring(self):
        self.assertEqual(un_gzip_bytestring(gzip_bytestring(b'contents')), b'contents')


class ArchiveTestBase:
    """Base for archive tests -- tests both archiving and unarchiving directories.
    Subclasses must implement the archive() and unarchive() methods."""

    def archive(self, *args, **kwargs):
        raise NotImplementedError

    def unarchive(self, *args, **kwargs):
        raise NotImplementedError

    def test_has_files(self):
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: remove_path(temp_dir))

        output_dir = os.path.join(temp_dir, 'output')
        self.unarchive(self.archive(FILES_DIR, False, ['f2'], ['f1', 'b.txt']), output_dir, 'gz')
        output_dir_entries = os.listdir(output_dir)
        self.assertIn('dir1', output_dir_entries)
        self.assertIn('a.txt', output_dir_entries)
        self.assertNotIn('b.txt', output_dir_entries)
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'dir1', 'f1')))
        self.assertFalse(os.path.exists(os.path.join(output_dir, 'dir1', 'f2')))
        self.assertTrue(os.path.islink(os.path.join(output_dir, 'a-symlink.txt')))

    def test_empty(self):
        dir = tempfile.mkdtemp()
        self.addCleanup(lambda: remove_path(dir))
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: remove_path(temp_dir))

        output_dir = os.path.join(temp_dir, 'output')
        self.unarchive(self.archive(dir), output_dir, 'gz')
        self.assertEqual(os.listdir(output_dir), [])

    def test_exclude_ignore(self):
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: remove_path(temp_dir))
        output_dir = os.path.join(temp_dir, 'output')

        self.unarchive(self.archive(IGNORE_TEST_DIR, ignore_file='.tarignore'), output_dir, 'gz')
        output_dir_entries = os.listdir(output_dir)
        self.assertIn('not_ignored.txt', output_dir_entries)
        self.assertIn('dir', output_dir_entries)
        self.assertNotIn('ignored.txt', output_dir_entries)
        self.assertNotIn('ignored_dir', output_dir_entries)
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'dir', 'not_ignored2.txt')))
        self.assertFalse(os.path.exists(os.path.join(output_dir, 'dir', 'ignored2.txt')))

    def test_always_ignore(self):
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: remove_path(temp_dir))
        output_dir = os.path.join(temp_dir, 'output')

        self.unarchive(self.archive(IGNORE_TEST_DIR), output_dir, 'gz')
        output_dir_entries = os.listdir(output_dir)
        self.assertNotIn('._ignored', output_dir_entries)
        self.assertIn('dir', output_dir_entries)
        self.assertNotIn('__MACOSX', output_dir_entries)
        self.assertFalse(os.path.exists(os.path.join(output_dir, 'dir', '__MACOSX')))
        self.assertFalse(os.path.exists(os.path.join(output_dir, 'dir', '._ignored2')))


class TarArchiveTest(ArchiveTestBase, unittest.TestCase):
    """Archive test for tar/gzip methods."""

    def archive(self, *args, **kwargs):
        return tar_gzip_directory(*args, **kwargs)

    def unarchive(self, *args, **kwargs):
        return un_tar_directory(*args, **kwargs)


class ZipArchiveTest(ArchiveTestBase, unittest.TestCase):
    """Archive test for zip methods."""

    def archive(self, *args, **kwargs):
        return zip_directory(*args, **kwargs)

    def unarchive(self, *args, **kwargs):
        return unzip_directory(*args, **kwargs)

    def test_empty(self):
        # zip doesn't create files when it's supposed to create an empty zip file.
        pass

    def test_exclude_ignore(self):
        # TODO(Ashwin): make zip files properly work with exclude ignore.
        pass
