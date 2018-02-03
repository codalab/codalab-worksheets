import os
import tempfile
import unittest
import bz2

from codalabworker.file_util import gzip_file, gzip_string, remove_path, tar_gzip_directory, un_gzip_stream, un_bz2_file, un_gzip_string, un_tar_directory


class FileUtilTest(unittest.TestCase):
    def test_tar_has_files(self):
        dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'files')
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: remove_path(temp_dir))

        output_dir = os.path.join(temp_dir, 'output')
        un_tar_directory(
            tar_gzip_directory(dir, False, ['f2'], ['f1', 'b.txt']),
            output_dir, 'gz')
        output_dir_entries = os.listdir(output_dir)
        self.assertIn('dir1', output_dir_entries)
        self.assertIn('a.txt', output_dir_entries)
        self.assertNotIn('b.txt', output_dir_entries)
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'dir1', 'f1')))
        self.assertFalse(os.path.exists(os.path.join(output_dir, 'dir1', 'f2')))
        self.assertTrue(os.path.islink(os.path.join(output_dir, 'a-symlink.txt')))

    def test_tar_empty(self):
        dir = tempfile.mkdtemp()
        self.addCleanup(lambda: remove_path(dir))
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: remove_path(temp_dir))

        output_dir = os.path.join(temp_dir, 'output')
        un_tar_directory(tar_gzip_directory(dir), output_dir, 'gz')
        self.assertEquals(os.listdir(output_dir), [])

    def test_gzip_stream(self):
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            self.addCleanup(lambda: os.remove(temp_file.name))
            temp_file.write('contents')
            name = temp_file.name

        self.assertEquals(un_gzip_stream(gzip_file(name)).read(), 'contents')

    def test_bz2_file(self):
        source_write = tempfile.NamedTemporaryFile(delete=False)
        self.addCleanup(lambda: os.remove(source_write.name))
        source_write.write(bz2.compress('contents'))
        source_write.flush()
        source_read = open(source_write.name, 'r')
        destination = tempfile.NamedTemporaryFile(delete=False)
        self.addCleanup(lambda: os.remove(destination.name))
        un_bz2_file(source_read, destination.name)
        self.assertEquals(destination.read(), 'contents')
        source_write.close()
        source_read.close()
        destination.close()

    def test_gzip_string(self):
        self.assertEqual(un_gzip_string(gzip_string('contents')), 'contents')

