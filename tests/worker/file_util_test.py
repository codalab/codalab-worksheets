import os
import tempfile
import unittest

from worker.file_util import gzip_file, gzip_string, remove_path, tar_gzip_directory, un_gzip_stream, un_gzip_string, un_tar_gzip_directory


class FileUtilTest(unittest.TestCase):
    def test_tar_has_files(self):
        dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'files')
        output_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: remove_path(output_dir))

        un_tar_gzip_directory(
            tar_gzip_directory(dir, False, ['f2'], ['f1', 'b.txt']),
            output_dir)
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
        output_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: remove_path(output_dir))

        un_tar_gzip_directory(tar_gzip_directory(dir), output_dir)
        self.assertEquals(os.listdir(output_dir), [])

    def test_gzip_stream(self):
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            self.addCleanup(lambda: os.remove(temp_file.name))    
            temp_file.write('contents')
            name = temp_file.name

        self.assertEquals(un_gzip_stream(gzip_file(name)).read(), 'contents')

    def test_gzip_string(self):
        self.assertEqual(un_gzip_string(gzip_string('contents')), 'contents')
    