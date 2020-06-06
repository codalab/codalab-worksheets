import os
import tempfile
import unittest
import bz2
import gzip

from codalab.worker.file_util import (
    gzip_file,
    gzip_bytestring,
    remove_path,
    tar_gzip_directory,
    un_gzip_stream,
    un_bz2_file,
    un_gzip_bytestring,
    un_tar_directory,
    read_file_section,
    summarize_file,
    get_path_size,
    get_path_exists,
)


class FileUtilTest(unittest.TestCase):
    def test_tar_has_files(self):
        dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'files')
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: remove_path(temp_dir))

        output_dir = os.path.join(temp_dir, 'output')
        un_tar_directory(tar_gzip_directory(dir, False, ['f2'], ['f1', 'b.txt']), output_dir, 'gz')
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
        self.assertEqual(os.listdir(output_dir), [])

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

    def test_tar_exclude_ignore(self):
        dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'files/ignore_test')
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: remove_path(temp_dir))
        output_dir = os.path.join(temp_dir, 'output')

        un_tar_directory(tar_gzip_directory(dir, ignore_file='.tarignore'), output_dir, 'gz')
        output_dir_entries = os.listdir(output_dir)
        self.assertIn('not_ignored.txt', output_dir_entries)
        self.assertIn('dir', output_dir_entries)
        self.assertNotIn('ignored.txt', output_dir_entries)
        self.assertNotIn('ignored_dir', output_dir_entries)
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'dir', 'not_ignored2.txt')))
        self.assertFalse(os.path.exists(os.path.join(output_dir, 'dir', 'ignored2.txt')))

    def test_tar_always_ignore(self):
        dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'files/ignore_test')
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: remove_path(temp_dir))
        output_dir = os.path.join(temp_dir, 'output')

        un_tar_directory(tar_gzip_directory(dir), output_dir, 'gz')
        output_dir_entries = os.listdir(output_dir)
        self.assertNotIn('._ignored', output_dir_entries)
        self.assertIn('dir', output_dir_entries)
        self.assertNotIn('__MACOSX', output_dir_entries)
        self.assertFalse(os.path.exists(os.path.join(output_dir, 'dir', '__MACOSX')))
        self.assertFalse(os.path.exists(os.path.join(output_dir, 'dir', '._ignored2')))

    def test_gzip_file(self):
        with tempfile.NamedTemporaryFile() as f:
            f.write(b"hello world")
            f.seek(0)
            gzipped_file = gzip_file(f.name)
            with gzip.GzipFile(fileobj=gzipped_file) as gzf:
                self.assertEqual(gzf.read(), b"hello world")

    def test_read_file_section(self):
        with tempfile.NamedTemporaryFile() as f:
            f.write(b"hello world")
            f.seek(0)
            results = read_file_section(f.name, 2, 5)
            self.assertEqual(results, b"llo w")

    def test_summarize_file(self):
        with tempfile.NamedTemporaryFile() as f:
            f.write(b"1\n2\n3\n4\n5\n6\n7\n8\n9\n10 loooooooooong line\n11")
            f.seek(0)
            results = summarize_file(f.name, 2, 4, 9, "...")
            self.assertEqual(results, "1\n2\n...8\n9\n10 loooooooooong line\n11\n")

    def test_summarize_file_binary(self):
        with tempfile.NamedTemporaryFile() as f:
            with gzip.GzipFile(fileobj=f, mode="wb") as gzf:
                gzf.write(b"hello world")
            f.seek(0)
            results = summarize_file(f.name, 2, 4, 9, "...")
            self.assertEqual(results, "<binary>")

    def test_summarize_file_notfound(self):
        results = summarize_file("invalid file name", 2, 4, 9, "...")
        self.assertEqual(results, "<none>")

    def test_get_path_size(self):
        with tempfile.NamedTemporaryFile() as f:
            with gzip.GzipFile(fileobj=f, mode="wb") as gzf:
                gzf.write(b"hello world")
            f.seek(0)
            results = get_path_size(f.name)
            self.assertEqual(results, 43)

    def test_get_path_size_dir(self):
        with tempfile.TemporaryDirectory() as dirname, tempfile.NamedTemporaryFile(
            dir=dirname
        ) as f:
            f.write(b"hello world")
            f.seek(0)
            results = get_path_size(dirname)
            self.assertEqual(results, 4107)

    def test_get_path_size_nested_dir(self):
        with tempfile.TemporaryDirectory() as dirname, tempfile.NamedTemporaryFile(
            dir=dirname
        ) as f, tempfile.TemporaryDirectory(dir=dirname) as dirname2, tempfile.NamedTemporaryFile(
            dir=dirname2
        ) as f2:
            f.write(b"hello world")
            f.seek(0)
            f2.write(b"hello world")
            f2.seek(0)
            results = get_path_size(dirname)
            self.assertEqual(results, 8215)

    def test_remove_path(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            self.assertEqual(get_path_exists(f.name), True)
            remove_path(f.name)
            self.assertEqual(get_path_exists(f.name), False)

    def test_remove_path_dir(self):
        dirname = tempfile.mkdtemp()
        with tempfile.NamedTemporaryFile(dir=dirname, delete=False) as f:
            self.assertEqual(get_path_exists(dirname), True)
            self.assertEqual(get_path_exists(f.name), True)
            remove_path(dirname)
            self.assertEqual(get_path_exists(dirname), False)
            self.assertEqual(get_path_exists(f.name), False)
