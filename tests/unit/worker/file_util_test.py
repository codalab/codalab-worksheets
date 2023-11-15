import tests.unit.azure_blob_mock  # noqa: F401
import os
import tarfile
import tempfile
import unittest
import bz2
import gzip

from io import BytesIO

from codalab.worker.file_util import (
    gzip_file,
    get_file_size,
    gzip_bytestring,
    remove_path,
    tar_gzip_directory,
    un_bz2_file,
    un_gzip_bytestring,
    read_file_section,
    zip_directory,
    unzip_directory,
    OpenFile,
    summarize_file,
)
from codalab.worker.un_gzip_stream import un_gzip_stream, ZipToTarStream, BytesBuffer
from codalab.worker.un_tar_directory import un_tar_directory
from tests.unit.worker.download_util_test import AzureBlobTestBase

FILES_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'cli', 'files')
IGNORE_TEST_DIR = os.path.join(FILES_DIR, 'ignore_test')

SAMPLE_CONTENTS = b"hello world"


class ReadOneByOne(BytesIO):
    """A wrapper that reads a fileobj one by one. Calls to .read(n) will only
    return one byte. This simulates, for example, a HTTP request body that is
    going to the server one byte at a time."""

    def __init__(self, fileobj):
        self._fileobj = fileobj

    def read(self, num_bytes=None):
        return self._fileobj.read(min(1, num_bytes or 0))

    def __getattr__(self, name):
        """
        Proxy any methods/attributes to the fileobj.
        """
        return getattr(self._fileobj, name)


class FileUtilTest(unittest.TestCase):
    def test_get_file_size(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"hello world")
        self.assertEqual(get_file_size(f.name), 11)

    def test_read_file_section(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"hello world")
        self.assertEqual(read_file_section(f.name, 2, 4), b"llo ")
        self.assertEqual(read_file_section(f.name, 100, 4), b"")

    def test_summarize_file(self):
        with tempfile.NamedTemporaryFile() as f:
            f.write(("aaa\nbbb\n").encode())
            f.flush()
            self.assertEqual(
                summarize_file(
                    f.name,
                    num_head_lines=1,
                    num_tail_lines=0,
                    max_line_length=4,
                    truncation_text="....",
                ),
                "aaa\n",
            )
            self.assertEqual(
                summarize_file(
                    f.name,
                    num_head_lines=0,
                    num_tail_lines=1,
                    max_line_length=4,
                    truncation_text="....",
                ),
                "bbb\n",
            )
            self.assertEqual(
                summarize_file(
                    f.name,
                    num_head_lines=1,
                    num_tail_lines=1,
                    max_line_length=4,
                    truncation_text="....",
                ),
                "aaa\nbbb\n",
            )
            # Should not recognize a line if max_line_length is smaller than the actual line length (4)
            self.assertEqual(
                summarize_file(
                    f.name,
                    num_head_lines=1,
                    num_tail_lines=0,
                    max_line_length=3,
                    truncation_text="....",
                ),
                "",
            )
            self.assertEqual(
                summarize_file(
                    f.name,
                    num_head_lines=0,
                    num_tail_lines=1,
                    max_line_length=3,
                    truncation_text="....",
                ),
                "",
            )
            self.assertEqual(
                summarize_file(
                    f.name,
                    num_head_lines=1,
                    num_tail_lines=1,
                    max_line_length=3,
                    truncation_text="....",
                ),
                "....",
            )

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

    def create_zip_single_file(self):
        """Create a simple .zip file with a single file in it."""
        with tempfile.TemporaryDirectory() as tmpdir, open(
            os.path.join(tmpdir, "file.txt"), "wb"
        ) as f:
            f.write(SAMPLE_CONTENTS)
            f.flush()
            zip_contents = zip_directory(tmpdir).read()
            return zip_contents

    def create_zip_complex(self):
        """Create a complex .zip file with files / directories / nested directories in it."""
        with tempfile.TemporaryDirectory() as tmpdir:
            os.makedirs(os.path.join(tmpdir, "a/b"))
            os.makedirs(os.path.join(tmpdir, "c/d/e"))
            with open(os.path.join(tmpdir, "file.txt"), "wb") as f, open(
                os.path.join(tmpdir, "a", "b", "file.txt"), "wb"
            ) as f2:
                f.write(SAMPLE_CONTENTS)
                f.flush()
                f2.write(SAMPLE_CONTENTS)
                f2.flush()
                zip_contents = zip_directory(tmpdir).read()
                return zip_contents

    def test_zip_to_tar_single(self):
        """Test converting a zip to a tar stream with a single file in the tar archive."""
        zip_contents = self.create_zip_single_file()
        with tarfile.open(fileobj=ZipToTarStream(BytesIO(zip_contents)), mode="r|") as tf:
            for tinfo in tf:
                self.assertEqual(tinfo.name, "file.txt")
                self.assertEqual(tinfo.size, 11)
                self.assertEqual(tinfo.type, tarfile.REGTYPE)
                self.assertEqual(tf.extractfile(tinfo).read(), b"hello world")

    def test_zip_to_tar_single_read_partial(self):
        """Test converting a zip to a tar stream with a single file in the tar archive,
        while partially reading the file within the archive."""
        zip_contents = self.create_zip_single_file()
        with tarfile.open(fileobj=ZipToTarStream(BytesIO(zip_contents)), mode="r|") as tf:
            for tinfo in tf:
                self.assertEqual(tinfo.name, "file.txt")
                self.assertEqual(tinfo.size, 11)
                self.assertEqual(tinfo.type, tarfile.REGTYPE)
                with tf.extractfile(tinfo) as f:
                    self.assertEqual(f.read(1), b"h")
                    self.assertEqual(f.read(1), b"e")
                    self.assertEqual(f.read(1), b"l")
                    self.assertEqual(f.read(1), b"l")
                    self.assertEqual(f.read(1), b"o")
                    self.assertEqual(f.read(1), b" ")
                    self.assertEqual(f.read(1), b"w")
                    self.assertEqual(f.read(1), b"o")
                    self.assertEqual(f.read(1), b"r")
                    self.assertEqual(f.read(1), b"l")
                    self.assertEqual(f.read(1), b"d")

    def test_zip_to_tar_complex(self):
        """Test converting a zip to a tar stream with a complex set of files in the tar archive."""
        zip_contents = self.create_zip_complex()
        expected_tinfos = [
            ('a', 0, tarfile.DIRTYPE, b''),
            ('a/b', 0, tarfile.DIRTYPE, b''),
            ('a/b/file.txt', 11, tarfile.REGTYPE, b'hello world'),
            ('c', 0, tarfile.DIRTYPE, b''),
            ('c/d', 0, tarfile.DIRTYPE, b''),
            ('c/d/e', 0, tarfile.DIRTYPE, b''),
            ('file.txt', 11, tarfile.REGTYPE, b'hello world'),
        ]
        with tarfile.open(fileobj=ZipToTarStream(BytesIO(zip_contents)), mode="r|") as tf:
            tinfos = [
                (
                    tinfo.name,
                    tinfo.size,
                    tinfo.type,
                    tf.extractfile(tinfo).read() if tinfo.type == tarfile.REGTYPE else b"",
                )
                for tinfo in tf
            ]
            self.assertEqual(sorted(tinfos), expected_tinfos)

    def test_zip_to_tar_read_byte_by_byte(self):
        """Test converting a zip to a tar stream, while reading the input fileobj
        and the output ZipToTarStream byte-by-byte (so that the final tar archive
        is also assembled byte-by-byte)."""
        for (name, zip_contents) in [
            ("single file", self.create_zip_single_file()),
            ("complex file", self.create_zip_complex()),
        ]:
            with self.subTest(name=name):
                expected_tar_contents = ZipToTarStream(BytesIO(zip_contents)).read()
                buf = BytesBuffer()
                buf.write(zip_contents)
                zts = ZipToTarStream(ReadOneByOne(buf))
                out = BytesBuffer()
                while True:
                    chunk = zts.read(1)
                    if not chunk:
                        break
                    out.write(chunk)
                self.assertEqual(out.read(), expected_tar_contents)


class FileUtilTestAzureBlob(AzureBlobTestBase, unittest.TestCase):
    """Test file util methods that specifically have different code paths
    for files stored in Azure Blob Storage."""

    def test_get_file_size(self):
        _, fname = self.create_file()
        self.assertEqual(get_file_size(fname), 11)  # uncompressed size of entire bundle

        _, dirname = self.create_directory()
        self.assertEqual(get_file_size(dirname), 249)
        self.assertEqual(get_file_size(f"{dirname}/README.md"), 11)

    def test_read_file_section(self):
        _, fname = self.create_file()
        self.assertEqual(read_file_section(fname, 2, 4), b"llo ")
        self.assertEqual(read_file_section(fname, 100, 4), b"")

        _, dirname = self.create_directory()
        self.assertEqual(read_file_section(f"{dirname}/README.md", 2, 4), b"llo ")

    def test_gzip_stream(self):
        _, fname = self.create_file()
        self.assertEqual(un_gzip_stream(gzip_file(fname)).read(), b'hello world')

        _, dirname = self.create_directory()
        self.assertEqual(un_gzip_stream(gzip_file(f"{dirname}/README.md")).read(), b'hello world')

    def test_open_file(self):
        _, fname = self.create_file()

        # Read single file (gzipped)
        with OpenFile(fname, gzipped=True) as f:
            self.assertEqual(gzip.decompress(f.read()), b"hello world")

        # Read single file (non-gzipped):
        with OpenFile(fname) as f:
            self.assertEqual(f.read(), b"hello world")

        _, dirname = self.create_directory()

        # Read single file from directory (gzipped):
        with OpenFile(f"{dirname}/README.md", gzipped=True) as f:
            self.assertEqual(gzip.decompress(f.read()), b"hello world")

        # Read single file from directory (non-gzipped):
        with OpenFile(f"{dirname}/README.md") as f:
            self.assertEqual(f.read(), b"hello world")

        # Read entire directory (gzipped)
        with OpenFile(dirname, gzipped=True) as f:
            self.assertEqual(
                tarfile.open(fileobj=f, mode='r:gz').getnames(),
                [
                    './README.md',
                    './src',
                    './src/test.sh',
                    './dist',
                    './dist/a',
                    './dist/a/b',
                    './dist/a/b/test2.sh',
                ],
            )

        # Read entire directory (non-gzipped)
        with self.assertRaises(IOError):
            with OpenFile(dirname, gzipped=False) as f:
                pass

        # Read a subdirectory (gzipped)
        with OpenFile(f"{dirname}/src", gzipped=True) as f:
            self.assertEqual(
                tarfile.open(fileobj=f, mode='r:gz').getnames(), ['.', './test.sh'],
            )

        # Read a subdirectory (non-gzipped)
        with self.assertRaises(IOError):
            with OpenFile(f"{dirname}/src") as f:
                pass

        # Read a subdirectory with nested children
        with OpenFile(f"{dirname}/dist", gzipped=True) as f:
            self.assertEqual(
                tarfile.open(fileobj=f, mode='r:gz').getnames(),
                ['.', './a', './a/b', './a/b/test2.sh'],
            )


class ArchiveTestBase:
    """Base for archive tests -- tests both archiving and unarchiving directories.
    Subclasses must implement the archive() and unarchive() methods."""

    # Set to True if symlink tests should be skipped.
    skip_symlinks = False

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
        if not self.skip_symlinks:
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

    def test_do_not_always_ignore(self):
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: remove_path(temp_dir))
        output_dir = os.path.join(temp_dir, 'output')

        self.unarchive(self.archive(IGNORE_TEST_DIR, exclude_patterns=None), output_dir, 'gz')
        output_dir_entries = os.listdir(output_dir)
        self.assertNotIn('._ignored', output_dir_entries)
        self.assertIn('dir', output_dir_entries)
        self.assertIn('__MACOSX', output_dir_entries)
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'dir', '__MACOSX')))


class ZipArchiveTest(ArchiveTestBase, unittest.TestCase):
    """Archive test for zip methods."""

    # ZipFile does not preserve symlinks, so we should skip
    # symlink tests.
    skip_symlinks = True

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
