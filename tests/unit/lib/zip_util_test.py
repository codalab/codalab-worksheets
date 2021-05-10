import bz2
import gzip
import os
import tarfile
import tempfile
import unittest
import zipfile

from io import BytesIO

from codalab.common import UsageError
from codalab.lib.zip_util import (
    get_archive_ext,
    strip_archive_ext,
    path_is_archive,
    pack_files_for_upload,
    unpack,
    unpack_to_archive,
)
from codalab.worker.file_util import tar_gzip_directory, zip_directory

SAMPLE_CONTENTS = b"hello world"


def tar_bz2_directory(*args, **kwargs):
    """Method used for creating a .tar.bz2 archive from a directory.
    This is just used for tests; it's not performance optimized
    and should not be used in production code."""
    output = tar_gzip_directory(*args, **kwargs)
    return BytesIO(bz2.compress(gzip.decompress(output.read())))


class ZipUtilTest(unittest.TestCase):
    """Test for zip util methods."""

    def test_strip_archive_ext_local_file_non_archive(self):
        """This local file should be categorized as not an archive."""
        path = "/tmp/file.txt"
        self.assertEqual(path_is_archive(path), False)
        self.assertEqual(get_archive_ext(path), "")
        with self.assertRaises(UsageError):
            strip_archive_ext(path)

    def test_strip_archive_ext_url_non_archive(self):
        """This URL should be categorized as not an archive."""
        path = "https://codalab.org/file.txt"
        self.assertEqual(path_is_archive(path), False)
        self.assertEqual(get_archive_ext(path), "")
        with self.assertRaises(UsageError):
            strip_archive_ext(path)

    def test_strip_archive_ext_local_file_archive(self):
        """This local should be categorized as an archive."""
        path = "/tmp/file.tar.gz"
        self.assertEqual(path_is_archive(path), True)
        self.assertEqual(get_archive_ext(path), ".tar.gz")
        self.assertEqual(strip_archive_ext(path), "/tmp/file")

    def test_strip_archive_ext_url_archive(self):
        """This URL should be categorized as an archive."""
        path = "https://codalab.org/file.tar.gz"
        self.assertEqual(path_is_archive(path), True)
        self.assertEqual(get_archive_ext(path), ".tar.gz")
        self.assertEqual(strip_archive_ext(path), "https://codalab.org/file")

    def test_pack_single_file(self):
        """Pack a single file."""
        with tempfile.NamedTemporaryFile() as f:
            f.write(SAMPLE_CONTENTS)
            f.flush()
            packed = pack_files_for_upload(
                sources=[f.name], should_unpack=False, follow_symlinks=False
            )
            self.assertEqual(packed.pop("fileobj").read(), SAMPLE_CONTENTS)
            self.assertEqual(
                packed,
                {
                    "filename": os.path.basename(f.name),
                    "filesize": len(SAMPLE_CONTENTS),
                    "should_unpack": False,
                },
            )

    def test_pack_single_file_force_compression(self):
        """Pack a single file with force_compression set to True."""
        with tempfile.NamedTemporaryFile() as f:
            f.write(SAMPLE_CONTENTS)
            f.flush()
            packed = pack_files_for_upload(
                sources=[f.name], should_unpack=False, follow_symlinks=False, force_compression=True
            )
            self.assertEqual(gzip.decompress(packed.pop("fileobj").read()), SAMPLE_CONTENTS)
            self.assertEqual(
                packed,
                {
                    "filename": os.path.basename(f.name) + '.gz',
                    "filesize": None,
                    "should_unpack": True,
                },
            )

    def test_pack_directory(self):
        """Pack a single directory."""
        with tempfile.TemporaryDirectory() as tmpdir, open(
            os.path.join(tmpdir, "file.txt"), "wb"
        ) as f:
            f.write(SAMPLE_CONTENTS)
            f.seek(0)
            packed = pack_files_for_upload(
                sources=[tmpdir], should_unpack=False, follow_symlinks=False
            )
            tf = tarfile.open(fileobj=packed.pop("fileobj"), mode="r:gz")
            self.assertEqual(tf.getnames(), ['.', './file.txt'])
            self.assertEqual(tf.extractfile('./file.txt').read(), SAMPLE_CONTENTS)
            self.assertEqual(
                packed,
                {
                    "filename": os.path.basename(tmpdir) + '.tar.gz',
                    "filesize": None,
                    "should_unpack": True,
                },
            )

    def test_pack_single_archive(self):
        """Pack a single archive that is a .tar.gz / .tar.bz2 file."""
        for (compress_fn, extension, mode) in [
            (tar_gzip_directory, ".tar.gz", "r:gz"),
            (tar_bz2_directory, ".tar.bz2", "r:bz2"),
        ]:
            with self.subTest(extension=extension), tempfile.TemporaryDirectory() as tmpdir, open(
                os.path.join(tmpdir, "file.txt"), "wb"
            ) as f, tempfile.NamedTemporaryFile(suffix=extension) as out_archive:
                f.write(SAMPLE_CONTENTS)
                f.flush()
                out_archive.write(compress_fn(tmpdir).read())
                out_archive_size = out_archive.tell()
                out_archive.flush()
                packed = pack_files_for_upload(
                    sources=[out_archive.name], should_unpack=False, follow_symlinks=False
                )
                tf = tarfile.open(fileobj=packed.pop("fileobj"), mode=mode)
                self.assertEqual(tf.getnames(), ['.', './file.txt'])
                self.assertEqual(tf.extractfile('./file.txt').read(), SAMPLE_CONTENTS)
                self.assertEqual(
                    packed,
                    {
                        "filename": os.path.basename(out_archive.name),
                        "filesize": out_archive_size,
                        "should_unpack": False,
                    },
                )

    def test_pack_single_archive_zip(self):
        """Pack a single archive that is a .zip file."""
        with tempfile.TemporaryDirectory() as tmpdir, open(
            os.path.join(tmpdir, "file.txt"), "wb"
        ) as f, tempfile.NamedTemporaryFile(suffix=".zip") as out_archive:
            f.write(SAMPLE_CONTENTS)
            f.flush()
            out_archive.write(zip_directory(tmpdir).read())
            out_archive_size = out_archive.tell()
            out_archive.flush()
            packed = pack_files_for_upload(
                sources=[out_archive.name], should_unpack=False, follow_symlinks=False
            )
            zf = zipfile.ZipFile(packed.pop("fileobj"), mode="r")
            self.assertEqual(zf.namelist(), ['file.txt'])
            self.assertEqual(zf.open('file.txt').read(), SAMPLE_CONTENTS)
            self.assertEqual(
                packed,
                {
                    "filename": os.path.basename(out_archive.name),
                    "filesize": out_archive_size,
                    "should_unpack": False,
                },
            )

    def test_pack_files_and_directories(self):
        """Pack a combination of files and directories."""
        with tempfile.NamedTemporaryFile() as f1, tempfile.TemporaryDirectory() as tmpdir, open(
            os.path.join(tmpdir, "file.txt"), "wb"
        ) as f2:
            f1.write(SAMPLE_CONTENTS)
            f1.seek(0)
            f2.write(SAMPLE_CONTENTS)
            f2.seek(0)
            packed = pack_files_for_upload(
                sources=[f1.name, tmpdir], should_unpack=False, follow_symlinks=False
            )
            fileobj = packed.pop("fileobj")
            tf = tarfile.open(fileobj=fileobj, mode="r:gz")
            expected_names = [
                os.path.basename(f1.name),
                os.path.basename(tmpdir),
                os.path.join(os.path.basename(tmpdir), "file.txt"),
            ]
            self.assertEqual(tf.getnames(), expected_names)
            self.assertEqual(tf.extractfile(expected_names[0]).read(), SAMPLE_CONTENTS)
            self.assertEqual(tf.extractfile(expected_names[2]).read(), SAMPLE_CONTENTS)
            fileobj.seek(0, os.SEEK_END)
            self.assertEqual(
                packed,
                {"filename": 'contents.tar.gz', "filesize": fileobj.tell(), "should_unpack": True,},
            )

    def test_unpack_single_archive(self):
        """Unpack a single archive."""
        for (compress_fn, extension) in [
            (tar_gzip_directory, ".tar.gz"),
            (tar_bz2_directory, ".tar.bz2"),
            (zip_directory, ".zip"),
        ]:
            with self.subTest(extension=extension), tempfile.TemporaryDirectory() as tmpdir, open(
                os.path.join(tmpdir, "file.txt"), "wb"
            ) as f, tempfile.TemporaryDirectory() as dest_path:
                f.write(SAMPLE_CONTENTS)
                f.flush()
                unpack(extension, compress_fn(tmpdir), os.path.join(dest_path, "out"))
                self.assertEqual(os.listdir(tmpdir), ["file.txt"])
                self.assertEqual(os.listdir(os.path.join(dest_path, "out")), ["file.txt"])
                self.assertEqual(
                    open(os.path.join(dest_path, "out", "file.txt"), "rb").read(), SAMPLE_CONTENTS
                )

    def test_unpack_single_compressed_file(self):
        """Unpack a single compressed file."""
        for (compress_fn, extension) in [
            (bz2.compress, ".bz2"),
            (gzip.compress, ".gz"),
        ]:
            with self.subTest(extension=extension), tempfile.TemporaryDirectory() as tmpdir, open(
                os.path.join(tmpdir, "file.txt"), "wb"
            ) as f, tempfile.TemporaryDirectory() as dest_path:
                f.write(compress_fn(SAMPLE_CONTENTS))
                f.flush()
                unpack(
                    extension,
                    open(os.path.join(tmpdir, "file.txt"), "rb"),
                    os.path.join(dest_path, "out"),
                )
                self.assertEqual(SAMPLE_CONTENTS, open(os.path.join(dest_path, "out"), "rb").read())

    def test_unpack_to_archive_single_archive(self):
        """Unpack a single archive to a .tar.gz file."""
        for (compress_fn, extension) in [
            (tar_gzip_directory, ".tar.gz"),
            (tar_bz2_directory, ".tar.bz2"),
            (zip_directory, ".zip"),
        ]:
            with self.subTest(extension=extension), tempfile.TemporaryDirectory() as tmpdir, open(
                os.path.join(tmpdir, "file.txt"), "wb"
            ) as f, tempfile.TemporaryDirectory() as dest_path:
                f.write(SAMPLE_CONTENTS)
                f.flush()
                archive_fileobj = unpack_to_archive(extension, compress_fn(tmpdir))
                unpack(".tar.gz", archive_fileobj, os.path.join(dest_path, "out"))
                self.assertEqual(os.listdir(tmpdir), ["file.txt"])
                self.assertEqual(os.listdir(os.path.join(dest_path, "out")), ["file.txt"])
                self.assertEqual(
                    open(os.path.join(dest_path, "out", "file.txt"), "rb").read(), SAMPLE_CONTENTS
                )

    def test_unpack_to_archive_single_compressed_file(self):
        """Unpack a single compressed file to a .gz file."""
        for (compress_fn, extension) in [
            (bz2.compress, ".bz2"),
            (gzip.compress, ".gz"),
        ]:
            with self.subTest(extension=extension), tempfile.TemporaryDirectory() as tmpdir, open(
                os.path.join(tmpdir, "file.txt"), "wb"
            ) as f, tempfile.TemporaryDirectory() as dest_path:
                f.write(compress_fn(SAMPLE_CONTENTS))
                f.flush()
                archive_fileobj = unpack_to_archive(
                    extension, open(os.path.join(tmpdir, "file.txt"), "rb")
                )
                unpack(".gz", archive_fileobj, os.path.join(dest_path, "out"))
                self.assertEqual(SAMPLE_CONTENTS, open(os.path.join(dest_path, "out"), "rb").read())
