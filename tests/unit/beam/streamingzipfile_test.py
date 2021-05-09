import contextlib
import os
import pickle
import tempfile
import unittest

from io import BytesIO, UnsupportedOperation
from zipfile import ZipFile, BadZipFile

from codalab.lib.beam.streamingzipfile import StreamingZipFile
from codalab.worker.file_util import zip_directory

SAMPLE_CONTENTS = b"hello world"


class UnseekableBytesIO(BytesIO):
    def seek(self, *args, **kwargs):
        raise UnsupportedOperation

    def seekable(self):
        return False


class StreamingZipFileTest(unittest.TestCase):
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

    def test_seekable_file_read_by_zipfile(self):
        """Seekable file can be read by ZipFile"""
        zip_contents = self.create_zip_single_file()
        with ZipFile(BytesIO(zip_contents)) as zf:
            infolist = zf.infolist()
            self.assertEqual(infolist[0].filename, "file.txt")
            self.assertEqual(infolist[0].file_size, 11)
            self.assertEqual(zf.open(infolist[0]).read(), SAMPLE_CONTENTS)

    def test_unseekable_file_cannot_read_by_zipfile(self):
        """Unseekable file cannot be read by ZipFile"""
        zip_contents = self.create_zip_single_file()
        with self.assertRaises(BadZipFile):
            ZipFile(UnseekableBytesIO(zip_contents))

    def test_unseekable_file_read_by_streamingzipfile(self):
        """Unseekable file can be read by StreamingZipFile"""
        zip_contents = self.create_zip_single_file()
        with StreamingZipFile(UnseekableBytesIO(zip_contents)) as zf:
            for zinfo in zf:
                self.assertEqual(zinfo.filename, "file.txt")
                self.assertEqual(zinfo.file_size, 11)
                self.assertEqual(zf.open(zinfo).read(), SAMPLE_CONTENTS)

        # Ensure fields have been extracted properly and correspond with fields read by ZipFile
        with ZipFile(BytesIO(zip_contents)) as zf:
            infolist = zf.infolist()
            for field in (
                "extract_version",
                "reserved",
                "flag_bits",
                "compress_type",
                "date_time",
                "header_offset",
                "CRC",
                "compress_size",
                "file_size",
            ):
                self.assertEqual(getattr(zinfo, field), getattr(infolist[0], field), field)

    def test_read_complex(self):
        """Zip file with a complex directory structure can be read by ZipFile / StreamingZipFile properly"""
        zip_contents = self.create_zip_complex()
        expected_zinfos = [
            ('file.txt', 11, False, b'hello world'),
            ('a/', 0, True, b''),
            ('a/b/', 0, True, b''),
            ('a/b/file.txt', 11, False, b'hello world'),
            ('c/', 0, True, b''),
            ('c/d/', 0, True, b''),
            ('c/d/e/', 0, True, b''),
        ]
        with ZipFile(BytesIO(zip_contents)) as zf:
            zinfos = [
                (zinfo.filename, zinfo.file_size, zinfo.is_dir(), zf.open(zinfo).read())
                for zinfo in zf.infolist()
            ]
            self.assertEqual(zinfos, expected_zinfos)

        with StreamingZipFile(UnseekableBytesIO(zip_contents)) as zf:
            zinfos = [
                (zinfo.filename, zinfo.file_size, zinfo.is_dir(), zf.open(zinfo).read())
                for zinfo in zf
            ]
            self.assertEqual(zinfos, expected_zinfos)
