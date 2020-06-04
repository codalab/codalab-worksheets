import os
from io import BytesIO
import tempfile
import unittest
from gzip import GzipFile

from codalab.worker import file_util

class FileUtilTest(unittest.TestCase):
    def test_gzip_file(self):
        with tempfile.NamedTemporaryFile() as f:
            f.write(b"hello world")
            f.flush()
            f.seek(0)
            gzipped_file = file_util.gzip_file(f.name)
            with GzipFile(fileobj=gzipped_file) as gzf:
                self.assertEqual(gzf.read(), b"hello world")