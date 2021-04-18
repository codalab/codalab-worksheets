import tempfile
import unittest

from io import BytesIO

from tests.unit.mock_network import MockNetwork
from codalab.lib.file_util import download_url


class FileUtilTest(MockNetwork, unittest.TestCase):
    def test_download_url_to_path(self):
        url = self.mock_url_sources(BytesIO(b"hello world"))[0]
        with tempfile.NamedTemporaryFile(delete=False) as f:
            download_url(url, f.name)
            self.assertEqual(f.read(), b"hello world")

    def test_download_url_to_fileobj(self):
        url = self.mock_url_sources(BytesIO(b"hello world"))[0]
        with tempfile.NamedTemporaryFile(delete=False) as f:
            download_url(url, out_file=f)
            f.seek(0)
            self.assertEqual(f.read(), b"hello world")
