import unittest
from codalab.common import UsageError
from codalab.lib.zip_util import get_archive_ext, strip_archive_ext, path_is_archive


class ZipUtilTest(unittest.TestCase):
    """Test for zip util methods."""

    def test_local_file_non_archive(self):
        """This local file should be categorized as not an archive."""
        path = "/tmp/file.txt"
        self.assertEqual(path_is_archive(path), False)
        self.assertEqual(get_archive_ext(path), "")
        with self.assertRaises(UsageError):
            strip_archive_ext(path)

    def test_url_non_archive(self):
        """This URL should be categorized as not an archive."""
        path = "https://codalab.org/file.txt"
        self.assertEqual(path_is_archive(path), False)
        self.assertEqual(get_archive_ext(path), "")
        with self.assertRaises(UsageError):
            strip_archive_ext(path)

    def test_local_file_archive(self):
        """This local should be categorized as an archive."""
        path = "/tmp/file.tar.gz"
        self.assertEqual(path_is_archive(path), True)
        self.assertEqual(get_archive_ext(path), ".tar.gz")
        self.assertEqual(strip_archive_ext(path), "/tmp/file")

    def test_url_archive(self):
        """This URL should be categorized as an archive."""
        path = "https://codalab.org/file.tar.gz"
        self.assertEqual(path_is_archive(path), True)
        self.assertEqual(get_archive_ext(path), ".tar.gz")
        self.assertEqual(strip_archive_ext(path), "https://codalab.org/file")
