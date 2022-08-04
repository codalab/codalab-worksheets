import unittest
from codalab.lib.formatting import size_str


class SizeStrTest(unittest.TestCase):
    def test_bytes_formatting(self):
        """
        The number of bytes should be returned as a string.
        If include_bytes is True, 'bytes' should be appended to the return value.
        """
        # < 100 bytes
        size = size_str(99.01)
        self.assertEqual(size, '99.0')
        size = size_str(99.01, include_bytes=True)
        self.assertEqual(size, '99.0 bytes')

        # > 100 bytes
        size = size_str(700.3)
        self.assertEqual(size, '700')
        size = size_str(700.3, include_bytes=True)
        self.assertEqual(size, '700 bytes')

    def test_kilobytes_formatting(self):
        """
        Bytes should be converted to kilobytes (denoted 'k') if possible.
        The include_bytes flag should have no affect on kilobytes.
        """
        # < 100 kilobytes
        size = size_str(3500)
        self.assertEqual(size, '3.4k')
        size = size_str(3500, include_bytes=True)
        self.assertEqual(size, '3.4k')

        # > 100 kilobytes
        size = size_str(400000)
        self.assertEqual(size, '390k')
        size = size_str(400000, include_bytes=True)
        self.assertEqual(size, '390k')

    def test_megabytes_formatting(self):
        """
        Bytes should be converted to megabytes (denoted 'm') if possible.
        The include_bytes flag should have no affect on megabytes.
        """
        # < 100 megabytes
        size = size_str(4000000)
        self.assertEqual(size, '3.8m')
        size = size_str(4000000, include_bytes=True)
        self.assertEqual(size, '3.8m')

        # > 100 megabytes
        size = size_str(400000000)
        self.assertEqual(size, '381m')
        size = size_str(400000000, include_bytes=True)
        self.assertEqual(size, '381m')

    def test_gigabytes_formatting(self):
        """
        Bytes should be converted to gigabytes (denoted 'g') if possible.
        The include_bytes flag should have no affect on gigabytes.
        """
        # < 100 gigabytes
        size = size_str(4000000000)
        self.assertEqual(size, '3.7g')
        size = size_str(4000000000, include_bytes=True)
        self.assertEqual(size, '3.7g')

        # > 100 gigabytes
        size = size_str(350000000000)
        self.assertEqual(size, '325g')
        size = size_str(350000000000, include_bytes=True)
        self.assertEqual(size, '325g')

    def test_terabytes_formatting(self):
        """
        Bytes should be converted to terabytes (denoted 't') if possible.
        The include_bytes flag should have no affect on terabytes.
        """
        # < 100 terabytes
        size = size_str(3500000000000)
        self.assertEqual(size, '3.2t')
        size = size_str(3500000000000, include_bytes=True)
        self.assertEqual(size, '3.2t')

        # > 100 terabytes
        size = size_str(450000000000000)
        self.assertEqual(size, '409t')
        size = size_str(450000000000000, include_bytes=True)
        self.assertEqual(size, '409t')
