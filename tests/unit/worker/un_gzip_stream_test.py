from io import UnsupportedOperation
import unittest

from codalab.worker.un_gzip_stream import BytesBuffer


class BytesBufferTest(unittest.TestCase):
    def test_read_basic(self):
        """Test basic operations: read(), tell(), write()."""
        for kwargs in [dict(), dict(extra_buffer_size=20)]:
            b = BytesBuffer(**kwargs)
            b.write(b"hello")
            self.assertEqual(b.tell(), 0)
            self.assertEqual(b.read(), b"hello")
            b.write(b"h")
            b.write(b"e")
            self.assertEqual(b.read(1), b"h")
            self.assertEqual(b.read(1), b"e")
            self.assertEqual(b.read(1), b"")
            self.assertEqual(b.read(), b"")
            self.assertEqual(b.tell(), 7)
            b.write(b"he")
            self.assertEqual(b.read(1), b"h")
            b.write(b"llo")
            self.assertEqual(b.read(), b"ello")
            self.assertEqual(b.tell(), 12)

    def test_extra_buffer_seek(self):
        """Test initializing BytesBuffer with extra_buffer_size,
        allowing for limited seeking."""
        b = BytesBuffer(extra_buffer_size=20)
        b.write(b"1hello2hello")
        self.assertEqual(b.read(), b"1hello2hello")
        b.seek(0)
        self.assertEqual(b.read(), b"1hello2hello")
        b.seek(1)
        b.seek(2)
        b.seek(3)
        b.seek(2)
        self.assertEqual(b.read(), b"ello2hello")
        b.seek(6)
        self.assertEqual(b.read(), b"2hello")

    def test_extra_buffer_seek_size_exceeded(self):
        """Once extra_buffer_size is exceeded, seeking should no longer work."""
        b = BytesBuffer(extra_buffer_size=2)
        b.write(b"1hello2hello")
        self.assertEqual(b.read(), b"1hello2hello")
        b.seek(10)
        with self.assertRaises(UnsupportedOperation):
            b.seek(9)
