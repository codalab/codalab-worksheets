import tempfile
import time
import unittest

from threading import Thread

from codalab.lib.beam.MultiReaderFileStream import MultiReaderFileStream

FILESIZE = 100000000
CHUNKSIZE = FILESIZE/10

class MultiReaderFileStreamTest(unittest.TestCase):
    def test_reader_distance(self):
        with tempfile.NamedTemporaryFile(delete=True) as f:
            f.seek(FILESIZE - 1)
            f.write(b"\0")

            m_stream = MultiReaderFileStream(f)
            reader_1 = m_stream.readers[0]
            reader_2 = m_stream.readers[1]

            def thread1():
                while True:
                    status = reader_1.read(CHUNKSIZE)
                    if not status:
                        break

            def thread2():
                # This reader will only read 4/10 of the file
                for _ in range(4):
                    status = reader_2.read(CHUNKSIZE)

            t1 = Thread(target=thread1)
            t2 = Thread(target=thread2)

            t1.start()

            # Sleep a little for thread 1 to start reading
            time.sleep(3)

            # Assert that the first reader has not read past the Maximum threshold
            self.assertGreater(70000000, m_stream._pos[0])

            t2.start()

            # Sleep a little for thread 2 to start reading
            time.sleep(3)

            # Assert that the first reader is at 100000000, second reader is at 40000000
            self.assertEqual(100000000, m_stream._pos[0])
            self.assertEqual(40000000, m_stream._pos[1])

            # Assert that the buffer is at 6445568 (40000000 - LOOKBACK_LENGTH)
            self.assertEqual(6445568, m_stream._buffer_pos)

            # Assert that the buffer is length 100000000 - 6445568 
            self.assertEqual(93554432, m_stream._size)

            t1.join()
            t2.join()
    
    def test_seek(self):
        with tempfile.NamedTemporaryFile(delete=True) as f:
            f.seek(FILESIZE - 1)
            f.write(b"\0")

            m_stream = MultiReaderFileStream(f)
            reader_1 = m_stream.readers[0]
            reader_2 = m_stream.readers[1]

            result = None

            def thread1():
                while True:
                    status = reader_1.read(CHUNKSIZE)
                    if not status:
                        break

            def thread2():
                # This reader will only read 4/10 of the file, then seek to 10000000 and read another 4/10 of the file
                for _ in range(4):
                    reader_2.read(CHUNKSIZE)
                
                try:
                    reader_2.seek(10000000)
                except AssertionError as e: 
                    result = e

                for _ in range(4):
                    reader_2.read(CHUNKSIZE)

            t1 = Thread(target=thread1)
            t2 = Thread(target=thread2)
            t1.start()
            t2.start()

            t1.join()
            t2.join()

            self.assertIsNone(result)

            # Check that reader 2 is at 50000000 and buffer position is correct
            self.assertEqual(50000000, m_stream._pos[1])
            self.assertEqual(16445568, m_stream._buffer_pos)


    def test_toofar_seek(self):
        with tempfile.NamedTemporaryFile(delete=True) as f:
            f.seek(FILESIZE - 1)
            f.write(b"\0")

            m_stream = MultiReaderFileStream(f)
            reader_1 = m_stream.readers[0]
            reader_2 = m_stream.readers[1]

            result = None

            def thread1():
                while True:
                    status = reader_1.read(CHUNKSIZE)
                    if not status:
                        break

            def thread2():
                # This reader will only read 4/10 of the file, then seek to the beginning
                for _ in range(4):
                    status = reader_2.read(CHUNKSIZE)
                
                try:
                    reader_2.seek(0)
                except AssertionError as e: 
                    result = e

            t1 = Thread(target=thread1)
            t2 = Thread(target=thread2)
            t1.start()
            t2.start()

            t1.join()
            t2.join()

            self.assertIsInstance(result, AssertionError)
