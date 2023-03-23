from io import BytesIO
from threading import Lock

from codalab.worker.un_gzip_stream import BytesBuffer


class MultiReaderFileStream(BytesIO):
    """
    FileStream that support multiple readers
    """
    NUM_READERS = 2

    def __init__(self, fileobj):
        self._bufs = [BytesBuffer() for _ in range(0, self.NUM_READERS)]
        self._pos = [0 for _ in range(0, self.NUM_READERS)]
        self._fileobj = fileobj
        self._lock = Lock()  # lock to ensure one does not concurrently read self._fileobj / write to the buffers.

        class FileStreamReader(BytesIO):
            def __init__(s, index):
                s._index = index

            def read(s, num_bytes=None):
                return self.read(s._index, num_bytes)

            def peek(s, num_bytes):
                return self.peek(s._index, num_bytes)

        self.readers = [FileStreamReader(i) for i in range(0, self.NUM_READERS)]

    def _fill_buf_bytes(self, index: int, num_bytes=None):
        with self._lock:
            while num_bytes is None or len(self._bufs[index]) < num_bytes:
                s = self._fileobj.read(num_bytes)
                if not s:
                    break
                for i in range(0, self.NUM_READERS):
                    self._bufs[i].write(s)

    def read(self, index: int, num_bytes=None):  # type: ignore
        """Read the specified number of bytes from the associated file.
        index: index that specifies which reader is reading.
        """
        self._fill_buf_bytes(index, num_bytes)
        if num_bytes is None:
            num_bytes = len(self._bufs[index])
        s = self._bufs[index].read(num_bytes)
        self._pos[index] += len(s)
        return s

    def peek(self, index: int, num_bytes):   # type: ignore
        self._fill_buf_bytes(index, num_bytes)
        s = self._bufs[index].peek(num_bytes)
        return s

    def close(self):
        self.__input.close()
