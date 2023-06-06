from io import BytesIO
from threading import Lock

from codalab.worker.un_gzip_stream import BytesBuffer


class MultiReaderFileStream(BytesIO):
    """
    FileStream that support multiple readers
    """
    NUM_READERS = 2

    # MAX memory usage <= MAX_BUF_SIZE + max(num_bytes called in read)
    MAX_BUF_SIZE = 1024 * 1024 * 1024  # 10 MiB for test

    def __init__(self, fileobj):
        self._bufs = [BytesBuffer() for _ in range(0, self.NUM_READERS)]
        self._pos = [0 for _ in range(0, self.NUM_READERS)]
        self._fileobj = fileobj
        self._lock = Lock()  # lock to ensure one does not concurrently read self._fileobj / write to the buffers.
        self._current_max_buf_length = 0

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
                self.find_largest_buffer()

    def find_largest_buffer(self):
        self._current_max_buf_length = len(self._bufs[0])
        for i in range(1, self.NUM_READERS):
            self._current_max_buf_length = max(self._current_max_buf_length, len(self._bufs[i]))
        # print(f"find largest buffer:  {self._current_max_buf_length} in thread: {threading.current_thread().name}")

    def read(self, index: int, num_bytes=None):  # type: ignore
        """Read the specified number of bytes from the associated file.
        index: index that specifies which reader is reading.
        """

        # print(f"calling read() in thread {threading.current_thread().name}, num_bytes={num_bytes}")
        # busy waiting until
        while(self._current_max_buf_length > self.MAX_BUF_SIZE and len(self._bufs[index]) < self._current_max_buf_length):
            # only the slowest reader could read
            # print(f"Busy waiting in thread: {threading.current_thread().name}, current max_len = {self._current_max_buf_length}, current_buf_size = {len(self._bufs[index])}")
            pass

        # If current thread is the slowest reader, continue read.
        # If current thread is the slowest reader, and num_bytes > len(self._buf[index]) / num_bytes = None, will continue grow the buffer.
        # max memory usage <= MAX_BUF_SIZE + max(num_bytes called in read)
        self._fill_buf_bytes(index, num_bytes)
        assert self._current_max_buf_length <= 2 * self.MAX_BUF_SIZE
        if num_bytes is None:
            num_bytes = len(self._bufs[index])
        s = self._bufs[index].read(num_bytes)
        self.find_largest_buffer()
        # print("Current thread name: ", threading.current_thread().name)

        self._pos[index] += len(s)
        return s

    def peek(self, index: int, num_bytes):   # type: ignore
        self._fill_buf_bytes(index, num_bytes)
        s = self._bufs[index].peek(num_bytes)
        return s

    def close(self):
        self.__input.close()
