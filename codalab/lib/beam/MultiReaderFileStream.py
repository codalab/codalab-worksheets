from io import BytesIO, SEEK_SET, SEEK_END
from threading import Lock
import time
class MultiReaderFileStream(BytesIO):
    """
    FileStream that supports N readers with the following features and constraints:
        - Each reader's postion is tracked
        - A buffer of bytes() is stored which stores bytes from the position of the slowest reader
          minus a lookback length of 32MiB to the fastest reader
        - The fastest reader can be at most 64MiB ahead of the slowest reader, reads made
          further than 64MiB will sleep until the slowest reader catches up
    """
    NUM_READERS = 2
    LOOKBACK_LENGTH = 33554432 # 32 MiB
    MAX_THRESHOLD = LOOKBACK_LENGTH * 2

    def __init__(self, fileobj):
        self._buffer = bytes() # Buffer of bytes read from the file object within the limits defined
        self._buffer_pos = 0 # start position of buffer in the fileobj (min reader position - LOOKBACK LENGTH)
        self._pos = [0 for _ in range(self.NUM_READERS)] # position of each reader in the fileobj
        self._fileobj = fileobj # The original file object the readers are reading from
        self._lock = Lock()  # lock to ensure one does not concurrently read self._fileobj / write to the buffer.
        class FileStreamReader(BytesIO):
            def __init__(s, index):
                s._index = index

            def read(s, num_bytes=None):
                return self.read(s._index, num_bytes)

            def peek(s, num_bytes):
                return self.peek(s._index, num_bytes)
            
            def seek(s, offset, whence=SEEK_SET):
                return self.seek(s._index, offset, whence)

        self.readers = [FileStreamReader(i) for i in range(0, self.NUM_READERS)]

    def _fill_buf_bytes(self, num_bytes=None):
        """
        Fills the buffer with bytes from the fileobj
        """
        s = self._fileobj.read(num_bytes)
        if not s:
            return
        self._buffer += s

    def read(self, index: int, num_bytes=0):  # type: ignore
        """Read the specified number of bytes from the associated file.
        index: index that specifies which reader is reading.
        """
        while (self._pos[index] + num_bytes) - self._buffer_pos > self.MAX_THRESHOLD:
            time.sleep(.1) # 100 ms
        
        with self._lock:
            # Calculate how many new bytes need to be read
            new_pos = self._pos[index] + num_bytes
            new_bytes_needed = new_pos - max(self._pos)
            if new_bytes_needed > 0:
                self._fill_buf_bytes(new_bytes_needed)

            # Get the bytes in the buffer that correspond to the read function call
            buffer_index = self._pos[index] - self._buffer_pos
            s = self._buffer[buffer_index:buffer_index + num_bytes]

            # Modify reader position in fileobj
            self._pos[index] += len(s)

            # If this reader is the minimum reader, we can remove some bytes from the beginning of the buffer
            # Calculated min position of buffer minus current min position of buffer
            diff = (min(self._pos) - self.LOOKBACK_LENGTH) - self._buffer_pos 
            # NOTE: it's possible for diff < 0 if seek backwards occur
            if diff > 0:
                self._buffer = self._buffer[diff:]
                self._buffer_pos += diff
        return s

    def peek(self, index: int, num_bytes):   # type: ignore
        while (self._pos[index] + num_bytes) - self._buffer_pos > self.MAX_THRESHOLD:
            time.sleep(.1) # 100 ms
        
        with self._lock:
            # Calculate how many new bytes need to be read
            new_pos = self._pos[index] + num_bytes
            new_bytes_needed = new_pos - max(self._pos)
            if new_bytes_needed > 0:
                self._fill_buf_bytes(new_bytes_needed)
        
            # Get the bytes in the buffer that correspond to the read function call
            buffer_index = self._pos[index] - self._buffer_pos
            s = self._buffer[buffer_index:buffer_index + num_bytes]
        return s

    def seek(self, index: int, offset: int, whence=SEEK_SET):
        if whence == SEEK_END:
            super().seek(offset, whence)
        else:
            assert offset >= self._buffer_pos
            self._pos[index] = offset
            
    def close(self):
        self.__input.close()
