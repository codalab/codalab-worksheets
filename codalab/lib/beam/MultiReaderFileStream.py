from io import BytesIO, SEEK_SET, SEEK_END
from threading import Lock
import time

from codalab.worker.un_gzip_stream import BytesBuffer

import heapq

class MinMaxHeap:
    def __init__(self):
        self.heap = []
        self.item_index = {}  # Dictionary to store indices of elements
        
    def push(self, item):
        heapq.heappush(self.heap, item)
        index = len(self.heap) - 1
        self.item_index[item] = index
    
    def pop(self):
        if self.heap:
            item = heapq.heappop(self.heap)
            del self.item_index[item]
            return item
        else:
            raise IndexError("pop from an empty heap")
    
    def update(self, index, new_item):
        old_item = self.heap[index]
        self.heap[index] = new_item
        self.item_index[new_item] = index
        del self.item_index[old_item]
        heapq._siftup(self.heap, index)
        heapq._siftdown(self.heap, 0, index)
    
    def min(self):
        if self.heap:
            return self.heap[0]
    
    def max(self):
        if self.heap:
            if len(self.heap) == 1:
                return self.heap[0]
            elif len(self.heap) == 2:
                return self.heap[1]
            else:
                return max(self.heap[1], self.heap[2])
    
    def min_index(self):
        if self.heap:
            return self.item_index[self.min()]
    
    def max_index(self):
        if self.heap:
            return self.item_index[self.max()]
        
    def get_at_index(self, index: int):
        if index < len(self.heap):
            return self.heap[index]
        else:
            raise IndexError("Index out of range")

class MultiReaderFileStream(BytesIO):
    """
    FileStream that support multiple readers and seeks backwards
    """
    NUM_READERS = 2
    LOOKBACK_LENGTH = 33554432
    MAX_THRESHOLD = LOOKBACK_LENGTH * 4

    def __init__(self, fileobj):
        self._buffer = bytes()
        self._buffer_pos = 0 # start position of buffer in the fileobj (min reader position - LOOKBACK LENGTH)
        self._size = 0 # size of bytes (for convenience)
        self._pos = MinMaxHeap() # position of each reader
        self._fileobj = fileobj
        self._lock = Lock()  # lock to ensure one does not concurrently read self._fileobj / write to the buffer.

        for i in range(0, self.NUM_READERS):
            self._pos.push(0)
            assert self._pos.get_at_index(i) == 0
        class FileStreamReader(BytesIO):
            def __init__(s, index):
                s._index = index

            def read(s, num_bytes=None):
                return self.read(s._index, num_bytes)

            def peek(s, num_bytes):
                return self.peek(s._index, num_bytes)
            
            def seek(s, offset, whence):
                return self.seek(s._index, offset, whence)

        self.readers = [FileStreamReader(i) for i in range(0, self.NUM_READERS)]

    def _fill_buf_bytes(self, num_bytes=None):
        with self._lock:
            s = self._fileobj.read(num_bytes)
            if not s:
                return
            self._buffer += s
            self._size += len(s)

    def read(self, index: int, num_bytes=0):  # type: ignore
        """Read the specified number of bytes from the associated file.
        index: index that specifies which reader is reading.
        """
        self._fill_buf_bytes(index, num_bytes)
        # if num_bytes is None:
        #     num_bytes = len(self._bufs[index])
        while (self._pos.get_at_index(index) + num_bytes) - self._buffer_pos > self.MAX_THRESHOLD:
            time.sleep(.1) # 100 ms

        old_position = self._pos.get_at_index(index)
        s = self._buffer[old_position:old_position + num_bytes]

        # Modify position
        new_position = old_position + len(s)
        self._pos.update(index, new_position) 

        # Update buffer if this reader is the minimum reader
        diff = (self._pos.min() - self.LOOKBACK_LENGTH) - self._buffer_pos # calculated min position of buffer minus current min position of buffer
        # NOTE: it's possible for diff < 0 if seek backwards occur
        if diff > 0:
            self._buffer = self._buffer[diff:]
            self._buffer_pos += diff
            self._size -= diff

        return s

    def peek(self, index: int, num_bytes):   # type: ignore
        self._fill_buf_bytes(index, num_bytes)
        s = self._bufs[index].peek(num_bytes)
        return s

    def seek(self, index: int, offset: int, whence=SEEK_SET):
        pass

    def close(self):
        self.__input.close()
