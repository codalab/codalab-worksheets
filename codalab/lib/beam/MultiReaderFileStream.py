from io import BytesIO
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
    
    def update(self, old_item, new_item):
        index = self.item_index.pop(old_item)
        self.heap[index] = new_item
        self.item_index[new_item] = index
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
        
    def get_at_index(self, index):
        if index < len(self.heap):
            return self.heap[index]
        else:
            raise IndexError("Index out of range")

class MultiReaderFileStream2(BytesIO):
    """
    FileStream that support multiple readers and seeks backwards
    """
    NUM_READERS = 2
    LOOKBACK_LENGTH = 33554432
    MAX_THRESHOLD = LOOKBACK_LENGTH * 4

    def __init__(self, fileobj):
        self._buffer = bytes()
        self._buffer_pos = 0 # position in the fileobj (min reader position - LOOKBACK LENGTH)
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

        self.readers = [FileStreamReader(i) for i in range(0, self.NUM_READERS)]

    def _fill_buf_bytes(self, num_bytes=None):
        with self._lock:
            s = self._fileobj.read(num_bytes)
            if not s:
                return
            self._buffer += s

    def read(self, index: int, num_bytes=0):  # type: ignore
        """Read the specified number of bytes from the associated file.
        index: index that specifies which reader is reading.
        """
        self._fill_buf_bytes(index, num_bytes)
        # if num_bytes is None:
        #     num_bytes = len(self._bufs[index])
        while (self._pos[index] + num_bytes) - self._buffer_pos > self.MAX_THRESHOLD:
            time.sleep(.1) # 100 ms
        s = self._bufs[index].read(num_bytes)
        self._pos[index] += len(s)
        return s

    def peek(self, index: int, num_bytes):   # type: ignore
        self._fill_buf_bytes(index, num_bytes)
        s = self._bufs[index].peek(num_bytes)
        return s

    def close(self):
        self.__input.close()
