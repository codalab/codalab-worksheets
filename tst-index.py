from collections import deque
import gzip
from io import BytesIO
from typing import Optional, IO

import indexed_gzip
import io
import os

class BytesBuffer(BytesIO):
    """
    A class for a buffer of bytes. Unlike io.BytesIO(), this class
    keeps track of the buffer's size (in bytes).
    """

    def __init__(self):
        self.__buf = deque()
        self.__size = 0
        self.__pos = 0

    def __len__(self):
        return self.__size

    def write(self, data):
        self.__buf.append(data)
        self.__size += len(data)

    def read(self, size: Optional[int] = None):
        if size is None:
            size = self.__size
        ret_list = []
        while size > 0 and len(self.__buf):
            s = self.__buf.popleft()
            size -= len(s)
            ret_list.append(s)
        if size < 0:
            ret_list[-1], remainder = ret_list[-1][:size], ret_list[-1][size:]
            self.__buf.appendleft(remainder)
        ret = b''.join(ret_list)
        self.__size -= len(ret)
        self.__pos += len(ret)
        return ret

    def peek(self, size: int):
        b = bytearray()
        for i in range(0, min(size, len(self.__buf))):
            b.extend(self.__buf[i])
        return bytes(b)[:size]

    def flush(self):
        pass

    def close(self):
        pass

    def tell(self):
        return self.__pos

    def __bool__(self):
        return True


class GzipStream(BytesIO):
    """A stream that gzips a file in chunks.
    """
    def __init__(self, fileobj: IO[bytes]):
        self.__input = fileobj
        self.__buffer = BytesBuffer()
        self.__gzip = gzip.GzipFile(None, mode='wb', fileobj=self.__buffer)

    def _fill_buf_bytes(self, num_bytes=None):
        while num_bytes is None or len(self.__buffer) < num_bytes:
            # print("Call _fill_buf_bytes in GZipStream, num_bytes: ", len(self.__buffer))
            # print("Before: ", len(self.__buffer))
            s = self.__input.read(num_bytes)
            # print("Middle: s size is ", len(s))
            if not s:
                print("close")
                self.__gzip.close()
                break
            self.__gzip.write(s)

    def read(self, num_bytes=None) -> bytes:
        print("before fill buffer, " + str(len(self.__buffer)) + ", num bytes: " +  str(num_bytes))
        self._fill_buf_bytes(num_bytes)
        print("After fill buffer, ", len(self.__buffer))
        # print("Read of Gzip is called, data: ")
        s = self.__buffer.read(num_bytes)
        print("After read buffer, ", len(self.__buffer))
        return s
        

    
    def close(self):
        self.__input.close()
    
    def peek(self, num_bytes):
        # print("Peek of Gzip is called, data: " + data)
        self._fill_buf_bytes(num_bytes)
        data = self.__buffer.peek(num_bytes)
        # print("Peek of Gzip is called, data: " + data)
        return data


def test_indexed_gzip(file_path):
    source_fileobj = open(file_path, 'rb')
    
    # def fn(*args, **kwargs):
    #     raise io.UnsupportedOperation
    # source_fileobj.seekable = lambda: False
    # source_fileobj.seek = fn

    tar_file = indexed_gzip.IndexedGzipFile(fileobj=GzipStream(source_fileobj), drop_handles=False)
    # peek_info = tar_file.read(2)
    # print(peek_info)
    tar_file.build_full_index()
    # print(len(tar_file.read(10)))
    # print("Second read()")
    # print(len(tar_file.read(1024 * 1024)))
        
file_path = 'test_1g'
test_indexed_gzip(file_path)


def test_gzipstream(file_path):
    source_fileobj = open(file_path, 'rb')
    gzip_file = GzipStream(source_fileobj)
    # peek_info = gzip_file.peek(2)
    # print(peek_info)
    gzip_file.read()
    gzip_file.read()
       
    # tar_file = indexed_gzip.IndexedGzipFile(fileobj=GzipStream(source_fileobj), drop_handles=False, spacing=4*1024*1024)

file_path = 'test_1g'
# test_gzipstream(file_path)



