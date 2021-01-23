from collections import deque
import zlib


def un_gzip_stream(fileobj):
    """
    Returns a file-like object containing the contents of the given file-like
    object after gunzipping.

    Raises an IOError if the archive is not valid.
    """

    class UnGzipStream(object):
        def __init__(self, fileobj):
            self._fileobj = fileobj
            self._decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
            self._buffer = BytesBuffer()
            self._finished = False

        def read(self, num_bytes=None):
            # Read more data, if we need to.
            while not self._finished and (num_bytes is None or len(self._buffer) < num_bytes):
                chunk = (
                    self._fileobj.read(num_bytes) if num_bytes is not None else self._fileobj.read()
                )
                if chunk:
                    self._buffer.write(self._decoder.decompress(chunk))
                else:
                    self._buffer.write(self._decoder.flush())
                    self._finished = True
            if num_bytes is None:
                num_bytes = len(self._buffer)
            return self._buffer.read(num_bytes)

        def close(self):
            self._fileobj.close()

        def __getattr__(self, name):
            """
            Proxy any methods/attributes besides read() and close() to the
            fileobj (for example, if we're wrapping an HTTP response object.)
            Behavior is undefined if other file methods such as tell() are
            attempted through this proxy.
            """
            return getattr(self._fileobj, name)

    # Note, that we don't use gzip.GzipFile or the gunzip shell command since
    # they require the input file-like object to support either tell() or
    # fileno(). Our version requires only read() and close().
    return UnGzipStream(fileobj)


class BytesBuffer:
    """
    A class for a buffer of bytes. Unlike io.BytesIO(), this class
    keeps track of the buffer's size (in bytes).
    """

    def __init__(self):
        self.__buf = deque()
        self.__size = 0

    def __len__(self):
        return self.__size

    def write(self, data):
        self.__buf.append(data)
        self.__size += len(data)

    def read(self, size=-1):
        if size < 0:
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
        return ret

    def flush(self):
        pass

    def close(self):
        pass
