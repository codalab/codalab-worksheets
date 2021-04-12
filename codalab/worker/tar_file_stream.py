from contextlib import ExitStack
import ratarmount
import tarfile
from typing import Optional
from dataclasses import dataclass
from io import SEEK_SET, SEEK_CUR, SEEK_END

from codalab.worker.un_gzip_stream import BytesBuffer
from codalab.common import parse_linked_bundle_url
from ratarmount import FileInfo, SQLiteIndexedTar


class TarFileStream(object):
    """Streams a file from a tar archive.
    """

    BUFFER_SIZE = 100 * 1024 * 1024  # Read in chunks of 100MB

    def __init__(self, tf: SQLiteIndexedTar, finfo: FileInfo):
        self.tf = tf
        self.finfo = finfo
        self._buffer = BytesBuffer()
        self.output = tarfile.open(fileobj=self._buffer, mode="w:")
        self.pos = 0

    def _read_from_tar(self, num_bytes):
        # Read the contents of the current descendant.
        contents = self.tf.read(
            path="",
            fileInfo=self.finfo,
            size=self.finfo.size
            if num_bytes is None
            else min(self.finfo.size - self.pos, num_bytes),
            offset=self.pos,
        )
        self._buffer.write(contents)
        self.pos += len(contents)

    def read(self, num_bytes=None):
        # Read more data, if we need to.
        while (self.pos < self.finfo.size) and (num_bytes is None or len(self._buffer) < num_bytes):
            self._read_from_tar(TarFileStream.BUFFER_SIZE)
        if num_bytes is None:
            num_bytes = len(self._buffer)
        return self._buffer.read(num_bytes)

    def seek(self, pos, whence=SEEK_SET):
        # TODO: implement whence for file seeking.
        if whence == SEEK_SET:
            self.pos = pos
        elif whence == SEEK_CUR:
            self.pos += pos
        elif whence == SEEK_END:
            self.pos = self.finfo.size - pos

    def tell(self):
        return self.pos

    def __getattr__(self, name):
        """
        Proxy any methods/attributes besides read() and close() to the
        fileobj (for example, if we're wrapping an HTTP response object.)
        Behavior is undefined if other file methods such as tell() are
        attempted through this proxy.
        """
        return getattr(self._buffer, name)
