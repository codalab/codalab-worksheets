from io import SEEK_SET, SEEK_CUR, SEEK_END, BytesIO

from codalab.worker.un_gzip_stream import BytesBuffer
from ratarmountcore import FileInfo, SQLiteIndexedTar


class TarFileStream(BytesIO):
    """Streams a file from a tar archive stored on Blob Storage.

    The general idea is that whenever .read() is called on this class,
    it will read the specified number of bytes through ratarmount's tf.open()
    API on the associated file and return those bytes.

    TODO (Ashwin): If we can add tf.open() support upstream to the ratarmount API
    (right now it only supports tf.read()), we may not have a need for this class anymore.
    """

    def __init__(self, tf: SQLiteIndexedTar, finfo: FileInfo):
        """Initialize TarFileStream.

        Args:
            tf (SQLiteIndexedTar): Tar archive indexed by ratarmount.
            finfo (FileInfo): FileInfo object describing the file that is to be read from the aforementioned tar archive.
        """
        self.tf = tf
        self.finfo = finfo
        self._buffer = BytesBuffer()
        self.pos = 0

    def _read_from_tar(self, num_bytes):
        """Read the contents of the specified file from within
        the tar archive.
        """
        contents = self.tf.read(
            fileInfo=self.finfo,
            size=self.finfo.size
            if num_bytes is None
            else min(self.finfo.size - self.pos, num_bytes),
            offset=self.pos,
        )
        self._buffer.write(contents)
        self.pos += len(contents)

    def read(self, num_bytes=None):
        """Read the specified number of bytes from the associated file.
        """
        while (self.pos < self.finfo.size) and (num_bytes is None or len(self._buffer) < num_bytes):
            self._read_from_tar(num_bytes)
        if num_bytes is None:
            num_bytes = len(self._buffer)
        return self._buffer.read(num_bytes)

    def seek(self, pos, whence=SEEK_SET):
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
