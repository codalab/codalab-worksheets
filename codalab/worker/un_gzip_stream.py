import bz2
import datetime
import struct
import tarfile
import time
import zlib

from collections import deque
from io import BytesIO
from typing import Optional
from zipfile import (  # type: ignore
    BadZipFile,
    structFileHeader,
    stringFileHeader,
    sizeFileHeader,
    stringCentralDir,
    _FH_SIGNATURE,
    _FH_FILENAME_LENGTH,
    _FH_EXTRA_FIELD_LENGTH,
    _FH_COMPRESSED_SIZE,
    _FH_UNCOMPRESSED_SIZE,
)

from codalab.lib.beam.streamingzipfile import StreamingZipFile


class GenericUncompressStream(BytesIO):
    """Generic base class that uncompresses a stream.
    Subclasses must set decoder, which must be an instance of a
    class that implements the decompress(chunk) method and (optionally)
    then flush() method.
    """

    decoder = None

    def __init__(self, fileobj):
        self._fileobj = fileobj
        self._buffer = BytesBuffer()
        self._finished = False

    def read(self, num_bytes=None):
        # Read more data, if we need to.
        while not self._finished and (num_bytes is None or len(self._buffer) < num_bytes):
            chunk = self._fileobj.read(num_bytes) if num_bytes is not None else self._fileobj.read()
            if chunk:
                self._buffer.write(self.decoder.decompress(chunk))
            else:
                if hasattr(self.decoder, "flush"):
                    self._buffer.write(self.decoder.flush())
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


class UnGzipStream(GenericUncompressStream):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)


class UnBz2Stream(GenericUncompressStream):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.decoder = bz2.BZ2Decompressor()


class ZipToTarDecompressor:
    def __init__(self, buffer):
        self.output = tarfile.open(fileobj=buffer, mode="w:")
        self.finished = False
        self.reset_info()

    def reset_info(self):
        """Reset information. This method should be called when
        at the beginning of each new file in the zip archive.
        """
        # Buffers used to construct file headers.
        self.header_buf = BytesBuffer()
        self.header_buf_2 = BytesBuffer()

        # fheader struct corresponding to the first part of the file header
        # of the current file.
        self.current_fheader = None

        # If set, we've finished reading the current entire file header,
        # converted it to the currently set zipinfo, and are now
        # reading the current file body.
        self.current_zipinfo = None

        # Compressed size of the current file.
        self.current_file_compressed_size = None

        # Uncompressed size of the current file.
        self.current_file_uncompressed_size = None

        # Number of bytes we have already obtained for the current compressed file.
        self.current_file_compressed_bytes_obtained = 0

        # File handle to the current file that is open in the zip file.
        self.current_zef = None

        # Buffer used to send a file header / body to StreamingZipFile to be read.
        self.buf = BytesBuffer()

    def decompress(self, chunk):
        chunkbuffer = BytesBuffer()
        chunkbuffer.write(chunk)
        output = BytesBuffer()
        while not self.finished and len(chunkbuffer) > 0:
            if not self.current_fheader:
                # Read the first part of the header (constant size of sizeFileHeader).
                if len(self.header_buf) <= sizeFileHeader:
                    bytes_remaining = sizeFileHeader - len(self.header_buf)
                    self.header_buf.write(chunkbuffer.read(bytes_remaining))

                if len(self.header_buf) >= sizeFileHeader:
                    fheader = self.header_buf.read(sizeFileHeader)
                    self.current_fheader = struct.unpack(structFileHeader, fheader)
                    if self.current_fheader[_FH_SIGNATURE] == stringCentralDir:
                        # We've reached the central directory. This means that we've finished iterating through
                        # all entries in the zip file. We can do this check because the file header signature
                        # and central directory signature are stored in the same spot (index 0) and with the same format.
                        self.finished = True
                        break
                    if self.current_fheader[_FH_SIGNATURE] != stringFileHeader:
                        raise BadZipFile("Bad magic number for file header")
                    self.current_file_compressed_size = self.current_fheader[_FH_COMPRESSED_SIZE]
                    self.current_file_uncompressed_size = self.current_fheader[
                        _FH_UNCOMPRESSED_SIZE
                    ]
                    # Finished reading the first part of the header.
                    self.buf.write(fheader)

            if self.current_fheader and not self.current_zipinfo:
                # Read the second part of the header (variable size sizeFileHeaderExtra obtained by reading self.current_fheader).
                sizeFileHeader2 = (
                    self.current_fheader[_FH_FILENAME_LENGTH]
                    + self.current_fheader[_FH_EXTRA_FIELD_LENGTH]
                )
                if len(self.header_buf_2) <= sizeFileHeader2:
                    bytes_remaining = sizeFileHeader2 - len(self.header_buf_2)
                    self.header_buf_2.write(chunkbuffer.read(bytes_remaining))

                if len(self.header_buf_2) >= sizeFileHeader2:
                    fheader_2 = self.header_buf_2.read(sizeFileHeader2)
                    # Finished reading the entire header.
                    self.buf.write(fheader_2)

                    with StreamingZipFile(self.buf) as zf:
                        # Header finished; write the tarfile header now.
                        zinfo = zf.next()
                        self.current_zipinfo = zinfo
                        tarinfo = tarfile.TarInfo(name=zinfo.filename)
                        tarinfo.size = zinfo.file_size
                        tarinfo.mode = 0o755  # ZipFile doesn't store permissions, so we just set it to a sensible default.
                        tarinfo.type = tarfile.DIRTYPE if zinfo.is_dir() else tarfile.REGTYPE
                        tarinfo.mtime = time.mktime(
                            datetime.datetime(*zinfo.date_time).timetuple()
                        )  # From https://fossies.org/linux/littleutils/scripts/zip2tarcat.in
                        self.output.addfile(tarinfo)

            if self.current_zipinfo:
                # Header finished; write (up to) the entire body of the current member.
                bytes_remaining = (
                    self.current_file_compressed_size - self.current_file_compressed_bytes_obtained
                )
                remaining = chunkbuffer.read(bytes_remaining)
                self.current_file_compressed_bytes_obtained += len(remaining)
                self.buf.write(remaining)

                # Extract what's remaining from the zipfile and write it to the tarfile.
                if not self.current_zef:
                    with StreamingZipFile(self.buf) as zf:
                        self.current_zef = zf.open(self.current_zipinfo)

                # ZipExtFile._read1(n) reads up to n compressed bytes. We use this rather than ZipExtFile.read(n), which takes in uncompressed
                # bytes, because we only know how much compressed bytes we have added so far.
                uncompressed = self.current_zef._read1(len(remaining))
                self.output.fileobj.write(uncompressed)
                self.output.offset += len(uncompressed)

                if self.current_file_compressed_bytes_obtained == self.current_file_compressed_size:
                    # We've obtained the entire file.
                    # Write the remainder of the block, if needed, and then reset the current information.
                    # This code for writing the remainder of the block is taken from
                    # https://github.com/python/cpython/blob/9d2c2a8e3b8fe18ee1568bfa4a419847b3e78575/Lib/tarfile.py#L2008-L2012.
                    blocks, remainder = divmod(self.current_file_compressed_size, tarfile.BLOCKSIZE)
                    if remainder > 0:
                        assert self.output.fileobj is not None
                        self.output.fileobj.write(tarfile.NUL * (tarfile.BLOCKSIZE - remainder))
                        blocks += 1
                    self.output.offset += blocks * tarfile.BLOCKSIZE  # type: ignore
                    # Reset the current information.
                    self.reset_info()

        return output.read()


class ZipToTarStream(GenericUncompressStream):
    """Converts a .zip file to a .tar file."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.decoder = ZipToTarDecompressor(self._buffer)


def un_gzip_stream(fileobj):
    """
    Returns a file-like object containing the contents of the given file-like
    object after gunzipping.

    Raises an IOError if the archive is not valid.
    """

    # Note, that we don't use gzip.GzipFile or the gunzip shell command since
    # they require the input file-like object to support either tell() or
    # fileno(). Our version requires only read() and close().
    return UnGzipStream(fileobj)


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

    def flush(self):
        pass

    def close(self):
        pass

    def tell(self):
        return self.__pos

    def __bool__(self):
        return True
