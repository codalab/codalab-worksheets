import bz2
import os
import tarfile
import zlib

from io import BytesIO
from typing import Optional, Any, IO

from codalab.worker.un_gzip_stream import BytesBuffer
from codalab.common import parse_linked_bundle_url
from codalab.lib.upload_manager import Source
from codalab.lib.zip_util import ARCHIVE_EXTS


class BaseSource:
    source_name: str
    simplify_archives: Optional[bool]
    _buffer: IO
    output_fileobj: IO
    output_tarfile: tarfile.TarFile

    def __init__(
        self,
        source_name: str,
        output_fileobj: IO,
        output_tarfile: tarfile.TarFile,
        simplify_archives: Optional[bool] = None,
    ):
        self.source_name = source_name
        self.simplify_archives = simplify_archives
        self._buffer = BytesBuffer()
        self.output_fileobj = output_fileobj
        self.output_tarfile = output_tarfile

    def add_header(self, size=0):
        tinfo = tarfile.TarInfo(self.source_name)
        tinfo.type = tarfile.DIRTYPE
        tinfo.size = size
        self.output_tarfile.write()

    def add_body(self, data):
        pass

    def write(self, data):
        raise NotImplementedError

    def close(self):
        pass


class TarSource(BaseSource):
    # def ()
    pass


class ZipSource(BaseSource):
    pass


class BaseSingleFileSource(BaseSource):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        self._size = 0
    
    @property
    def decoder(self):
        raise NotImplementedError
    
    def write(self, data):
        chunk = self.decoder.decompress(data)
        self.output_fileobj.write(chunk)
        self._size += len(chunk)

    def close(self):
        end_of_file = self.tell()

        if self._size > 0:
            # This code for writing the remainder of the block is taken from
            # https://github.com/python/cpython/blob/9d2c2a8e3b8fe18ee1568bfa4a419847b3e78575/Lib/tarfile.py#L2008-L2012.
            blocks, remainder = divmod(self._size, tarfile.BLOCKSIZE)
            if remainder > 0:
                assert self.output.fileobj is not None
                self.output.fileobj.write(tarfile.NUL * (tarfile.BLOCKSIZE - remainder))
                blocks += 1
            self.output_tarfile.offset += blocks * tarfile.BLOCKSIZE  # type: ignore
        end_of_block = self.tell()

        # Go back and update the file header to include this file's size.
        self.output_fileobj.seek(end_of_file - self._size - 512)
        self.add_header(self._size)
        self.seek(end_of_block)


class GzSource(BaseSingleFileSource):
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)


class Bz2Source(BaseSingleFileSource):
    decoder = bz2.BZ2Decompressor().decompress


class PlainFileSource(BaseSource):
    pass


# Corresponding extensions
EXT_TO_CLASS = {
    '.tar.gz': TarSource(mode='r:gz'),
    '.tgz': TarSource(mode='r:gz'),
    '.tar.bz2': TarSource(mode='r:bz2'),
    '.zip': ZipSource(),
    '.gz': GzSource(),
    '.bz2': Bz2Source(),
}


class TarFromSources:
    """Creates a tar archive from multiple sources. As sources are added using
    .add_source(), the sources will be streamed and added to the tar file.
    
    If only one source with a single file is added, the archive will contain
    only a single file with an arcname equal to the bundle UUID.

    Otherwise, if multiple sources are added, the archive will contain the contents
    of each source under a subfolder with the source name.

    Finally, when everything has been written, you should call the .close() method
    to finish writing the tarfile.
    """

    # UUID of related bundle.
    bundle_uuid: str

    # Whether the bundle is a directory.
    is_directory: Optional[bool]

    # Underlying file-like object for the output tarfile.
    _fileobj: IO

    # Output tarfile.
    output: tarfile.TarFile

    def __init__(self, fileobj: IO, mode: str, bundle_uuid: str):
        """Initialize TarFromSources

        Args:
            fileobj (IO): The fileobj 
            mode (str): Mode used for writing the final tarfile. For example: "w:gz" or "w:"
            bundle_uuid (str): UUID of related bundle.
        """
        self.bundle_uuid = bundle_uuid
        self._fileobj = fileobj
        self.output = tarfile.open(fileobj=fileobj, mode=mode)

    def add_source(
        self,
        source_name: str,
        archive_ext: Optional[str] = None,
        simplify_archives: Optional[bool] = None,
    ):
        """Add a source to the tarfile. Returns a file object that the source contents should be written to.

        Args:
            source_name (str): [description]
            archive_ext (Optional[str], optional): [description]. Defaults to None.
            simplify_archives (Optional[bool], optional): [description]. Defaults to None.
        """
        source_cls = EXT_TO_CLASS[archive_ext] if archive_ext else PlainFileSource
        return source_cls(
            source_name=source_name,
            output_fileobj=self._fileobj,
            output_tarfile=self.output,
            simplify_archives=simplify_archives,
        )

    def close(self):
        """Close the tarfile; should be called when finished writing.
        """
        self.tarfile.close()
        # todo: fix this logic.
        self.is_directory = True
        # TODO: if is_dir is true, run simplify directory in constructor of TarFromSources
        # with tarfile.open(tmp_tar_file.name, "w:gz") as tar:
        #             tar.add(bundle_path, arcname=bundle.uuid)
