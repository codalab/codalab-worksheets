from contextlib import ExitStack
import ratarmount
import tarfile
from typing import Optional
from dataclasses import dataclass

from codalab.worker.un_gzip_stream import BytesBuffer
from codalab.common import parse_linked_bundle_url


@dataclass()
class CurrentDescendant:
    """Current descendant, used in TarSubdirStream.
    """

    index: int  # Current index of descendants list
    pos: int  # Position within the current descendant
    read_header: bool  # Whether header has been read yet
    finfo: Optional[ratarmount.FileInfo]  # FileInfo corresponding to current descendant
    tinfo: Optional[tarfile.TarInfo]  # TarInfo corresponding to current descendant


class TarSubdirStream(object):
    """Streams a subdirectory from a tar file.
    """

    current_desc: CurrentDescendant

    BUFFER_SIZE = 100 * 1024 * 1024  # Read in chunks of 100MB

    def __init__(self, path: str):
        from codalab.worker.file_util import OpenIndexedTarGzFile
        from codalab.worker.download_util import compute_target_info_beam_descendants_flat

        self.linked_bundle_path = parse_linked_bundle_url(path)
        with ExitStack() as stack:
            self.tf = stack.enter_context(OpenIndexedTarGzFile(self.linked_bundle_path.bundle_path))
            self._stack = stack.pop_all()
        self.descendants = compute_target_info_beam_descendants_flat(path)
        self.current_desc = CurrentDescendant(
            index=0, pos=0, read_header=False, finfo=None, tinfo=None
        )
        self._buffer = BytesBuffer()
        self.output = tarfile.open(fileobj=self._buffer, mode="w:")

    def _read_from_tar(self, num_bytes=None) -> bytes:
        """Read from all descendants."""
        # Extract other members of the directory.
        # TODO (Ashwin): Make sure this works with symlinks, too (it should work, but add a test to ensure it).
        if not self.current_desc.read_header:
            # Read the header of the current descendant.
            member = self.descendants[self.current_desc.index]
            full_name = f"{self.linked_bundle_path.archive_subpath}/{member['name']}"
            member_finfo = self.tf.getFileInfo("/" + full_name)
            member_tarinfo = tarfile.TarInfo(name="./" + member['name'] if member['name'] else '.')
            for attr in ("size", "mtime", "mode", "type", "linkname", "uid", "gid"):
                setattr(member_tarinfo, attr, getattr(member_finfo, attr))
            self.current_desc.finfo = member_finfo
            self.current_desc.tinfo = member_tarinfo
            self.current_desc.read_header = True
            self.output.addfile(member_tarinfo)
        elif self.current_desc.pos < self.current_desc.finfo.size:
            # Read the contents of the current descendant.
            chunk = self.tf.read(
                path="",
                fileInfo=self.current_desc.finfo,
                size=self.current_desc.finfo.size
                if num_bytes is None
                else min(self.current_desc.finfo.size - self.current_desc.pos, num_bytes),
                offset=self.current_desc.pos,
            )
            self.output.fileobj.write(chunk)
            self.current_desc.pos += len(chunk)
            self.output.offset += len(chunk)
        else:
            # Finished reading the entire current descendant. Write the remainder of the block,
            # if needed, and then move on to the next descendant.
            if self.current_desc.pos > 0:
                # Add the right remainder of the tar block -- from https://github.com/python/cpython/blob/9d2c2a8e3b8fe18ee1568bfa4a419847b3e78575/Lib/tarfile.py#L2008-L2012.
                blocks, remainder = divmod(self.current_desc.tinfo.size, tarfile.BLOCKSIZE)
                if remainder > 0:
                    self.output.fileobj.write(tarfile.NUL * (tarfile.BLOCKSIZE - remainder))
                    blocks += 1
                self.output.offset += blocks * tarfile.BLOCKSIZE
            self.current_desc = CurrentDescendant(
                index=self.current_desc.index + 1, pos=0, read_header=False, finfo=None, tinfo=None,
            )

    def read(self, num_bytes=None):
        # Read more data, if we need to.
        while num_bytes is None or len(self._buffer) < num_bytes:
            if self.current_desc.index >= len(self.descendants):
                self.close()
                break
            self._read_from_tar(TarSubdirStream.BUFFER_SIZE)
        if num_bytes is None:
            num_bytes = len(self._buffer)
        return self._buffer.read(num_bytes)

    def close(self):
        self._stack.__exit__(self, None, None)

    def __getattr__(self, name):
        """
        Proxy any methods/attributes besides read() and close() to the
        fileobj (for example, if we're wrapping an HTTP response object.)
        Behavior is undefined if other file methods such as tell() are
        attempted through this proxy.
        """
        return getattr(self._buffer, name)
