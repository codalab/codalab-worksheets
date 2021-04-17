from contextlib import ExitStack
from ratarmount import FileInfo
import tarfile
from io import BytesIO
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
    finfo: FileInfo  # FileInfo corresponding to current descendant (ratarmount-specific data structure)
    tinfo: tarfile.TarInfo  # TarInfo corresponding to current descendant (tarfile-specific data structure)


# Used to initialize empty FileInfo objects
EmptyFileInfo = FileInfo(
    offsetheader=None,
    offset=None,
    size=None,
    mtime=None,
    mode=None,
    type=None,
    linkname=None,
    uid=None,
    gid=None,
    istar=None,
    issparse=None,
)


class TarSubdirStream(BytesIO):
    """Streams a subdirectory from a tar file stored on Blob Storage, as its own tar archive.

    The general idea is that on initialization, this class will construct a list
    "descendants" that contains all files within the specified subdirectory in the tar file.
    Whenever .read() is called on this class, it will partially construct a tar file
    with the headers and contents of each descendant, up to the specified number of bytes,
    and return those bytes.

    Inspired by https://gist.github.com/chipx86/9598b1e4a9a1a7831054.

    TODO (Ashwin): Right now, all the descendants of the subdirectory must first be retrieved
    before the subdirectory can begin to be streamed. We can stream those descendants
    as well, though it may not be that important of a performance optimization
    because the descendants are all stored in the index.
    """

    current_desc: CurrentDescendant

    def __init__(self, path: str):
        """Initialize TarSubdirStream.

        Args:
            path (str): Specified path of the subdirectory on Blob Storage. Must refer to a subdirectory path within a .tar.gz file.
        """
        from codalab.worker.file_util import OpenIndexedTarGzFile
        from codalab.worker.download_util import compute_target_info_beam_descendants_flat

        self.linked_bundle_path = parse_linked_bundle_url(path)

        # We add OpenIndexedTarGzFile to self._stack so that the context manager remains open and is exited
        # only in the method self.close().
        with ExitStack() as stack:
            self.tf = stack.enter_context(OpenIndexedTarGzFile(self.linked_bundle_path.bundle_path))
            self._stack = stack.pop_all()

        # Keep track of descendants of the specified subdirectory and the current descendant
        self.descendants = compute_target_info_beam_descendants_flat(path)
        self.current_desc = CurrentDescendant(
            index=0, pos=0, read_header=False, finfo=EmptyFileInfo, tinfo=tarfile.TarInfo()
        )

        # Buffer that stores the underlying bytes of the output tar archive
        self._buffer = BytesBuffer()

        # Output tar archive
        self.output = tarfile.open(fileobj=self._buffer, mode="w:")

    def _read_from_tar(self, num_bytes=None) -> None:
        """Read the specified number of bytes from the tar file
        associated with the given subdirectory.

        Based on where we currently are within the subdirectory's descendants,
        either read the next descendant's header or its contents.
        """
        if not self.current_desc.read_header:
            # Read the header of the current descendant.
            # TODO (Ashwin): Make sure this works with symlinks, too (it should work, but add a test to ensure it).
            member = self.descendants[self.current_desc.index]
            full_name = f"{self.linked_bundle_path.archive_subpath}/{member['name']}"
            member_finfo = self.tf.getFileInfo("/" + full_name)
            member_tarinfo = tarfile.TarInfo(name="./" + member['name'] if member['name'] else '.')
            for attr in ("size", "mtime", "mode", "type", "linkname", "uid", "gid"):
                setattr(member_tarinfo, attr, getattr(member_finfo, attr))

            # finfo is a ratarmount-specific data structure, while tinfo is a tarfile-specific data structure.
            # We need the former in order to read from the file with ratarmount and the latter in order to
            # construct the output tar archive.
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
            assert self.output.fileobj is not None
            self.output.fileobj.write(chunk)
            self.current_desc.pos += len(chunk)
            # We're ignoring types here because the TarFile.offset type is missing.
            # TODO: Remove "# type: ignore" annotations once this PR is merged: https://github.com/python/typeshed/pull/5210
            self.output.offset += len(chunk)  # type: ignore
        else:
            # We've finished reading the entire current descendant.
            # Write the remainder of the block, if needed, and then move on to the next descendant.
            if self.current_desc.pos > 0:
                # This code for writing the remainder of the block is taken from
                # https://github.com/python/cpython/blob/9d2c2a8e3b8fe18ee1568bfa4a419847b3e78575/Lib/tarfile.py#L2008-L2012.
                blocks, remainder = divmod(self.current_desc.tinfo.size, tarfile.BLOCKSIZE)
                if remainder > 0:
                    assert self.output.fileobj is not None
                    self.output.fileobj.write(tarfile.NUL * (tarfile.BLOCKSIZE - remainder))
                    blocks += 1
                self.output.offset += blocks * tarfile.BLOCKSIZE  # type: ignore
            self.current_desc = CurrentDescendant(
                index=self.current_desc.index + 1,
                pos=0,
                read_header=False,
                finfo=EmptyFileInfo,
                tinfo=tarfile.TarInfo(),
            )

    def read(self, num_bytes=None):
        """Read the specified number of bytes from the tar version of the associated subdirectory.
        """
        while num_bytes is None or len(self._buffer) < num_bytes:
            if self.current_desc.index >= len(self.descendants):
                self.close()
                break
            self._read_from_tar(num_bytes)
        if num_bytes is None:
            num_bytes = len(self._buffer)
        return self._buffer.read(num_bytes)

    def close(self):
        # Close the OpenIndexedTarGzFile context manager that was initialized in __init__.
        self._stack.__exit__(self, None, None)

    def __getattr__(self, name):
        """
        Proxy any methods/attributes besides read() and close() to the
        fileobj (for example, if we're wrapping an HTTP response object.)
        Behavior is undefined if other file methods such as tell() are
        attempted through this proxy.
        """
        return getattr(self._buffer, name)
