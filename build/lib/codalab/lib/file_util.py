"""
file_util provides helpers for dealing with file handles in robust,
memory-efficent ways.
"""

import subprocess
import sys

from . import formatting

BUFFER_SIZE = 2 * 1024 * 1024  # 2 MB


def tracked(fileobj, progress_callback):
    class WrappedFile(object):
        def __init__(self):
            self.bytes_read = 0

        def read(self, num_bytes=None):
            buf = fileobj.read(num_bytes)
            self.bytes_read += len(buf)
            progress_callback(self.bytes_read)
            return buf

        def close(self):
            return fileobj.close()

    return WrappedFile()


def copy(source, dest, autoflush=True, print_status=None):
    """
    Read from the source file handle and write the data to the dest file handle.
    """
    n = 0
    while True:
        buf = source.read(BUFFER_SIZE)
        if not buf:
            break
        dest.write(buf)
        n += len(buf)
        if autoflush:
            dest.flush()
        if print_status:
            print("\r%s: %s" % (print_status, formatting.size_str(n)), end=' ', file=sys.stderr)
            sys.stderr.flush()
    if print_status:
        print("\r%s: %s [done]" % (print_status, formatting.size_str(n)), file=sys.stderr)


def strip_git_ext(path):
    GIT_EXT = '.git'
    if path.endswith(GIT_EXT):
        path = path[: -len(GIT_EXT)]
    return path


def git_clone(source_url: str, target_path: str) -> int:
    return subprocess.call(['git', 'clone', '--depth', '1', source_url, target_path])
