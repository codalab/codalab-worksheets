"""
file_util provides helpers for dealing with file handles in robust,
memory-efficent ways.
"""

import sys
from . import formatting
import subprocess
from codalab.common import urlopen_with_retry

BUFFER_SIZE = 2 * 1024 * 1024


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


def git_clone(source_url, target_path):
    return subprocess.call(['git', 'clone', source_url, target_path])


def download_url(source_url, target_path, print_status=False):
    """
    Download the file at |source_url| and write it to |target_path|.
    """
    in_file = urlopen_with_retry(source_url)
    total_bytes = in_file.info().get('Content-Length')
    if total_bytes:
        total_bytes = int(total_bytes)

    num_bytes = 0
    out_file = open(target_path, 'wb')

    def status_str():
        if total_bytes:
            return 'Downloaded %s/%s (%d%%)' % (
                formatting.size_str(num_bytes),
                formatting.size_str(total_bytes),
                100.0 * num_bytes / total_bytes,
            )
        else:
            return 'Downloaded %s' % (formatting.size_str(num_bytes))

    while True:
        s = in_file.read(BUFFER_SIZE)
        if not s:
            break
        out_file.write(s)
        num_bytes += len(s)
        if print_status:
            print('\r' + status_str(), end=' ', file=sys.stderr)
            sys.stderr.flush()
    if print_status:
        print('\r' + status_str() + ' [done]', file=sys.stderr)
