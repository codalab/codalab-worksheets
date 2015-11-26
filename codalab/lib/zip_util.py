"""
zip_util provides helpers that:
  a) zip a directory on the local filesystem and return the zip file
  b) unzip a zip file and extract the zipped directory

The zip files here are not arbitrary: they contain one designated
file/directory.  In other words, zip files represent unnamed file/directories.

To zip/unzip, we use the standard temp files.
"""
import os
import shutil
import sys
import subprocess
import tempfile

from codalab.common import UsageError
from codalab.lib import path_util, print_util, file_util

# Files with these extensions are considered archive.
ARCHIVE_EXTS = ['.tar.gz', '.tgz', '.tar.bz2', '.zip']

# When deciding whether an archive contains a single file/directory, ignore
# these contents.
IGNORE_FILES = ['.DS_Store', '__MACOSX']

def path_is_archive(path):
    if isinstance(path, basestring):
        for ext in ARCHIVE_EXTS:
            if path.endswith(ext):
                return True
    return False


def strip_archive_ext(path):
    for ext in ARCHIVE_EXTS:
        if path.endswith(ext):
            return path[:-len(ext)]
    raise UsageError('Not an archive: %s' % path)


def add_packed_suffix(path):
    """
    Add the packed suffix for path if it's not an archive.
    """
    if path_is_archive(path):
        return path
    return path + '.tar.gz'


def open_packed_path(source, follow_symlinks, exclude_patterns):
    """
    Return file handle corresponding to |source|, which is either
    - an archive file: just stream it.
    - else: turn it into an archive
    """
    if path_is_archive(source):
        return open(source)
    args = ['tar', 'cfz', '-', '-C', os.path.dirname(source) or '.', os.path.basename(source)]
    if follow_symlinks:
        args.append('-h')
    if exclude_patterns is not None:
        for pattern in exclude_patterns:
            args.append('--exclude=' + pattern)
    proc = subprocess.Popen(args, stdout=subprocess.PIPE)
    return proc.stdout


def unpack(source, dest_path):
    """
    Unpack the archive |source_path| to |dest_path|.
    Note: |source| can be a file handle or a path.
    """
    # Unpack to a temporary location.
    tmp_path = tempfile.mkdtemp('-zip_util.unpack')
    if isinstance(source, basestring):
        source_path = source
        if source_path.endswith('tar.gz') or source_path.endswith('tgz'):
            subprocess.call(['tar', 'xfz', source_path, '-C', tmp_path])
        elif source_path.endswith('tar.bz2'):
            subprocess.call(['tar', 'xfj', source_path, '-C', tmp_path])
        elif source_path.endswith('zip'):
            subprocess.call(['unzip', '-q', source_path, '-d', tmp_path])
        else:
            raise UsageError('Not an archive: ' % source_path)
    else:
        # File handle, stream the contents!
        source_handle = source
        proc = subprocess.Popen(['tar', 'xfz', '-', '-C', tmp_path], stdin=subprocess.PIPE)
        file_util.copy(source_handle, proc.stdin, print_status='Downloading and unpacking to %s' % tmp_path)
        proc.stdin.close()
        proc.wait()

    # Move files into the right place.
    # If archive only contains one path, then use that.
    files = [f for f in os.listdir(tmp_path) if f not in IGNORE_FILES]
    if len(files) == 1:
        path_util.rename(os.path.join(tmp_path, files[0]), dest_path)
        path_util.remove(tmp_path)
    else:
        path_util.rename(tmp_path, dest_path)
