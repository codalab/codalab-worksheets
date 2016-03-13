"""
zip_util provides helpers for unzipping a few standard archive types when
the user uploads an archive of a known type. Note that when any archives with
just a single file are uploaded, the result is a file, not a directory.

To unzip, we use the standard temp files.
"""
import os
import re
import subprocess
import tempfile

from codalab.common import UsageError
from codalab.lib import path_util, file_util

# Files with these extensions are considered archive.
ARCHIVE_EXTS = ['.tar.gz', '.tgz', '.tar.bz2', '.zip', '.gz']

# When deciding whether an archive contains a single file/directory...

# ... ignore files that match any of these exactly
IGNORE_FILE_EXACT = ['.DS_Store', '__MACOSX']
# ... ignore files that match any of these patterns
IGNORE_FILE_PATTERNS = [re.compile(s) for s in ['^\._.*']]


def ignore_file(filename):
    if filename in IGNORE_FILE_EXACT:
        return True
    return any([pattern.match(filename) for pattern in IGNORE_FILE_PATTERNS])


def path_is_archive(path):
    if isinstance(path, basestring):
        for ext in ARCHIVE_EXTS:
            if path.endswith(ext):
                return True
    return False

def get_archive_ext(fname):
    """Returns the extension for fname if it's an archive, empty string otherwise."""
    for ext in ARCHIVE_EXTS:
        if fname.endswith(ext):
            return ext
    return ''

def strip_archive_ext(path):
    for ext in ARCHIVE_EXTS:
        if path.endswith(ext):
            return path[:-len(ext)]
    raise UsageError('Not an archive: %s' % path)


def unpack(source, dest_path):
    """
    Unpack the archive |source_path| to |dest_path|.
    Note: |source| can be a file handle or a path.
    """
    # Unpack to a temporary location.
    # TODO: guard against zip bombs.  Put a maximum limit and enforce it here.
    # In the future, we probably don't want to be unpacking things all over the place.
    tmp_path = tempfile.mkdtemp('-zip_util.unpack')
    if isinstance(source, basestring):
        source_path = source
        if source_path.endswith('tar.gz') or source_path.endswith('tgz'):
            exitcode = subprocess.call(['tar', 'xfz', source_path, '-C', tmp_path])
        elif source_path.endswith('tar.bz2'):
            exitcode = subprocess.call(['tar', 'xfj', source_path, '-C', tmp_path])
        elif source_path.endswith('zip'):
            exitcode = subprocess.call(['unzip', '-q', source_path, '-d', tmp_path])
        elif source_path.endswith('.gz'):
            with open(os.path.join(tmp_path, os.path.basename(strip_archive_ext(source_path))), 'wb') as f:
                exitcode = subprocess.call(['gunzip', '-q', '-c', source_path], stdout=f)
        else:
            raise UsageError('Not an archive: %s' % source_path)
        if exitcode != 0:
            raise UsageError('Error unpacking %s' % source_path)
    else:
        # File handle, stream the contents!
        source_handle = source
        proc = subprocess.Popen(['tar', 'xfz', '-', '-C', tmp_path], stdin=subprocess.PIPE)
        file_util.copy(source_handle, proc.stdin, print_status='Downloading and unpacking to %s' % tmp_path)
        proc.stdin.close()
        proc.wait()

    # Move files into the right place.
    # If archive only contains one path, then use that.
    files = [f for f in os.listdir(tmp_path) if not ignore_file(f)]
    if len(files) == 1:
        path_util.rename(os.path.join(tmp_path, files[0]), dest_path)
        path_util.remove(tmp_path)
    else:
        path_util.rename(tmp_path, dest_path)
