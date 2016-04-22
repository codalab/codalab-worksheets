"""
zip_util provides helpers for unzipping a few standard archive types when
the user uploads an archive of a known type.
"""
import os
import shutil
import subprocess
import tarfile
import tempfile

from codalab.common import UsageError
from codalab.lib import path_util, file_util
from worker.file_util import un_tar_directory, un_gzip_stream


# Files with these extensions are considered archive.
ARCHIVE_EXTS = ['.tar.gz', '.tgz', '.tar.bz2', '.zip', '.gz']


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


def unpack(ext, source, dest_path):
    """
    Unpack the archive |source| to |dest_path|.
    Note: |source| can be a file handle or a path.
    |ext| contains the extension of the archive.
    """
    if ext != '.zip':
        close_source = False
        try:
            if isinstance(source, basestring):
                source = open(source, 'rb')
                close_source = True

            if ext == '.tar.gz' or ext == '.tgz':
                un_tar_directory(source, dest_path, 'gz')
            elif ext == '.tar.bz2':
                un_tar_directory(source, dest_path, 'bz2')
            elif ext == '.gz':
                with open(dest_path, 'wb') as f:
                    shutil.copyfileobj(un_gzip_stream(source), f)
            else:
                raise UsageError('Not an archive.')
        except (tarfile.TarError, IOError):
            raise UsageError('Invalid archive upload.')
        finally:
            if close_source:
                source.close()
    else:
        delete_source = False
        try:
            # unzip doesn't accept input from standard input, so we have to save
            # to a temporary file.
            if not isinstance(source, basestring):
                temp_path = dest_path + '.zip'
                with open(temp_path, 'wb') as f:
                    shutil.copyfileobj(source, f)
                source = temp_path
                delete_source = True

            exitcode = subprocess.call(['unzip', '-q', source, '-d', dest_path])
            if exitcode != 0:
                raise UsageError('Invalid archive upload.')
        finally:
            if delete_source:
                path_util.remove(source)
