'''
zip_util provides helpers that:
  a) zip a directory on the local filesystem and return the zip file
  b) unzip a zip file and extract the zipped directory

The zip files here are not arbitrary: they contain one designated
file/directory.  In other words, zip files represent unnamed file/directories.

To zip/unzip, we use the standard temp files.
'''
import os
import shutil
import sys
import tempfile
from zipfile import ZipFile

from codalab.common import UsageError
from codalab.lib import path_util, print_util

ZIP_SUBPATH = 'zip_subpath'


def zip(path, follow_symlinks, exclude_patterns=[], file_name=None):
    '''
    Take a path to a file or directory and return the path to a zip archive
    containing its contents.
    '''
    if isinstance(path, list):
        for p in path:
            absolute_path = path_util.normalize(p)
            path_util.check_isvalid(absolute_path, 'zip_directory')
    else:
        absolute_path = path_util.normalize(path)
        path_util.check_isvalid(absolute_path, 'zip_directory')

    # Add proper name
    if file_name:
        sub_path = file_name
    else:
        sub_path = ZIP_SUBPATH

    # Recursively copy the directory into a temp directory.
    temp_path = tempfile.mkdtemp()
    temp_subpath = os.path.join(temp_path, sub_path)

    print_util.open_line('Copying %s to %s' % (path, temp_subpath))
    if isinstance(path, list):
        os.mkdir(temp_subpath)
        for p in path:
            absolute_path = path_util.normalize(p)
            path_util.copy(absolute_path, os.path.join(temp_subpath, os.path.basename(p)), follow_symlinks=follow_symlinks, exclude_patterns=exclude_patterns)
    else:
        absolute_path = path_util.normalize(path)
        path_util.copy(absolute_path, temp_subpath, follow_symlinks=follow_symlinks, exclude_patterns=exclude_patterns)
    print_util.clear_line()

    zip_path = temp_path + '.zip'
    opts = '-qr'
    if not follow_symlinks: opts += ' --symlinks'
    print_util.open_line('Zipping to %s' % zip_path)
    if os.system("cd %s && zip %s %s %s" % (temp_path, opts, zip_path, sub_path)) != 0:
        raise UsageError('zip failed')

    path_util.remove(temp_path)
    return zip_path, sub_path


def unzip(zip_path, temp_path, sub_path=ZIP_SUBPATH):
    '''
    Take an absolute path to a zip file and return the path to a file or
    directory containing its unzipped contents.
    The returned contents should live in temp_path.
    '''
    path_util.check_isfile(zip_path, 'unzip_directory')
    temp_subpath = os.path.join(temp_path, sub_path)

    print_util.open_line('Unzipping %s to %s' % (zip_path, temp_path))
    if os.system("cd %s && unzip -q %s" % (temp_path, zip_path)) != 0:
        raise UsageError('unzip failed')
    print_util.clear_line()
    # Corner case: note that the temp_subpath might not 'exist' because it is a
    # symlink (which is broken until it's put in the right place).
    if not os.path.exists(temp_subpath) and not os.path.islink(temp_subpath):
        raise UsageError('Zip file %s missing %s (%s doesn\'t exist)' % (zip_path, ZIP_SUBPATH, temp_subpath))

    return temp_subpath
