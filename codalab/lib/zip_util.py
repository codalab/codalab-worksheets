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
import tempfile
from zipfile import ZipFile

from codalab.common import UsageError
from codalab.lib import path_util


ZIP_SUBPATH = 'zip_subpath'


def zip(path):
    '''
    Take a path to a file or directory and return the path to a zip archive
    containing its contents.
    '''
    absolute_path = path_util.normalize(path)
    path_util.check_isvalid(absolute_path, 'zip_directory')
    # Recursively copy the directory into a temp directory.
    temp_path = tempfile.mkdtemp()
    temp_subpath = os.path.join(temp_path, ZIP_SUBPATH)

    # TODO: this is inefficient; do the zipping from the original source
    # directly.
    path_util.copy(absolute_path, temp_subpath)

    # TODO: These methods of zipping don't preserve permissions, so using a
    # system call for now (only works in Linux)
    # Multiplex between zipping a directory and zipping a file here, because
    # make_archive does NOT handle the file case cleanly.
    #if os.path.isdir(temp_subpath):
    #    zip_path = shutil.make_archive(
    #      base_name=temp_path,
    #      base_dir=ZIP_SUBPATH,
    #      root_dir=temp_path,
    #      format='zip',
    #    )
    #else:
    #    zip_path = temp_path + '.zip'
    #    with ZipFile(zip_path, 'w') as zip_file:
    #        zip_file.write(temp_subpath, ZIP_SUBPATH)
    # Clean up the temporary directory and return the zip file's path.

    zip_path = temp_path + '.zip'
    os.system("cd %s && zip -qr %s %s" % (temp_path, zip_path, ZIP_SUBPATH))

    path_util.remove(temp_path)
    return zip_path


def unzip(zip_path, temp_path):
    '''
    Take an absolute path to a zip file and return the path to a file or
    directory containing its unzipped contents.
    The returned contents should live in temp_path.
    '''
    path_util.check_isfile(zip_path, 'unzip_directory')
    temp_subpath = os.path.join(temp_path, ZIP_SUBPATH)

    # TODO(pliang): ZipFile doesn't preserve permissions, so do hack
    os.system("cd %s && unzip -q %s" % (temp_path, zip_path))
    if not os.path.exists(temp_subpath):
        raise UsageError('Zip file %s missing %s' % zip_path, ZIP_SUBPATH)

    #zip_file = ZipFile(zip_path, 'r')
    #names = zip_file.namelist()
    #if any(not name.startswith(ZIP_SUBPATH) for name in names):
    #    raise UsageError('Got unexpected member in zip: %s' % (name,))
    #zip_file.extractall(temp_path)
    return temp_subpath
