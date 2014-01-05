'''
zip_util provides helpers that:
  a) zip a directory on the local filesystem and return the zip file
  b) unzip a zip file and extract the zipped directory
'''
# TODO(skishore): Clean up temp file management here and in BundleStore.
import os
import shutil
import tempfile
from zipfile import ZipFile

from codalab.common import UsageError
from codalab.lib import path_util


ZIP_SUBPATH = 'zip_subpath'


def zip_directory(path):
  '''
  Take a path to a directory and return the path to a zip archive containing it.
  '''
  absolute_path = path_util.normalize(path)
  path_util.check_isdir(absolute_path, 'zip_directory')
  # Recursively copy the directory into a temp directory.
  temp_path = tempfile.mkdtemp()
  temp_subpath = os.path.join(temp_path, ZIP_SUBPATH)
  shutil.copytree(absolute_path, temp_subpath, symlinks=False)
  # Zip and then clean up the temporary directory.
  zip_path = shutil.make_archive(
    base_name=temp_path,
    base_dir=ZIP_SUBPATH,
    root_dir=temp_path,
    format='zip',
  )
  shutil.rmtree(temp_path)
  return zip_path


def unzip_directory(zip_path):
  '''
  Take an absolute path to a zip file and return the path to a directory
  containing its unzipped contents.
  '''
  path_util.check_isfile(zip_path, 'unzip_directory')
  temp_path = tempfile.mkdtemp()
  temp_subpath = os.path.join(temp_path, ZIP_SUBPATH)
  zip_file = ZipFile(zip_path, 'r')
  names = zip_file.namelist()
  if any(not name.startswith(ZIP_SUBPATH) for name in names):
    raise UsageError('Got unexpected member in zip: %s' % (name,))
  zip_file.extractall(temp_path)
  return temp_subpath
