# TODO(skishore): Add code to clean up the temp directory based on mtimes.
import os
import shutil
import tempfile

from codalab.lib import path_util


class BundleStore(object):
  DATA_SUBDIRECTORY = 'data'
  TEMP_SUBDIRECTORY = 'temp'

  def __init__(self, codalab_home):
    self.codalab_home = path_util.normalize(codalab_home)
    self.data = os.path.join(self.codalab_home, self.DATA_SUBDIRECTORY)
    self.temp = os.path.join(self.codalab_home, self.TEMP_SUBDIRECTORY)
    self.make_directories()

  def _reset(self):
    '''
    Delete all stored bundles and then recreate the root directories.
    '''
    # Do not run this function in production!
    shutil.rmtree(self.data)
    shutil.rmtree(self.temp)
    self.make_directories()

  def make_directories(self):
    '''
    Create the root, data, and temp directories for this BundleStore.
    '''
    for path in (self.codalab_home, self.data, self.temp):
      path_util.make_directory(path)

  def get_location(self, data_hash, relative=False):
    '''
    Returns the on-disk location of the bundle with the given data hash.
    '''
    if relative:
      return data_hash
    return os.path.join(self.data, data_hash)

  def upload(self, path, allow_symlinks=False):
    '''
    Copy the contents of the directory at path into the data subdirectory,
    in a subfolder named by a hash of the contents of the new data directory.

    Return the name of the new subfolder, that is, the data hash.
    '''
    absolute_path = path_util.normalize(path)
    path_util.check_isdir(absolute_path, 'upload')
    # Recursively copy the directory into a new BundleStore temp directory.
    temp_path = tempfile.mkdtemp(dir=self.temp)
    shutil.copytree(absolute_path, temp_path, symlinks=allow_symlinks)
    # Recursively list the directory just once as an optimization.
    dirs_and_files = path_util.recursive_ls(temp_path)
    if not allow_symlinks:
      path_util.check_for_symlinks(temp_path, dirs_and_files)
    path_util.set_permissions(temp_path, 0o755, dirs_and_files)
    # Hash the contents of the temporary directory, and then if there is no
    # data with this hash value, move this directory into the data directory.
    data_hash = path_util.hash_directory(temp_path, dirs_and_files)
    final_path = os.path.join(self.data, data_hash)
    if os.path.exists(final_path):
      shutil.rmtree(temp_path)
    else:
      os.rename(temp_path, final_path)
    # After this operation there should always be a directory at the final path.
    assert(os.path.isdir(final_path)), 'Uploaded to %s failed!' % (final_path,)
    return data_hash
