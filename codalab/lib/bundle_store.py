'''
BundleStore is a data store that stores actual bundle data. Bundles are logical
folders within this data store. This class provides two main methods:
  get_location: return the location of the folder with the given data hash.
  upload: upload a local directory to the store and return its data hash.
'''
import errno
import os
import time
import uuid

from codalab.lib import path_util


class BundleStore(object):
    DATA_SUBDIRECTORY = 'data'
    TEMP_SUBDIRECTORY = 'temp'
    # The amount of time an orphaned folder can live in the data and temp
    # directories before it is garbage collected by full_cleanup.
    DATA_CLEANUP_TIME = 60
    TEMP_CLEANUP_TIME = 60*60

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
        path_util.remove(self.data)
        path_util.remove(self.temp)
        self.make_directories()

    def make_directories(self):
        '''
        Create the data, and temp directories for this BundleStore.
        '''
        for path in (self.data, self.temp):
            path_util.make_directory(path)

    def get_location(self, data_hash, relative=False):
        '''
        Returns the on-disk location of the bundle with the given data hash.
        '''
        if relative:
            return data_hash
        return os.path.join(self.data, data_hash)

    def get_temp_location(self, identifier):
        '''
        Returns the on-disk location of the temporary bundle directory.
        '''
        return os.path.join(self.temp, identifier)

    def make_temp_location(self, identifier):
        '''
        Creates directory with given name under TEMP_SUBDIRECTORY
        '''
        path_util.make_directory(self.get_temp_location(identifier));


    def upload(self, path, allow_symlinks=False):
        '''
        Copy the contents of the directory at path into the data subdirectory,
        in a subfolder named by a hash of the contents of the new data directory.

        Return a (data_hash, metadata) pair, where the metadata is a dict mapping
        keys to precomputed statistics about the new data directory.
        '''
        absolute_path = path_util.normalize(path)
        path_util.check_isvalid(absolute_path, 'upload')
        # Recursively copy the directory into a new BundleStore temp directory.
        temp_directory = uuid.uuid4().hex
        temp_path = os.path.join(self.temp, temp_directory)
        path_util.copy(absolute_path, temp_path)
        # Multiplex between uploading a directory and uploading a file here.
        # All other path_util calls will use these lists of directories and files.
        if os.path.isdir(temp_path):
            dirs_and_files = path_util.recursive_ls(temp_path)
        else:
            dirs_and_files = ([], [temp_path])
        if not allow_symlinks:
            path_util.check_for_symlinks(temp_path, dirs_and_files)
        path_util.set_permissions(temp_path, 0o755, dirs_and_files)
        # Hash the contents of the temporary directory, and then if there is no
        # data with this hash value, move this directory into the data directory.
        data_hash = '0x%s' % (path_util.hash_directory(temp_path, dirs_and_files),)
        data_size = path_util.get_size(temp_path, dirs_and_files)
        final_path = os.path.join(self.data, data_hash)
        final_path_exists = False
        try:
            os.utime(final_path, None)
            final_path_exists = True
        except OSError, e:
            if e.errno == errno.ENOENT:
                os.rename(temp_path, final_path)
            else:
                raise
        if final_path_exists:
            path_util.remove(temp_path)
        # After this operation there should always be a directory at the final path.
        assert(os.path.exists(final_path)), 'Uploaded to %s failed!' % (final_path,)
        return (data_hash, {'data_size': data_size})

    def cleanup(self, model, data_hash):
        '''
        If the given data hash is not needed for any bundle, delete its data.
        '''
        bundles = model.batch_get_bundles(data_hash=data_hash)
        if not bundles:
            absolute_path = self.get_location(data_hash)
            path_util.remove(absolute_path)

    def full_cleanup(self, model):
        '''
        For each data hash in the store, check if it should be garbage collected and
        delete its data if so. In addition, delete any old temporary files.
        '''
        old_data_files = self.list_old_files(self.data, self.DATA_CLEANUP_TIME)
        for data_hash in old_data_files:
            self.cleanup(model, data_hash)
        old_temp_files = self.list_old_files(self.temp, self.TEMP_CLEANUP_TIME)
        for temp_file in old_temp_files:
            temp_path = os.path.join(self.temp, temp_file)
            path_util.remove(temp_path)

    def list_old_files(self, path, cleanup_time):
        cleanup_cutoff = time.time() - cleanup_time
        result = []
        for file in os.listdir(path):
            absolute_path = os.path.join(path, file)
            if path_util.getmtime(absolute_path) < cleanup_cutoff:
                result.append(file)
        return result
