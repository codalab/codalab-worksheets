'''
BundleStore is a data store that stores actual bundle data. Bundles are logical
folders within this data store. This class provides two main methods:
  get_location: return the location of the folder with the given data hash.
  upload: upload a local directory to the store and return its data hash.
'''
import errno
import os
import time
import sys
import uuid

from codalab.lib import path_util, file_util
from codalab.common import UsageError

class BundleStore(object):
    DATA_SUBDIRECTORY = 'data'
    TEMP_SUBDIRECTORY = 'temp'
    # The amount of time an orphaned folder can live in the data and temp
    # directories before it is garbage collected by full_cleanup.
    DATA_CLEANUP_TIME = 60
    TEMP_CLEANUP_TIME = 60*60

    def __init__(self, codalab_home, direct_upload_paths):
        '''
        codalab_home: data/ is where all the bundles are actually stored, temp/ is temporary
        direct_upload_paths: we can accept file://... uploads from these paths.
        '''
        self.codalab_home = path_util.normalize(codalab_home)
        self.direct_upload_paths = direct_upload_paths
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


    def upload(self, path, follow_symlinks):
        '''
        Copy the contents of the directory at |path| into the data subdirectory,
        in a subfolder named by a hash of the contents of the new data directory.
        If |path| is in a temporary directory, then we just move it.

        Return a (data_hash, metadata) pair, where the metadata is a dict mapping
        keys to precomputed statistics about the new data directory.
        '''
        # Create temporary directory as a staging area.
        # If |path| is already temporary, then we use that directly
        # (with the understanding that |path| will be moved)
        if not isinstance(path, list) and os.path.realpath(path).startswith(os.path.realpath(self.temp)):
            temp_path = path
        else:
            temp_path = os.path.join(self.temp, uuid.uuid4().hex)

        if not isinstance(path, list) and path_util.path_is_url(path):
            # Have to be careful.  Want to make sure if we're fetching a URL
            # that points to a file, we are allowing this.
            if path.startswith('file://'):
                path_suffix = path[7:]
                if os.path.islink(path_suffix):
                    raise UsageError('Not allowed to upload symlink %s' % path_suffix)
                if not any(path_suffix.startswith(f) for f in self.direct_upload_paths):
                    raise UsageError('Not allowed to upload %s (only %s allowed)' % (path_suffix, self.direct_upload_paths))

            # Download |path| if it is a URL.
            print >>sys.stderr, 'BundleStore.upload: downloading %s to %s' % (path, temp_path)
            file_util.download_url(path, temp_path, print_status=True)
        elif path != temp_path:
            # Copy |path| into the temp_path.
            if isinstance(path, list):
                absolute_path = [path_util.normalize(p) for p in path]
                for p in absolute_path: path_util.check_isvalid(p, 'upload')
            else:
                absolute_path = path_util.normalize(path)
                path_util.check_isvalid(absolute_path, 'upload')

            # Recursively copy the directory into a new BundleStore temp directory.
            print >>sys.stderr, 'BundleStore.upload: copying %s to %s' % (absolute_path, temp_path)
            path_util.copy(absolute_path, temp_path, follow_symlinks=follow_symlinks)

        # Multiplex between uploading a directory and uploading a file here.
        # All other path_util calls will use these lists of directories and files.
        if os.path.isdir(temp_path):
            dirs_and_files = path_util.recursive_ls(temp_path)
        else:
            dirs_and_files = ([], [temp_path])

        # Hash the contents of the temporary directory, and then if there is no
        # data with this hash value, move this directory into the data directory.
        print >>sys.stderr, 'BundleStore.upload: hashing %s' % (temp_path)
        data_hash = '0x%s' % (path_util.hash_directory(temp_path, dirs_and_files),)
        data_size = path_util.get_size(temp_path, dirs_and_files)
        final_path = os.path.join(self.data, data_hash)
        final_path_exists = False
        try:
            # If data_hash already exists, then we don't need to move it over.
            os.utime(final_path, None)
            final_path_exists = True
        except OSError, e:
            if e.errno == errno.ENOENT:
                print 'BundleStore.upload: moving %s to %s' % (temp_path, final_path)
                path_util.rename(temp_path, final_path)
            else:
                raise
        if final_path_exists:
            path_util.remove(temp_path)

        # After this operation there should always be a directory at the final path.
        assert(os.path.exists(final_path)), 'Uploaded to %s failed!' % (final_path,)
        return (data_hash, {'data_size': data_size})

    def cleanup(self, model, data_hash, except_bundle_uuids, dry_run):
        '''
        If the given data hash is not needed by any bundle (not in
        except_bundle_uuids), delete the data.
        '''
        bundles = model.batch_get_bundles(data_hash=data_hash)
        if all(bundle.uuid in except_bundle_uuids for bundle in bundles):
            absolute_path = self.get_location(data_hash)
            print >>sys.stderr, "cleanup: data %s" % absolute_path
            if not dry_run:
                path_util.remove(absolute_path)

    def full_cleanup(self, model, dry_run):
        '''
        For each data hash in the store, check if it should be garbage collected and
        delete its data if so. In addition, delete any old temporary files.
        '''
        old_data_files = self.list_old_files(self.data, self.DATA_CLEANUP_TIME)
        for data_hash in old_data_files:
            self.cleanup(model, data_hash, [], dry_run)
        old_temp_files = self.list_old_files(self.temp, self.TEMP_CLEANUP_TIME)
        for temp_file in old_temp_files:
            temp_path = os.path.join(self.temp, temp_file)
            print >>sys.stderr, "cleanup: temp %s" % temp_path
            if not dry_run:
                path_util.remove(temp_path)

    def list_old_files(self, path, cleanup_time):
        cleanup_cutoff = time.time() - cleanup_time
        result = []
        for file in os.listdir(path):
            absolute_path = os.path.join(path, file)
            if path_util.getmtime(absolute_path) < cleanup_cutoff:
                result.append(file)
        return result
