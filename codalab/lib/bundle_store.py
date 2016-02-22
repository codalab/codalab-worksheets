import os
import time
import sys

from hash_ring import HashRing

from codalab.lib import path_util, file_util, print_util, zip_util

def require_disks(f):
    """Decorator added to MulitDiskBundleStore methods that require a disk to
    be added to the deployment for tasks to succeed. Prints a helpful error
    message prompting the user to add a new disk.
    """
    def wrapper(*args, **kwargs):
        self = args[0]
        if self.ring.get_node("DOESN'T MATTER") is None:
            print >> sys.stderr,"""
Error: No disks available.
To use MultiDiskBundleStore, you must add at least one disk. Try the following:

    $ cl help add-disk
"""
            sys.exit(1)
        else:
            return f(*args, **kwargs)
    return wrapper


class BundleStoreCleanupMixin(object):
    """A mixin for BundleStores that wish to support a cleanup and full cleanup operation.
    """
    def cleanup(self, model, data_hash, except_bundle_uuids, dry_run):
        """
        Cleanup a given model
        """
        pass

    def full_cleanup(self, model, dry_run):
        """
        For each data hash in the store, check if it should be garbage collected and
        delete its data if so. In addition, delete any old temporary files.
        """
        pass


class BaseBundleStore(object):
    """
    BaseBundleStore defines the basic interface that all subclasses are *required* to implement. Concrete subtypes of
    this class my introduce new functionality, but they must all support at least these interfaces.
    """

    def __init__(self):
        """
        Create and initialize a new instance of the bundle store.
        """
        self.initialize_store()

    def initialize_store(self):
        """
        Initialize the bundle store with whatever structure is needed for use.
        """
        pass

    def upload(self, sources, follow_symlinks, exclude_patterns, git, unpack, remove_sources):
        """
        Allow the client to create a bundle via uploading a file, directory, or Git repository.
        """
        pass

    def get_bundle_location(self, data_hash):
        """
        Gets the location of the bundle with cryptographic hash digest data_hash. Returns the location in the method
        that makes the most sense for the storage mechanism being used.
        """
        pass

    def reset(self):
        """
        Clears the bundle store, resetting it to an empty state.
        """
        pass


class SingleDiskBundleStore(BaseBundleStore, BundleStoreCleanupMixin):
    """
    DEPRECATED: This is a legacy bundle store, it is being phased out in favor of the MultiDiskBundleStore.
    """
    DATA_SUBDIRECTORY = 'bundles'
    TEMP_SUBDIRECTORY = 'temp'

    # The amount of time a folder can live in the data and temp
    # directories before it is garbage collected by full_cleanup.
    # Note: this is not used right now since we clear out the bundle store
    # immediately.
    DATA_CLEANUP_TIME = 60
    TEMP_CLEANUP_TIME = 60 * 60

    def __init__(self, codalab_home):
        """
        codalab_home: data/ is where all the bundles are actually stored, temp/ is temporary
        """
        self.codalab_home = path_util.normalize(codalab_home)
        self.data = os.path.join(self.codalab_home, self.DATA_SUBDIRECTORY)
        self.temp = os.path.join(self.codalab_home, self.TEMP_SUBDIRECTORY)
        super(SingleDiskBundleStore, self).__init__()

    def initialize_store(self):
        """
        Initialize the singe-disk bundle store in a specific location. For the single-disk case,
        this is simply creating the directories if they weren't there already.
        """
        self.__make_directories()

    def reset(self):
        """
        Delete all stored bundles and then recreate the root directories.
        """
        # Do not run this function in production!
        path_util.remove(self.data)
        path_util.remove(self.temp)
        self.initialize_store()

    def __make_directories(self):
        """
        Create the data, and temp directories for this BundleStore.
        """
        for path in (self.data, self.temp):
            path_util.make_directory(path)

    def get_bundle_location(self, data_hash):
        """
        Returns the on-disk location of the bundle with the given data hash.
        """
        return os.path.join(self.data, data_hash)

    def upload(self, sources, follow_symlinks, exclude_patterns, git, unpack, remove_sources, uuid):
        """
        |sources|: specifies the locations of the contents to upload.  Each element is either a URL or a local path.
        |follow_symlinks|: for local path(s), whether to follow (resolve) symlinks
        |exclude_patterns|: for local path(s), don't upload these patterns (e.g., *.o)
        |git|: for URL, whether |source| is a git repo to clone.
        |unpack|: for each source in |sources|, whether to unpack it if it's an archive.
        |remove_sources|: remove |sources|.

        If |sources| contains one source, then the bundle contents will be that source.
        Otherwise, the bundle contents will be a directory with each of the sources.
        Exceptions:
        - If |git|, then each source is replaced with the result of running 'git clone |source|'
        - If |unpack| is True or a source is an archive (zip, tar.gz, etc.), then unpack the source.

        Install the contents of the directory at |source| into
        DATA_SUBDIRECTORY in a subdirectory named by a hash of the contents.

        Return a (data_hash, metadata) pair, where the metadata is a dict mapping
        keys to precomputed statistics about the new data directory.
        """
        to_delete = []

        # If just a single file, set the final path to be equal to that file
        single_path = len(sources) == 1

        final_path = os.path.join(self.data, uuid)
        if os.path.exists(final_path):
            raise UsageError('Path %s already present in bundle store' % final_path)
        # Only make if not there
        elif not single_path:
            path_util.make_directory(final_path)

        # Paths to resources
        subpaths = []

        for source in sources:
            # Where to save |source| to (might change this value if we unpack).
            if not single_path:
                subpath = os.path.join(final_path, os.path.basename(source))
            else:
                subpath = final_path

            if remove_sources:
                to_delete.append(source)
            source_unpack = unpack and zip_util.path_is_archive(source)

            if source_unpack:
                # Load the file into the bundle store under the given path
                subpath += zip_util.get_archive_ext(source)

            if path_util.path_is_url(source):
                # Download the URL.
                print_util.open_line('BundleStore.upload: downloading %s to %s' % (source, subpath))
                if git:
                    file_util.git_clone(source, subpath)
                else:
                    file_util.download_url(source, subpath, print_status=True)
                    if source_unpack:
                        zip_util.unpack(subpath, zip_util.strip_archive_ext(subpath))
                        path_util.remove(subpath)
                        subpath = zip_util.strip_archive_ext(subpath)
                print_util.clear_line()
            else:
                # Copy the local path.
                source_path = path_util.normalize(source)
                path_util.check_isvalid(source_path, 'upload')

                # Recursively copy the directory into the BundleStore
                print_util.open_line('BundleStore.upload: %s => %s' % (source_path, subpath))
                if source_unpack:
                    zip_util.unpack(source_path, zip_util.strip_archive_ext(subpath))
                    subpath = zip_util.strip_archive_ext(subpath)
                else:
                    if remove_sources:
                        path_util.rename(source_path, subpath)
                    else:
                        path_util.copy(source_path, subpath, follow_symlinks=follow_symlinks, exclude_patterns=exclude_patterns)
                print_util.clear_line()

            subpaths.append(subpath)

        dirs_and_files = None
        if os.path.isdir(final_path):
            dirs_and_files = path_util.recursive_ls(final_path)
        else:
            dirs_and_files = [], [final_path]

        # Hash the contents of the bundle directory. Update the data_hash attribute
        # for the bundle
        print_util.open_line('BundleStore.upload: hashing %s' % final_path)
        data_hash = '0x%s' % (path_util.hash_directory(final_path, dirs_and_files))
        print_util.clear_line()
        print_util.open_line('BundleStore.upload: computing size of %s' % final_path)
        data_size = path_util.get_size(final_path, dirs_and_files)
        print_util.clear_line()

        # Delete paths.
        for path in to_delete:
            if os.path.exists(path):
                path_util.remove(path)

        # After this operation there should always be a directory at the final path.
        assert (os.path.lexists(final_path)), 'Uploaded to %s failed!' % (final_path,)
        return (data_hash, {'data_size': data_size})


    def cleanup(self, model, data_hash, except_bundle_uuids, dry_run):
        """
        If the given data hash is not needed by any bundle (not in
        except_bundle_uuids), delete the data.
        """
        bundles = model.batch_get_bundles(data_hash=data_hash)
        if all(bundle.uuid in except_bundle_uuids for bundle in bundles):
            absolute_path = self.get_bundle_location(data_hash)
            print >> sys.stderr, "cleanup: data %s" % absolute_path
            if not dry_run:
                path_util.remove(absolute_path)

    def full_cleanup(self, model, dry_run):
        """
        For each data hash in the store, check if it should be garbage collected and
        delete its data if so. In addition, delete any old temporary files.
        """
        old_data_files = self.list_old_files(self.data, self.DATA_CLEANUP_TIME)
        for data_hash in old_data_files:
            self.cleanup(model, data_hash, [], dry_run)
        old_temp_files = self.list_old_files(self.temp, self.TEMP_CLEANUP_TIME)
        for temp_file in old_temp_files:
            temp_path = os.path.join(self.temp, temp_file)
            print >> sys.stderr, "cleanup: temp %s" % temp_path
            if not dry_run:
                path_util.remove(temp_path)

    def list_old_files(self, path, cleanup_time):
        """
        Returns a list of old files that have not been modified since `cleanup_time` seconds ago.
        """
        cleanup_cutoff = time.time() - cleanup_time
        result = []
        for entry in os.listdir(path):
            absolute_path = os.path.join(path, entry)
            if path_util.getmtime(absolute_path) < cleanup_cutoff:
                result.append(entry)
        return result

class MultiDiskBundleStore(BaseBundleStore, BundleStoreCleanupMixin):
    """
    A MultiDiskBundleStore is responsible for taking a set of locations and load-balancing the placement of
    bundle data between the locations. It accomplishes this goal using a consistent hash ring, a technique
    discovered by Karger et al. in 1997.
    """

    # Location where MultiDiskBundleStore data and temp data is kept relative to CODALAB_HOME
    DATA_SUBDIRECTORY = 'mbundles'
    TEMP_SUBDIRECTORY = 'mtemp'
    MISC_TEMP_SUBDIRECTORY = 'mtemp'

    def __init__(self, codalab_home):
        self.codalab_home = path_util.normalize(codalab_home)

        self.mdisk = os.path.join(self.codalab_home, 'mdisk')
        self.mtemp = os.path.join(self.codalab_home, MultiDiskBundleStore.MISC_TEMP_SUBDIRECTORY)

        # Perform initialization first to ensure that directories will be populated
        super(MultiDiskBundleStore, self).__init__()
        nodes, _ = path_util.ls(self.mdisk)

        self.ring = HashRing(nodes)
        super(MultiDiskBundleStore, self).__init__()

    def get_bundle_location(self, uuid):
        """
        get_bundle_location: Perform a lookup in the hash ring to determine which disk the bundle is stored on.
        """
        disk = self.ring.get_node(uuid)
        return os.path.join(self.mdisk, disk, MultiDiskBundleStore.DATA_SUBDIRECTORY, uuid)

    @require_disks
    def upload(self, sources, follow_symlinks, exclude_patterns, git, unpack, remove_sources, uuid):
        """
        |sources|: specifies the locations of the contents to upload.  Each element is either a URL or a local path.
        |follow_symlinks|: for local path(s), whether to follow (resolve) symlinks
        |exclude_patterns|: for local path(s), don't upload these patterns (e.g., *.o)
        |git|: for URL, whether |source| is a git repo to clone.
        |unpack|: for each source in |sources|, whether to unpack it if it's an archive.
        |remove_sources|: remove |sources|.

        If |sources| contains one source, then the bundle contents will be that source.
        Otherwise, the bundle contents will be a directory with each of the sources.
        Exceptions:
        - If |git|, then each source is replaced with the result of running 'git clone |source|'
        - If |unpack| is True or a source is an archive (zip, tar.gz, etc.), then unpack the source.

        Install the contents of the directory at |source| into
        DATA_SUBDIRECTORY in a subdirectory named by a hash of the contents.

        Return a (data_hash, metadata) pair, where the metadata is a dict mapping
        keys to precomputed statistics about the new data directory.
        """
        to_delete = []

        # If just a single file, set the final path to be equal to that file
        single_path = len(sources) == 1

        # Determine which disk this will go on
        disk_choice = self.ring.get_node(uuid)

        final_path = os.path.join(self.mdisk, disk_choice, self.DATA_SUBDIRECTORY, uuid)
        if os.path.exists(final_path):
            raise UsageError('Path %s already present in bundle store' % final_path)
        # Only make if not there
        elif not single_path:
            path_util.make_directory(final_path)

        # Paths to resources
        subpaths = []

        for source in sources:
            # Where to save |source| to (might change this value if we unpack).
            if not single_path:
                subpath = os.path.join(final_path, os.path.basename(source))
            else:
                subpath = final_path

            if remove_sources:
                to_delete.append(source)
            source_unpack = unpack and zip_util.path_is_archive(source)

            if source_unpack:
                # Load the file into the bundle store under the given path
                subpath += zip_util.get_archive_ext(source)

            if path_util.path_is_url(source):
                # Download the URL.
                print_util.open_line('BundleStore.upload: downloading %s to %s' % (source, subpath))
                if git:
                    file_util.git_clone(source, subpath)
                else:
                    file_util.download_url(source, subpath, print_status=True)
                    if source_unpack:
                        zip_util.unpack(subpath, zip_util.strip_archive_ext(subpath))
                        path_util.remove(subpath)
                        subpath = zip_util.strip_archive_ext(subpath)
                print_util.clear_line()
            else:
                # Copy the local path.
                source_path = path_util.normalize(source)
                path_util.check_isvalid(source_path, 'upload')

                # Recursively copy the directory into the BundleStore
                print_util.open_line('BundleStore.upload: %s => %s' % (source_path, subpath))
                if source_unpack:
                    zip_util.unpack(source_path, zip_util.strip_archive_ext(subpath))
                    subpath = zip_util.strip_archive_ext(subpath)
                else:
                    if remove_sources:
                        path_util.rename(source_path, subpath)
                    else:
                        path_util.copy(source_path, subpath, follow_symlinks=follow_symlinks, exclude_patterns=exclude_patterns)
                print_util.clear_line()

            subpaths.append(subpath)

        dirs_and_files = None
        if os.path.isdir(final_path):
            dirs_and_files = path_util.recursive_ls(final_path)
        else:
            dirs_and_files = [], [final_path]

        # Hash the contents of the bundle directory. Update the data_hash attribute
        # for the bundle
        print_util.open_line('BundleStore.upload: hashing %s' % final_path)
        data_hash = '0x%s' % (path_util.hash_directory(final_path, dirs_and_files))
        print_util.clear_line()
        print_util.open_line('BundleStore.upload: computing size of %s' % final_path)
        data_size = path_util.get_size(final_path, dirs_and_files)
        print_util.clear_line()

        # Delete paths.
        for path in to_delete:
            if os.path.exists(path):
                path_util.remove(path)

        # After this operation there should always be a directory at the final path.
        assert (os.path.lexists(final_path)), 'Uploaded to %s failed!' % (final_path,)
        return (data_hash, {'data_size': data_size})

    def initialize_store(self):
        """
        Initializes the multi-disk bundle store.
        """
        path_util.make_directory(self.mdisk)
        path_util.make_directory(self.mtemp)

    def add_disk(self, target, new_disk_name):
        """
        MultiDiskBundleStore specific method. Add a new disk to the bundle store. The "target" is actually a symlink to
        the target directory, which the user has configured as the mountpoint for some desired disk.

        First, all bundles that are to be relocated onto the new disk are copied to a temp location to be resilient
        against failures. After the copy is performed, the bundles are subsequently moved to the new disk, and finally
        the original copy of the bundles are deleted from their old locations
        """
        new_disk_location = os.path.join(self.mdisk, new_disk_name)

        mtemp = os.path.join(target, MultiDiskBundleStore.TEMP_SUBDIRECTORY)

        try:
            path_util.make_directory(mtemp)
        except:
            print >> sys.stderr, "Could not make directory %s on disk %s, aborting" % (MultiDiskBundleStore.TEMP_SUBDIRECTORY, target)
            sys.exit(1)

        self.ring.add_node(new_disk_name)  # Add the node to the disk locations
        delete_on_success = []  # Paths to bundles that will be deleted after the copy finishes successfully

        print >> sys.stderr, "Marking bundles for placement on new disk %s (might take a while)" % new_disk_name
        # For each bundle in the bundle store, check to see if any hash to the new disk. If so move them over
        disks, _ = path_util.ls(self.mdisk)
        for disk in disks:
            disk_abs_path = os.path.join(self.mdisk, disk, MultiDiskBundleStore.DATA_SUBDIRECTORY)
            bundles = reduce(lambda dirs, files: dirs + files, path_util.ls(disk_abs_path))
            for bundle in bundles:
                correct_disk = self.ring.get_node(bundle)
                if correct_disk != disk:
                    # Reposition the node to the correct disk
                    print >> sys.stderr, "Marking %s for relocation" % bundle
                    from_path = os.path.join(self.mdisk, disk, MultiDiskBundleStore.DATA_SUBDIRECTORY, bundle)
                    to_path = os.path.join(mtemp, bundle)
                    path_util.copy(from_path, to_path)
                    delete_on_success += [from_path]

        print >> sys.stderr, "Adding new disk as %s..." % new_disk_location
        path_util.soft_link(target, new_disk_location)

        # Atomically move the temp location to the new disk's mdata
        new_mdata = os.path.join(new_disk_location, MultiDiskBundleStore.DATA_SUBDIRECTORY)
        new_mtemp = os.path.join(new_disk_location, MultiDiskBundleStore.TEMP_SUBDIRECTORY)
        path_util.rename(new_mtemp, new_mdata)
        path_util.make_directory(new_mtemp)

        # Go through and purge all of the originals at this time
        print >> sys.stderr, "Cleaning up drives..."
        for to_delete in delete_on_success:
            path_util.remove(to_delete)

        print >> sys.stderr, "Successfully added disk '%s' to the pool." % new_disk_name

    def reset(self):
        """
        Delete all stored bundles and then recreate the root directories.
        """
        # Do not run this function in production!
        path_util.remove(self.mdisk)
        self.initialize_store()

    def __get_num_disks(self):
        """
        Returns the current number of disks being used by this MultiDiskBundleStore.
        This is calculated as the number of directories in self.mdisk
        """
        return reduce(lambda dirs, _: len(dirs), path_util.ls(self.mdisk))


    @require_disks
    def rm_disk(self, disk):
        """
        Deletes the given disk from the bundle store, and if it is not the last disk, it redistributes the bundles
        from that disk across the remaining disks.
        """
        # Transfer all of the files to their correct locations.

        if self.__get_num_disks() == 1:
            """
            Prevent foot-shooting
            """
            print >> sys.stderr, "Error, cannot remove last disk. If you really wish to delete CodaLab, please run the following command:"
            print >> sys.stderr, "      rm -rf %s" % self.codalab_home
            return

        relocations = dict()
        disk_abs_path = os.path.join(self.mdisk, disk)
        old_mdata = os.path.join(disk_abs_path, MultiDiskBundleStore.DATA_SUBDIRECTORY)
        old_mtemp = os.path.join(disk_abs_path, MultiDiskBundleStore.TEMP_SUBDIRECTORY)

        try:
            print disk_abs_path
            path_util.check_isvalid(disk_abs_path, 'rm-disk')
        except:
            print >> sys.stderr, "Disk with name '%s' does not exist. Run `cl ls-disk` to see a list of mounted disks." % disk
            sys.exit(1)

        # Reset the ring to distribute across remaining disks
        self.ring.remove_node(disk)
        bundles_to_move = reduce(lambda dirs, files: dirs + files, path_util.ls(old_mdata))

        for bundle in bundles_to_move:
            new_disk = self.ring.get_node(bundle)
            relocations[bundle] = os.path.join(self.mdisk, new_disk)

        # Copy all bundles off of the old disk to temp directories on the new disks
        for bundle, disk in relocations.iteritems():
            # temporary directory on the disk
            temp_dir = os.path.join(disk, MultiDiskBundleStore.TEMP_SUBDIRECTORY)
            from_path = os.path.join(old_mdata, bundle)
            to_path = os.path.join(temp_dir, 'stage-%s' % bundle)
            path_util.copy(from_path, to_path)

        # Now that each bundle is on the proper disk, move each from the staging area to the
        # production mdata/ subdirectory on its disk.
        for bundle, disk in relocations.iteritems():
            temp_dir = os.path.join(disk, MultiDiskBundleStore.TEMP_SUBDIRECTORY)
            from_path = os.path.join(temp_dir, 'stage-%s' % bundle)
            to_path = os.path.join(disk, MultiDiskBundleStore.DATA_SUBDIRECTORY, bundle)
            path_util.rename(from_path, to_path)

        # Remove data from disk and unlink from CodaLab
        print >> sys.stderr, "Cleaning bundles off of disk..."
        path_util.remove(old_mdata)
        path_util.remove(old_mtemp)
        print >> sys.stderr, "Unlinking disk %s from CodaLab deployment..." % disk
        path_util.remove(disk_abs_path)
        print >> sys.stderr, "Disk removed successfully from bundle store pool"

    def ls_disk(self):
        """List all disks available for storing bundles and how many bundles are currently stored."""
        disks, _ = path_util.ls(self.mdisk)
        print '%d %s' % (len(disks), 'disk' if len(disks) == 1 else 'disks')
        for d in disks:
            disk_path = os.path.join(self.mdisk, d)
            real_path = os.readlink(disk_path)
            bundles = reduce(lambda x,y: x+y, path_util.ls(os.path.join(disk_path, MultiDiskBundleStore.DATA_SUBDIRECTORY)))
            print '- %-016s\n\tmountpoint: %s\n\t%d %s' % (d, real_path, len(bundles), 'bundle' if len(bundles) == 1 else 'bundles')
