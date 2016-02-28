import os
import time
import sys

from .hash_ring import HashRing

from codalab.lib import path_util, file_util, print_util, zip_util
from codalab.common import UsageError

def require_partitions(f):
    """Decorator added to MulitDiskBundleStore methods that require a disk to
    be added to the deployment for tasks to succeed. Prints a helpful error
    message prompting the user to add a new disk.
    """
    def wrapper(*args, **kwargs):
        self = args[0]
        if self.ring.get_node("DOESN'T MATTER") is None:
            print >> sys.stderr,"""
Error: No partitions available.
To use MultiDiskBundleStore, you must add at least one partition. Try the following:

    $ cl help bs-add-partition
"""
            sys.exit(1)
        else:
            return f(*args, **kwargs)
    return wrapper


class BundleStoreCleanupMixin(object):
    """A mixin for BundleStores that wish to support a cleanup operation
    """
    def cleanup(self, uuid, dry_run):
        """
        Cleanup a given bundle. If dry_run is True, do not actually
        delete the bundle from storage.
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


class MultiDiskBundleStore(BaseBundleStore, BundleStoreCleanupMixin):
    """
    A MultiDiskBundleStore is responsible for taking a set of locations and load-balancing the placement of
    bundle data between the locations. It accomplishes this goal using a consistent hash ring, a technique
    discovered by Karger et al. in 1997.
    """

    # Location where MultiDiskBundleStore data and temp data is kept relative to CODALAB_HOME
    DATA_SUBDIRECTORY = 'bundles'
    TEMP_SUBDIRECTORY = 'temp'
    MISC_TEMP_SUBDIRECTORY = 'misc_temp' # BundleServer writes out to here, so should have a different name

    def __init__(self, codalab_home):
        self.codalab_home = path_util.normalize(codalab_home)

        self.partitions = os.path.join(self.codalab_home, 'partitions')
        self.mtemp = os.path.join(self.codalab_home, MultiDiskBundleStore.MISC_TEMP_SUBDIRECTORY)

        # Perform initialization first to ensure that directories will be populated
        super(MultiDiskBundleStore, self).__init__()
        nodes, _ = path_util.ls(self.partitions)

        self.ring = HashRing(nodes)
        super(MultiDiskBundleStore, self).__init__()

    @require_partitions
    def get_bundle_location(self, uuid):
        """
        get_bundle_location: Perform a lookup in the hash ring to determine which disk the bundle is stored on.
        """
        disk = self.ring.get_node(uuid)
        return os.path.join(self.partitions, disk, MultiDiskBundleStore.DATA_SUBDIRECTORY, uuid)

    @require_partitions
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

        final_path = os.path.join(self.partitions, disk_choice, self.DATA_SUBDIRECTORY, uuid)
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

            if source_unpack and single_path:
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
        path_util.make_directory(self.partitions)
        path_util.make_directory(self.mtemp)

        # Create the default partition, if there are no partitions currently
        if self.__get_num_partitions() == 0:
            # Create a default partition that links to the codalab_home
            path_util.make_directory(os.path.join(self.codalab_home, MultiDiskBundleStore.DATA_SUBDIRECTORY))
            path_util.make_directory(os.path.join(self.codalab_home, MultiDiskBundleStore.TEMP_SUBDIRECTORY))
            default_partition = os.path.join(self.partitions, 'default')
            path_util.soft_link(self.codalab_home, default_partition)

    def add_partition(self, target, new_partition_name):
        """
        MultiDiskBundleStore specific method. Add a new partition to the bundle store. The "target" is actually a symlink to
        the target directory, which the user has configured as the mountpoint for some desired partition.

        First, all bundles that are to be relocated onto the new partition are copied to a temp location to be resilient
        against failures. After the copy is performed, the bundles are subsequently moved to the new partition, and finally
        the original copy of the bundles are deleted from their old locations
        """
        target = os.path.abspath(target)
        new_partition_location = os.path.join(self.partitions, new_partition_name)

        mtemp = os.path.join(target, MultiDiskBundleStore.TEMP_SUBDIRECTORY)

        try:
            path_util.make_directory(mtemp)
        except:
            print >> sys.stderr, "Could not make directory %s on partition %s, aborting" % (mtemp, target)
            sys.exit(1)

        self.ring.add_node(new_partition_name)  # Add the node to the partition locations
        delete_on_success = []  # Paths to bundles that will be deleted after the copy finishes successfully

        print >> sys.stderr, "Marking bundles for placement on new partition %s (might take a while)" % new_partition_name
        # For each bundle in the bundle store, check to see if any hash to the new partition. If so move them over
        partitions, _ = path_util.ls(self.partitions)
        for partition in partitions:
            partition_abs_path = os.path.join(self.partitions, partition, MultiDiskBundleStore.DATA_SUBDIRECTORY)
            bundles = reduce(lambda dirs, files: dirs + files, path_util.ls(partition_abs_path))
            for bundle in bundles:
                correct_partition = self.ring.get_node(bundle)
                if correct_partition != partition:
                    # Reposition the node to the correct partition
                    from_path = os.path.join(self.partitions, partition, MultiDiskBundleStore.DATA_SUBDIRECTORY, bundle)
                    to_path = os.path.join(mtemp, bundle)
                    print >> sys.stderr, "copying %s to %s" % (from_path, to_path)
                    path_util.copy(from_path, to_path)
                    delete_on_success += [from_path]

        print >> sys.stderr, "Adding new partition as %s..." % new_partition_location
        path_util.soft_link(target, new_partition_location)

        # Atomically move the temp location to the new partition's mdata
        new_mdata = os.path.join(new_partition_location, MultiDiskBundleStore.DATA_SUBDIRECTORY)
        new_mtemp = os.path.join(new_partition_location, MultiDiskBundleStore.TEMP_SUBDIRECTORY)
        path_util.rename(new_mtemp, new_mdata)
        path_util.make_directory(new_mtemp)

        # Go through and purge all of the originals at this time
        print >> sys.stderr, "Cleaning up drives..."
        for to_delete in delete_on_success:
            path_util.remove(to_delete)

        print >> sys.stderr, "Successfully added partition '%s' to the pool." % new_partition_name

    def reset(self):
        """
        Delete all stored bundles and then recreate the root directories.
        """
        # Do not run this function in production!
        path_util.remove(self.partitions)
        self.initialize_store()

    def __get_num_partitions(self):
        """
        Returns the current number of disks being used by this MultiDiskBundleStore.
        This is calculated as the number of directories in self.partitions
        """
        return reduce(lambda dirs, _: len(dirs), path_util.ls(self.partitions))


    @require_partitions
    def rm_partition(self, partition):
        """
        Deletes the given disk from the bundle store, and if it is not the last partition, it redistributes the bundles
        from that partition across the remaining partitions.
        """
        # Transfer all of the files to their correct locations.

        if self.__get_num_partitions() == 1:
            """
            Prevent foot-shooting
            """
            print >> sys.stderr, "Error, cannot remove last partition. If you really wish to delete CodaLab, please run the following command:"
            print >> sys.stderr, "      rm -rf %s" % self.codalab_home
            return

        relocations = dict()
        partition_abs_path = os.path.join(self.partitions, partition)
        old_mdata = os.path.join(partition_abs_path, MultiDiskBundleStore.DATA_SUBDIRECTORY)
        old_mtemp = os.path.join(partition_abs_path, MultiDiskBundleStore.TEMP_SUBDIRECTORY)

        try:
            print partition_abs_path
            path_util.check_isvalid(partition_abs_path, 'rm-partition')
        except:
            print >> sys.stderr, "Partition with name '%s' does not exist. Run `cl ls-partitions` to see a list of mounted partitions." % partition
            sys.exit(1)

        # Reset the ring to distribute across remaining partitions
        self.ring.remove_node(partition)
        bundles_to_move = reduce(lambda dirs, files: dirs + files, path_util.ls(old_mdata))

        for bundle in bundles_to_move:
            new_partition = self.ring.get_node(bundle)
            relocations[bundle] = os.path.join(self.partitions, new_partition)

        # Copy all bundles off of the old partition to temp directories on the new partition 
        for bundle, partition in relocations.iteritems():
            # temporary directory on the partition
            temp_dir = os.path.join(partition, MultiDiskBundleStore.TEMP_SUBDIRECTORY)
            from_path = os.path.join(old_mdata, bundle)
            to_path = os.path.join(temp_dir, 'stage-%s' % bundle)
            path_util.copy(from_path, to_path)

        # Now that each bundle is on the proper partition, move each from the staging area to the
        # production mdata/ subdirectory on its partition.
        for bundle, partition in relocations.iteritems():
            temp_dir = os.path.join(partition, MultiDiskBundleStore.TEMP_SUBDIRECTORY)
            from_path = os.path.join(temp_dir, 'stage-%s' % bundle)
            to_path = os.path.join(partition, MultiDiskBundleStore.DATA_SUBDIRECTORY, bundle)
            path_util.rename(from_path, to_path)

        # Remove data from partition and unlink from CodaLab
        print >> sys.stderr, "Cleaning bundles off of partition..."
        path_util.remove(old_mdata)
        path_util.remove(old_mtemp)
        print >> sys.stderr, "Unlinking partition %s from CodaLab deployment..." % partition
        path_util.remove(partition_abs_path)
        print >> sys.stderr, "Partition removed successfully from bundle store pool"

    def ls_partitions(self):
        """List all partitions available for storing bundles and how many bundles are currently stored."""
        partitions, _ = path_util.ls(self.partitions)
        print '%d %s' % (len(partitions), 'partition' if len(partitions) == 1 else 'partitions')
        for d in partitions:
            partition_path = os.path.join(self.partitions, d)
            real_path = os.readlink(partition_path)
            bundles = reduce(lambda x,y: x+y, path_util.ls(os.path.join(partition_path, MultiDiskBundleStore.DATA_SUBDIRECTORY)))
            print '- %-016s\n\tmountpoint: %s\n\t%d %s' % (d, real_path, len(bundles), 'bundle' if len(bundles) == 1 else 'bundles')

    def cleanup(self, uuid, dry_run):
        '''
        Remove the bundle with given UUID from on-disk storage.
        '''
        absolute_path = self.get_bundle_location(uuid)
        print >>sys.stderr, "cleanup: data %s" % absolute_path
        if not dry_run:
            path_util.remove(absolute_path)

