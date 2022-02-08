import os
import re
import sys
from collections import OrderedDict
from typing import Callable, Any, Tuple
from typing_extensions import TypedDict

from codalab.lib import path_util, spec_util
from codalab.worker.bundle_state import State
from functools import reduce
from codalab.common import StorageType, StorageFormat


def require_partitions(f: Callable[['MultiDiskBundleStore', Any], Any]):
    """Decorator added to MultiDiskBundleStore methods that require a disk to
    be added to the deployment for tasks to succeed. Prints a helpful error
    message prompting the user to add a new disk.
    """

    def wrapper(*args, **kwargs):
        self = args[0]
        if len(self.nodes) < 1:
            print(
                """
Error: No partitions available.
To use MultiDiskBundleStore, you must add at least one partition. Try the following:

    $ cl help bs-add-partition
""",
                file=sys.stderr,
            )
            sys.exit(1)
        else:
            return f(*args, **kwargs)

    return wrapper


class BundleStore(object):
    """
    Base class for a bundle store.
    """

    def __init__(self, bundle_model, codalab_home):
        self._bundle_model = bundle_model
        self.codalab_home = path_util.normalize(codalab_home)

    def get_bundle_location(self, uuid, bundle_store_uuid=None):
        raise NotImplementedError

    def cleanup(self, uuid, dry_run):
        raise NotImplementedError


class _MultiDiskBundleStoreBase(BundleStore):
    """
    A base class that contains logic for only storing bundles in multiple disks.
    This bundle store shouldn't be directly configured in CodaLab --
    instead, the MultiDiskBundleStore class is used by default, which inherits
    from this class and adds more functionality for Blob Storage, etc.

    This class is responsible for taking a set of locations and load-balancing the placement of
    bundle data between the locations.

    Use case: we store bundles in multiple disks, and they can be distributed in any arbitrary way.
    Due to efficiency reasons, it builds up an LRU cache of bundle locations over time. When retrieving
    a bundle that isn't recorded in the cache, the bundle store performs a linear search over all the locations.
    """

    # Location where MultiDiskBundleStore data and temp data is kept relative to CODALAB_HOME
    DATA_SUBDIRECTORY = 'bundles'
    CACHE_SIZE = 1 * 1000 * 1000  # number of entries to cache

    def __init__(self, bundle_model, codalab_home):
        BundleStore.__init__(self, bundle_model, codalab_home)

        self.partitions = os.path.join(self.codalab_home, 'partitions')
        path_util.make_directory(self.partitions)

        self.refresh_partitions()
        if self.__get_num_partitions() == 0:  # Ensure at least one partition exists.
            self.add_partition(None, 'default')

        self.lru_cache = OrderedDict()

    def refresh_partitions(self):
        nodes, _ = path_util.ls(self.partitions)
        self.nodes = nodes

    def get_node_avail(self, node):
        # get absolute free space
        st = os.statvfs(node)
        free = st.f_bavail * st.f_frsize
        return free

    @require_partitions
    def get_bundle_location(self, uuid, bundle_store_uuid=None):
        """
        get_bundle_location: look for bundle in the cache, or if not in cache, go through every partition.
        If not in any partition, return disk with largest free space.
        """
        if uuid in self.lru_cache:
            disk = self.lru_cache.pop(uuid)
        else:
            disk = None
            for n in self.nodes:  # go through every partition
                bundle_path = os.path.join(
                    self.partitions, n, MultiDiskBundleStore.DATA_SUBDIRECTORY, uuid
                )
                if os.path.exists(bundle_path):
                    disk = n
                    break

            if disk is None:
                # return disk with largest free space
                disk = max(
                    self.nodes,
                    key=lambda x: self.get_node_avail(
                        os.path.join(self.partitions, x, MultiDiskBundleStore.DATA_SUBDIRECTORY)
                    ),
                )

        if len(self.lru_cache) >= self.CACHE_SIZE:
            self.lru_cache.popitem(last=False)
        self.lru_cache[uuid] = disk
        return os.path.join(self.partitions, disk, MultiDiskBundleStore.DATA_SUBDIRECTORY, uuid)

    def add_partition(self, target, new_partition_name):
        """
        MultiDiskBundleStore specific method. Add a new partition to the bundle
        store, which is actually a symlink to the target directory, which the
        user has configured as the mountpoint for some desired partition.
        If `target` is None, then make the `new_partition_name` the actual directory.
        """
        if target is not None:
            target = os.path.abspath(target)
        new_partition_location = os.path.join(self.partitions, new_partition_name)

        print("Adding new partition as %s..." % new_partition_location, file=sys.stderr)
        if target is None:
            path_util.make_directory(new_partition_location)
        else:
            path_util.soft_link(target, new_partition_location)

        # Where the bundles are stored
        mdata = os.path.join(new_partition_location, MultiDiskBundleStore.DATA_SUBDIRECTORY)

        try:
            path_util.make_directory(mdata)
        except Exception as e:
            print(e, file=sys.stderr)
            print(
                "Could not make directory %s on partition %s, aborting" % (mdata, target),
                file=sys.stderr,
            )
            sys.exit(1)

        self.refresh_partitions()

        print(
            "Successfully added partition '%s' to the pool." % new_partition_name, file=sys.stderr
        )

    def __get_num_partitions(self):
        """
        Returns the current number of disks being used by this MultiDiskBundleStore.
        This is calculated as the number of directories in self.partitions
        """
        return reduce(lambda dirs, _: len(dirs), path_util.ls(self.partitions))

    @require_partitions
    def rm_partition(self, partition):
        """
        Deletes the given partition entry from the bundle store, and purges the lru cache. Does not move any bundles.
        """

        if self.__get_num_partitions() == 1:
            """
            Prevent foot-shooting
            """
            print(
                "Error, cannot remove last partition. If you really wish to delete CodaLab, please run the following command:",
                file=sys.stderr,
            )
            print("      rm -rf %s" % self.codalab_home, file=sys.stderr)
            return

        partition_abs_path = os.path.join(self.partitions, partition)

        try:
            print(partition_abs_path)
            path_util.check_isvalid(partition_abs_path, 'rm-partition')
        except Exception:
            print(
                "Partition with name '%s' does not exist. Run `cl ls-partitions` to see a list of mounted partitions."
                % partition,
                file=sys.stderr,
            )
            sys.exit(1)

        print("Unlinking partition %s from CodaLab deployment..." % partition, file=sys.stderr)
        path_util.remove(partition_abs_path)
        self.refresh_partitions()
        print("Partition removed successfully from bundle store pool", file=sys.stderr)
        print(
            "Warning: this does not affect the bundles in the removed partition or any entries in the bundle database",
            file=sys.stdout,
        )
        self.lru_cache = OrderedDict()

    def ls_partitions(self):
        """List all partitions available for storing bundles and how many bundles are currently stored."""
        partitions, _ = path_util.ls(self.partitions)
        print('%d %s' % (len(partitions), 'partition' if len(partitions) == 1 else 'partitions'))
        for d in partitions:
            partition_path = os.path.join(self.partitions, d)
            real_path = os.readlink(partition_path)
            bundles = reduce(
                lambda x, y: x + y,
                path_util.ls(os.path.join(partition_path, MultiDiskBundleStore.DATA_SUBDIRECTORY)),
            )
            print(
                (
                    '- %-016s\n\tmountpoint: %s\n\t%d %s'
                    % (d, real_path, len(bundles), 'bundle' if len(bundles) == 1 else 'bundles')
                )
            )

    def cleanup(self, uuid, dry_run):
        '''
        Remove the bundle with given UUID from on-disk storage.
        '''
        absolute_path = self.get_bundle_location(uuid)
        print("cleanup: data %s" % absolute_path, file=sys.stderr)
        if not dry_run:
            path_util.remove(absolute_path)

    def health_check(self, model, force=False, compute_data_hash=False, repair_hashes=False):
        """
        MultiDiskBundleStore.health_check(): In the MultiDiskBundleStore, bundle contents are stored on disk, and
        occasionally the disk gets out of sync with the database, in which case we make repairs in the following ways:

            1. Deletes bundles with corresponding UUID not in the database.
            3. Deletes any files not beginning with UUID string.
            4. For each bundle marked READY or FAILED, ensure that its dependencies are not located in the bundle
               directory. If they are then delete the dependencies.
            5. For bundle <UUID> marked READY or FAILED, <UUID>.cid or <UUID>.status, or the <UUID>(-internal).sh files
               should not exist.
        |force|: Perform any destructive operations on the bundle store the health check determines are necessary. False by default
        |compute_data_hash|: If True, compute the data_hash for every single bundle ourselves and see if it's consistent with what's in
                             the database. False by default.
        """
        UUID_REGEX = re.compile(r'^(%s)' % spec_util.UUID_STR)

        def _delete_path(loc):
            cmd = 'rm -r \'%s\'' % loc
            print(cmd)
            if force:
                path_util.remove(loc)

        def _get_uuid(path):
            fname = os.path.basename(path)
            try:
                return UUID_REGEX.match(fname).groups()[0]
            except Exception:
                return None

        def _is_bundle(path):
            """Returns whether the given path is a bundle directory/file"""
            return _get_uuid(path) == os.path.basename(path)

        def _check_bundle_paths(bundle_paths, db_bundle_by_uuid):
            """
            Takes in a list of bundle paths and a mapping of UUID to BundleModel, and returns a list of paths and
            subpaths that need to be removed.
            """
            to_delete = []
            # Batch get information for all bundles stored on-disk

            for bundle_path in bundle_paths:
                uuid = _get_uuid(bundle_path)
                # Screen for bundles stored on disk that are no longer in the database
                bundle = db_bundle_by_uuid.get(uuid, None)
                if bundle is None:
                    to_delete += [bundle_path]
                    continue
                # Delete dependencies stored inside of READY or FAILED bundles
                if bundle.state in [State.READY, State.FAILED]:
                    dep_paths = [
                        os.path.join(bundle_path, dep.child_path) for dep in bundle.dependencies
                    ]
                    to_delete += list(filter(os.path.exists, dep_paths))
            return to_delete

        def _check_other_paths(other_paths, db_bundle_by_uuid):
            """
            Given a list of non-bundle paths, and a mapping of UUID to BundleModel, returns a list of paths to delete.
            """
            to_delete = []
            for path in other_paths:
                uuid = _get_uuid(path)
                bundle = db_bundle_by_uuid.get(uuid, None)
                if bundle is None:
                    to_delete += [path]
                    continue
                ends_with_ext = (
                    path.endswith('.cid') or path.endswith('.status') or path.endswith('.sh')
                )
                if bundle.state in [State.READY, State.FAILED]:
                    if ends_with_ext:
                        to_delete += [path]
                        continue
                    elif '.' in path:
                        print('WARNING: File %s is likely junk.' % path, file=sys.stderr)
            return to_delete

        partitions, _ = path_util.ls(self.partitions)
        trash_count = 0

        for partition in partitions:
            print('Looking for trash in partition %s...' % partition, file=sys.stderr)
            partition_path = os.path.join(
                self.partitions, partition, MultiDiskBundleStore.DATA_SUBDIRECTORY
            )
            entries = list(
                map(
                    lambda f: os.path.join(partition_path, f),
                    reduce(lambda d, f: d + f, path_util.ls(partition_path)),
                )
            )
            bundle_paths = list(filter(_is_bundle, entries))
            other_paths = set(entries) - set(bundle_paths)

            uuids = list(map(_get_uuid, bundle_paths))
            db_bundles = model.batch_get_bundles(uuid=uuids)
            db_bundle_by_uuid = dict()
            for bundle in db_bundles:
                db_bundle_by_uuid[bundle.uuid] = bundle

            # Check both bundles and non-bundles and remove each
            for to_delete in _check_bundle_paths(bundle_paths, db_bundle_by_uuid):
                trash_count += 1
                _delete_path(to_delete)
            for to_delete in _check_other_paths(other_paths, db_bundle_by_uuid):
                trash_count += 1
                _delete_path(to_delete)

            # Check for each bundle if we need to compute its data_hash
            data_hash_recomputed = 0

            print('Checking data_hash of bundles in partition %s...' % partition, file=sys.stderr)
            for bundle_path in bundle_paths:
                uuid = _get_uuid(bundle_path)
                bundle = db_bundle_by_uuid.get(uuid, None)
                if bundle is None:
                    continue
                if compute_data_hash or bundle.data_hash is None:
                    dirs_and_files = (
                        path_util.recursive_ls(bundle_path)
                        if os.path.isdir(bundle_path)
                        else ([], [bundle_path])
                    )
                    data_hash = '0x%s' % path_util.hash_directory(bundle_path, dirs_and_files)
                    if bundle.data_hash is None:
                        data_hash_recomputed += 1
                        print(
                            'Giving bundle %s data_hash %s' % (bundle_path, data_hash),
                            file=sys.stderr,
                        )
                        if force:
                            db_update = dict(data_hash=data_hash)
                            model.update_bundle(bundle, db_update)
                    elif compute_data_hash and data_hash != bundle.data_hash:
                        data_hash_recomputed += 1
                        print(
                            'Bundle %s should have data_hash %s, actual digest is %s'
                            % (bundle_path, bundle.data_hash, data_hash),
                            file=sys.stderr,
                        )
                        if repair_hashes and force:
                            db_update = dict(data_hash=data_hash)
                            model.update_bundle(bundle, db_update)

        if force:
            print('\tDeleted %d objects from the bundle store' % trash_count, file=sys.stderr)
            print('\tRecomputed data_hash for %d bundles' % data_hash_recomputed, file=sys.stderr)
        else:
            print('Dry-Run Statistics, re-run with --force to perform updates:', file=sys.stderr)
            print('\tObjects marked for deletion: %d' % trash_count, file=sys.stderr)
            print(
                '\tBundles that need data_hash recompute: %d' % data_hash_recomputed,
                file=sys.stderr,
            )


BundleLocation = TypedDict(
    'BundleLocation', {"storage_type": str, "storage_format": str,}, total=False,
)


class MultiDiskBundleStore(_MultiDiskBundleStoreBase):
    """
    A multi-disk bundle store that also supports storing bundles in a CodaLab-managed
    Blob Storage container.

    If bundles are indicated to be stored in a custom BundleStore, they are retrieved from
    that bundle store. Otherwise, their storage type is determined by the legacy "storage_type"
    column, which indicates if they are in Blob Storage or from the underlying disk bundle store.

    In Blob Storage, each bundle is stored in the format:
    azfs://{container name}/bundles/{bundle uuid}/contents.tar.gz if a directory,
    azfs://{container name}/bundles/{bundle uuid}/contents.gz if a file.

    In GCS, each bundle is stored in the format:
    gs://{bucket name}/{bundle uuid}/contents.tar.gz if a directory,
    gs://{bucket name}/{bundle uuid}/contents.gz if a file.

    If the bundle is a directory, the entire contents of the bundle is stored in the .tar.gz file;
    otherwise, if the bundle is a single file, the file is stored in the .gz file as an archive
    member with name equal to the bundle uuid and is_dir is set to False in the database.

    See this design doc for more information about Blob Storage design:
    https://docs.google.com/document/d/1l4kOqi9irBjOApmn4E6vlzsjAXDJbetIyVw8gMRHrpU/edit#
    """

    def __init__(self, bundle_model, codalab_home, azure_blob_account_name):
        _MultiDiskBundleStoreBase.__init__(self, bundle_model, codalab_home)

        self._azure_blob_account_name = azure_blob_account_name

    def get_bundle_location_full_info(
        self, uuid, bundle_store_uuid=None
    ) -> Tuple[BundleLocation, str]:
        """
        Get the bundle location.
        Arguments:
            uuid (str): uuid of the bundle.
            bundle_store_uuid (str): uuid of a specific BundleLocation to use when retrieving the bundle's location.
                If unspecified, will pick an optimal location.
        Returns: Tuple (BundleLocation object with location info for the bundle, resolved path to access bundle)
        """
        bundle_locations = self._bundle_model.get_bundle_locations(uuid)
        if bundle_store_uuid:
            assert len(bundle_locations) >= 1
        storage_type, is_dir = self._bundle_model.get_bundle_storage_info(uuid)
        if len(bundle_locations) >= 1:
            # Use the BundleLocations stored with the bundle, along with some
            # precedence rules, to determine where the bundle is stored.
            selected_location = None
            selected_location_priority = 999
            for location in bundle_locations:
                # Highest precedence: bundle_store_uuid specified in this function.
                PRIORITY = 1
                if (
                    location["bundle_store_uuid"] == bundle_store_uuid
                    and PRIORITY < selected_location_priority
                ):
                    selected_location = location
                    selected_location_priority = PRIORITY
                # Next precedence: prefer blob storage over disk storage.
                PRIORITY = 2
                if (
                    location["storage_type"]
                    in (StorageType.AZURE_BLOB_STORAGE.value, StorageType.GCS_STORAGE.value)
                    and PRIORITY < selected_location_priority
                ):
                    selected_location = location
                    selected_location_priority = PRIORITY
                # Last precedence: pick whatever storage is available.
                PRIORITY = 3
                if PRIORITY < selected_location_priority:
                    selected_location = location
                    selected_location_priority = PRIORITY
            assert selected_location is not None

            # Now get the BundleLocation.
            # TODO: refactor this into a class-based system so different storage types can implement this method.
            if selected_location["storage_type"] == StorageType.AZURE_BLOB_STORAGE.value:
                assert (
                    selected_location["storage_format"] == StorageFormat.COMPRESSED_V1.value
                )  # Only supported format on Blob Storage
                file_name = "contents.tar.gz" if is_dir else "contents.gz"
                url = selected_location["url"]  # Format: "azfs://[container name]/bundles"
                assert url.startswith("azfs://")
                return selected_location, f"{url}/{uuid}/{file_name}"
            elif selected_location["storage_type"] == StorageType.GCS_STORAGE.value:
                assert (
                    selected_location["storage_format"] == StorageFormat.COMPRESSED_V1.value
                )  # Only supported format on GCS
                file_name = "contents.tar.gz" if is_dir else "contents.gz"
                url = selected_location["url"]  # Format: "gs://[bucket name]"
                assert url.startswith("gs://")
                return selected_location, f"{url}/{uuid}/{file_name}"
            else:
                assert (
                    selected_location["storage_format"] == StorageFormat.UNCOMPRESSED.value
                )  # Only supported format on disk
                return selected_location, _MultiDiskBundleStoreBase.get_bundle_location(self, uuid)
        # If no BundleLocations are available, use the legacy "storage_type" column to determine where the bundle is stored.
        elif storage_type == StorageType.AZURE_BLOB_STORAGE.value:
            file_name = "contents.tar.gz" if is_dir else "contents.gz"
            return (
                {
                    "storage_type": StorageType.AZURE_BLOB_STORAGE.value,
                    "storage_format": StorageFormat.COMPRESSED_V1.value,
                },
                f"azfs://{self._azure_blob_account_name}/bundles/{uuid}/{file_name}",
            )
        # Otherwise, we're on the default disk storage.
        return (
            {
                "storage_type": StorageType.DISK_STORAGE.value,
                "storage_format": StorageFormat.UNCOMPRESSED.value,
            },
            _MultiDiskBundleStoreBase.get_bundle_location(self, uuid),
        )

    def get_bundle_location(self, uuid, bundle_store_uuid=None):
        """
        Get the path to the specified bundle location.
        Arguments:
            uuid (str): uuid of the bundle.
            bundle_store_uuid (str): uuid of a specific BundleLocation to use when retrieving the bundle's location.
                If unspecified, will pick an optimal location.
        Returns: a string with the path to the bundle.
        """
        _, path = self.get_bundle_location_full_info(uuid, bundle_store_uuid)
        return path
