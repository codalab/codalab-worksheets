import os
import re
import sys
from collections import OrderedDict
from typing import Callable, Any

from codalab.lib import path_util, spec_util
from codalab.worker.bundle_state import State
from functools import reduce


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


class MultiDiskBundleStore(object):
    """
    Responsible for taking a set of locations and load-balancing the placement of
    bundle data between the locations.

    Use case: we store bundles in multiple disks, and they can be distributed in any arbitrary way.
    Due to efficiency reasons, it builds up an LRU cache of bundle locations over time. When retrieving
    a bundle that isn't recorded in the cache, the bundle store performs a linear search over all the locations.
    """

    # Location where MultiDiskBundleStore data and temp data is kept relative to CODALAB_HOME
    DATA_SUBDIRECTORY = 'bundles'
    CACHE_SIZE = 1 * 1000 * 1000  # number of entries to cache

    def __init__(self, codalab_home):
        self.codalab_home = path_util.normalize(codalab_home)

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
    def get_bundle_location(self, uuid):
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
