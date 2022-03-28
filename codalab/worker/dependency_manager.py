import logging
import os
import threading
import traceback
import time
import shutil
import uuid
from collections import namedtuple
from contextlib import closing
from datetime import timedelta
from typing import Dict, Set, Union, List

import codalab.worker.pyjson
from .bundle_service_client import BundleServiceClient
from codalab.lib.formatting import size_str
from codalab.worker.file_util import remove_path
from codalab.worker.un_tar_directory import un_tar_directory
from codalab.worker.fsm import BaseDependencyManager, DependencyStage, StateTransitioner
from codalab.worker.worker_thread import ThreadDict
from codalab.worker.bundle_state import DependencyKey
from codalab.worker.state_committer import JsonStateCommitter

from flufl.lock import Lock, AlreadyLockedError, NotLockedError  # noqa: E402

logging.getLogger('flufl.lock').setLevel(logging.WARNING)


logger = logging.getLogger(__name__)

DependencyState = namedtuple(
    'DependencyState',
    # downloading_by - worker id of which worker is downloading / has downloaded the dependency
    'stage downloading_by dependency_key path size_bytes dependents last_used last_downloading message killed',
)

DependencyManagerState = Dict[str, Union[Dict[DependencyKey, DependencyState], Set[str]]]


class DownloadAbortedException(Exception):
    """
    Exception raised by the download if a download is killed before it is complete
    """

    def __init__(self, message):
        super(DownloadAbortedException, self).__init__(message)


class NFSLock:
    def __init__(self, path):
        # Specify the path to a file that will be used to synchronize the lock.
        # Per the flufl.lock documentation, use a file that does not exist.
        self._lock = Lock(path)

        # Locks have a lifetime (default 15 seconds) which is the period of time that the process expects
        # to keep the lock once it has been acquired. We set the lifetime to be 5 minutes as we expect
        # all operations that require locks to be completed within that time.
        self._lock.lifetime = timedelta(minutes=5)

        # Ensure multiple threads within a process run NFSLock operations one at a time.
        # We must acquire the reentrant lock before acquiring the flufl lock and only release after
        # the flufl lock is released.
        self._r_lock = threading.RLock()

    def acquire(self):
        self._r_lock.acquire()
        try:
            self._lock.lock()
        except AlreadyLockedError:
            # Safe to re-attempt to acquire a lock
            pass

    def release(self):
        try:
            self._lock.unlock()
        except NotLockedError:
            # Safe to re-attempt to release a lock
            pass
        self._r_lock.release()

    def __enter__(self):
        self.acquire()

    def __exit__(self, t, v, tb):
        self.release()

    @property
    def is_locked(self):
        return self._lock.is_locked


class DependencyManager(StateTransitioner, BaseDependencyManager):
    """
    This dependency manager downloads dependency bundles from Codalab server
    to the local filesystem. It caches all downloaded dependencies but cleans up the
    old ones if the disk use hits the given threshold. It's also NFS-safe.
    In this class, dependencies are uniquely identified by DependencyKey.
    """

    DEPENDENCIES_DIR_NAME = 'dependencies'
    DEPENDENCY_FAILURE_COOLDOWN = 10
    # TODO(bkgoksel): The server writes these to the worker_dependencies table, which stores the dependencies
    # json as a SqlAlchemy LargeBinary, which defaults to MySQL BLOB, which has a size limit of
    # 65K. For now we limit this value to about 58K to avoid any issues but we probably want to do
    # something better (either specify MEDIUMBLOB in the SqlAlchemy definition of the table or change
    # the data format of how we store this)
    MAX_SERIALIZED_LEN = 60000

    # If it has been this long since a worker has downloaded anything, another worker will take over downloading.
    DEPENDENCY_DOWNLOAD_TIMEOUT_SECONDS = 5 * 60

    def __init__(
        self,
        commit_file: str,
        bundle_service: BundleServiceClient,
        worker_dir: str,
        max_cache_size_bytes: int,
        download_dependencies_max_retries: int,
    ):
        super(DependencyManager, self).__init__()
        self.add_transition(DependencyStage.DOWNLOADING, self._transition_from_DOWNLOADING)
        self.add_terminal(DependencyStage.READY)
        self.add_terminal(DependencyStage.FAILED)

        self._id: str = "worker-dependency-manager-{}".format(uuid.uuid4().hex[:8])
        self._state_committer = JsonStateCommitter(commit_file)
        self._bundle_service = bundle_service
        self._max_cache_size_bytes = max_cache_size_bytes
        self.dependencies_dir = os.path.join(worker_dir, DependencyManager.DEPENDENCIES_DIR_NAME)
        self._download_dependencies_max_retries = download_dependencies_max_retries
        if not os.path.exists(self.dependencies_dir):
            logger.info('{} doesn\'t exist, creating.'.format(self.dependencies_dir))
            os.makedirs(self.dependencies_dir, 0o770)

        # Create a lock for concurrency over NFS
        # Create a separate locks directory to hold the lock files.
        # Each lock file is created when a process tries to claim the main lock.
        locks_claims_dir: str = os.path.join(worker_dir, 'locks_claims')
        try:
            os.makedirs(locks_claims_dir)
        except FileExistsError:
            logger.info(f"A locks directory at {locks_claims_dir} already exists.")
        self._state_lock = NFSLock(os.path.join(locks_claims_dir, 'state.lock'))

        # File paths that are currently being used to store dependencies. Used to prevent conflicts
        self._paths: Set[str] = set()
        # DependencyKey -> WorkerThread(thread, success, failure_message)
        self._downloading = ThreadDict(fields={'success': False, 'failure_message': None})
        # Sync states between dependency-state.json and dependency directories on the local file system.
        self._sync_state()

        self._stop = False
        self._main_thread = None
        logger.info(f"Initialized Dependency Manager with ID: {self._id}")

    def _sync_state(self):
        """
        Synchronize dependency states between dependencies-state.json and the local file system as follows:
        1. dependencies and paths: populated from dependencies-state.json
        2. directories on the local file system: the bundle contents
        This function forces the 1 and 2 to be in sync by taking the intersection (e.g., deleting bundles from the
        local file system that don't appear in the dependencies-state.json and vice-versa)
        """
        with self._state_lock:
            # Load states from dependencies-state.json, which contains information about bundles (e.g., state,
            # dependencies, last used, etc.).
            if self._state_committer.state_file_exists:
                # If the state file exists, do not pass in a default. It's critical that we read the contents
                # of the state file, as this method prunes dependencies. If we can't read the contents of the
                # state file, fail immediately.
                dependencies, paths = self._fetch_state()
                logger.info(
                    'Found {} dependencies, {} paths from cache.'.format(
                        len(dependencies), len(paths)
                    )
                )
            else:
                dependencies: Dict[DependencyKey, DependencyState] = dict()
                paths: Set[str] = set()
                logger.info(
                    f'State file did not exist. Will create one at path {self._state_committer.path}.'
                )

            # Get the paths that exist in dependency state, loaded path and
            # the local file system (the dependency directories under self.dependencies_dir)
            local_directories = set(os.listdir(self.dependencies_dir))
            paths_in_loaded_state = [dep_state.path for dep_state in dependencies.values()]
            paths = paths.intersection(paths_in_loaded_state).intersection(local_directories)

            # Remove the orphaned dependencies if they don't exist in paths
            # (intersection of paths in dependency state, loaded paths and the paths on the local file system)
            dependencies_to_remove = [
                dep for dep, dep_state in dependencies.items() if dep_state.path not in paths
            ]
            for dep in dependencies_to_remove:
                logger.info(
                    "Dependency {} in dependency state but its path {} doesn't exist on the local file system. "
                    "Removing it from dependency state.".format(
                        dep, os.path.join(self.dependencies_dir, dependencies[dep].path)
                    )
                )
                del dependencies[dep]

            # Remove the orphaned directories from the local file system
            directories_to_remove = local_directories - paths
            for directory in directories_to_remove:
                full_path = os.path.join(self.dependencies_dir, directory)
                if os.path.exists(full_path):
                    logger.info(
                        "Remove orphaned directory {} from the local file system.".format(full_path)
                    )
                    remove_path(full_path)

            # Save the current synced state back to the state file: dependency-state.json as
            # the current state might have been changed during the state syncing phase
            self._commit_state(dependencies, paths)

    def _fetch_state(self, default=None):
        """
        Fetch state from JSON file stored on disk.
        NOT NFS-SAFE - Caller should acquire self._state_lock before calling this method.
        WARNING: If a value for `default` is specified, errors will be silently handled.
        """
        assert self._state_lock.is_locked
        state: DependencyManagerState = self._state_committer.load(default)
        dependencies: Dict[DependencyKey, DependencyState] = state['dependencies']
        paths: Set[str] = state['paths']
        return dependencies, paths

    def _fetch_dependencies(self, default=None) -> Dict[DependencyKey, DependencyState]:
        """
        Fetch dependencies from JSON file stored on disk.
        NOT NFS-SAFE - Caller should acquire self._state_lock before calling this method.
        WARNING: If a value for `default` is specified, errors will be silently handled.
        """
        assert self._state_lock.is_locked
        dependencies, _ = self._fetch_state(default)
        return dependencies

    def _commit_state(self, dependencies: Dict[DependencyKey, DependencyState], paths: Set[str]):
        """
        Update state in dependencies JSON file stored on disk.
        NOT NFS-SAFE - Caller should acquire self._state_lock before calling this method.
        """
        assert self._state_lock.is_locked
        state: DependencyManagerState = {'dependencies': dependencies, 'paths': paths}
        self._state_committer.commit(state)

    def start(self):
        logger.info('Starting local dependency manager...')

        def loop(self):
            while not self._stop:
                try:
                    self._transition_dependencies()
                    self._cleanup()
                except Exception:
                    traceback.print_exc()
                time.sleep(1)

        self._main_thread = threading.Thread(target=loop, args=[self])
        self._main_thread.start()

    def stop(self):
        logger.info('Stopping local dependency manager...')
        self._stop = True
        self._downloading.stop()
        self._main_thread.join()
        self._state_lock.release()
        logger.info('Stopped local dependency manager.')

    def _transition_dependencies(self):
        with self._state_lock:
            try:
                dependencies, paths = self._fetch_state()

                # Update the class variable _paths as the transition function may update it
                self._paths = paths
                for dep_key, dep_state in dependencies.items():
                    dependencies[dep_key] = self.transition(dep_state)
                self._commit_state(dependencies, self._paths)
            except (ValueError, EnvironmentError):
                # Do nothing if an error is thrown while reading from the state file
                logging.exception("Error reading from state file while transitioning dependencies")
                pass

    def _prune_failed_dependencies(self):
        """
        Prune failed dependencies older than DEPENDENCY_FAILURE_COOLDOWN seconds so that further runs
        get to retry the download. Without pruning, any future run depending on a
        failed dependency would automatically fail indefinitely.
        """
        with self._state_lock:
            try:
                dependencies, paths = self._fetch_state()
                failed_deps: Dict[DependencyKey, DependencyState] = {
                    dep_key: dep_state
                    for dep_key, dep_state in dependencies.items()
                    if dep_state.stage == DependencyStage.FAILED
                    and time.time() - dep_state.last_used
                    > DependencyManager.DEPENDENCY_FAILURE_COOLDOWN
                }
                if len(failed_deps) == 0:
                    return

                for dep_key, dep_state in failed_deps.items():
                    self._delete_dependency(dep_key, dependencies, paths)
                self._commit_state(dependencies, paths)
            except (ValueError, EnvironmentError):
                # Do nothing if an error is thrown while reading from the state file
                logging.exception(
                    "Error reading from state file while pruning failed dependencies."
                )
                pass

    def _cleanup(self):
        """
        Prune failed dependencies older than DEPENDENCY_FAILURE_COOLDOWN seconds.
        Limit the disk usage of the dependencies (both the bundle files and the serialized state file size)
        Deletes oldest failed dependencies first and then oldest finished dependencies.
        Doesn't touch downloading dependencies.
        """
        self._prune_failed_dependencies()

        while True:
            with self._state_lock:
                try:
                    dependencies, paths = self._fetch_state()
                except (ValueError, EnvironmentError):
                    # Do nothing if an error is thrown while reading from the state file
                    logging.exception(
                        "Error reading from state file when cleaning up dependencies. Trying again..."
                    )
                    continue

                bytes_used = sum(dep_state.size_bytes for dep_state in dependencies.values())
                serialized_length = len(codalab.worker.pyjson.dumps(dependencies))
                if (
                    bytes_used > self._max_cache_size_bytes
                    or serialized_length > DependencyManager.MAX_SERIALIZED_LEN
                ):
                    logger.debug(
                        '%d dependencies, disk usage: %s (max %s), serialized size: %s (max %s)',
                        len(dependencies),
                        size_str(bytes_used),
                        size_str(self._max_cache_size_bytes),
                        size_str(serialized_length),
                        DependencyManager.MAX_SERIALIZED_LEN,
                    )
                    ready_deps = {
                        dep_key: dep_state
                        for dep_key, dep_state in dependencies.items()
                        if dep_state.stage == DependencyStage.READY and not dep_state.dependents
                    }
                    failed_deps = {
                        dep_key: dep_state
                        for dep_key, dep_state in dependencies.items()
                        if dep_state.stage == DependencyStage.FAILED
                    }

                    if failed_deps:
                        dep_key_to_remove = min(
                            failed_deps.items(), key=lambda dep: dep[1].last_used
                        )[0]
                    elif ready_deps:
                        dep_key_to_remove = min(
                            ready_deps.items(), key=lambda dep: dep[1].last_used
                        )[0]
                    else:
                        logger.info(
                            'Dependency quota full but there are only downloading dependencies, not cleaning up '
                            'until downloads are over.'
                        )
                        break
                    if dep_key_to_remove:
                        self._delete_dependency(dep_key_to_remove, dependencies, paths)
                        self._commit_state(dependencies, paths)
                else:
                    break

    def _delete_dependency(self, dep_key, dependencies, paths):
        """
        Remove the given dependency from the manager's state
        Modifies `dependencies` and `paths` that are passed in.
        Also deletes any known files on the filesystem if any exist.

        NOT NFS-SAFE - Caller should acquire self._state_lock before calling this method.
        """
        assert self._state_lock.is_locked

        if dep_key in dependencies:
            try:
                path_to_remove = dependencies[dep_key].path
                paths.remove(path_to_remove)
                # Deletes dependency content from disk
                remove_path(path_to_remove)
            except Exception:
                pass
            finally:
                del dependencies[dep_key]
                logger.info(f"Deleted dependency {dep_key}.")

    def has(self, dependency_key):
        """
        Takes a DependencyKey and returns true if the manager has processed this dependency
        """
        with self._state_lock:
            dependencies: Dict[DependencyKey, DependencyState] = self._fetch_dependencies()
            return dependency_key in dependencies

    def get(self, uuid: str, dependency_key: DependencyKey) -> DependencyState:
        """
        Request the dependency for the run with uuid, registering uuid as a dependent of this dependency
        """
        with self._state_lock:
            dependencies, paths = self._fetch_state()

            now = time.time()
            # Add dependency state if it does not exist
            if dependency_key not in dependencies:
                dependencies[dependency_key] = DependencyState(
                    stage=DependencyStage.DOWNLOADING,
                    downloading_by=None,
                    dependency_key=dependency_key,
                    path=self._assign_path(paths, dependency_key),
                    size_bytes=0,
                    dependents={uuid},
                    last_used=now,
                    last_downloading=now,
                    message="Starting download",
                    killed=False,
                )

            # Update last_used as long as it isn't in a FAILED stage
            if dependencies[dependency_key].stage != DependencyStage.FAILED:
                dependencies[dependency_key].dependents.add(uuid)
                dependencies[dependency_key] = dependencies[dependency_key]._replace(last_used=now)

            self._commit_state(dependencies, paths)
            return dependencies[dependency_key]

    def release(self, uuid, dependency_key):
        """
        Register that the run with uuid is no longer dependent on this dependency
        If no more runs are dependent on this dependency, kill it.
        """
        with self._state_lock:
            dependencies, paths = self._fetch_state()

            if dependency_key in dependencies:
                dep_state = dependencies[dependency_key]
                if uuid in dep_state.dependents:
                    dep_state.dependents.remove(uuid)
                if not dep_state.dependents:
                    dep_state = dep_state._replace(killed=True)
                    dependencies[dependency_key] = dep_state
                self._commit_state(dependencies, paths)

    def _assign_path(self, paths: Set[str], dependency_key: DependencyKey) -> str:
        """
        Checks the current path against `paths`.
        Normalize the path for the dependency by replacing / with _, avoiding conflicts.
        Adds the new path to `paths`.
        """
        path: str = (
            os.path.join(dependency_key.parent_uuid, dependency_key.parent_path)
            if dependency_key.parent_path
            else dependency_key.parent_uuid
        )
        path = path.replace(os.path.sep, '_')

        # You could have a conflict between, for example a/b_c and a_b/c
        while path in paths:
            path = path + '_'

        paths.add(path)
        return path

    def _store_dependency(self, dependency_path, fileobj, target_type):
        """
        Copy the dependency fileobj to its path on the local filesystem
        Overwrite existing files by the same name if found
        (may happen if filesystem modified outside the dependency manager,
         for example during an update if the state gets reset but filesystem
         doesn't get cleared)
        """
        try:
            if os.path.exists(dependency_path):
                logger.info('Path %s already exists, overwriting', dependency_path)
                if os.path.isdir(dependency_path):
                    shutil.rmtree(dependency_path)
                else:
                    os.remove(dependency_path)
            if target_type == 'directory':
                un_tar_directory(fileobj, dependency_path, 'gz')
            else:
                with open(dependency_path, 'wb') as f:
                    logger.debug('copying file to %s', dependency_path)
                    shutil.copyfileobj(fileobj, f)
        except Exception:
            raise

    @property
    def all_dependencies(self) -> List[DependencyKey]:
        with self._state_lock:
            dependencies: Dict[DependencyKey, DependencyState] = self._fetch_dependencies(
                default={'dependencies': {}, 'paths': set()}
            )
            return list(dependencies.keys())

    def _transition_from_DOWNLOADING(self, dependency_state: DependencyState):
        """
        Checks if the dependency is downloading or not.
        NOT NFS-SAFE - Caller should acquire self._state_lock before calling this method.
        """
        assert self._state_lock.is_locked

        def download():
            """
            Runs in a separate thread. Only one worker should be running this in a thread at a time.
            """

            def update_state_and_check_killed(bytes_downloaded):
                """
                Callback method for bundle service client updates dependency state and
                raises DownloadAbortedException if download is killed by dep. manager

                Note: This function needs to be fast, since it's called every time fileobj.read is called.
                      Therefore, we keep a copy of the state in memory (self._downloading) and copy over
                      non-critical fields (last_downloading, size_bytes and message) when the download transition
                      function is executed.
                """
                state = self._downloading[dependency_state.dependency_key]['state']
                if state.killed:
                    raise DownloadAbortedException("Aborted by user")
                self._downloading[dependency_state.dependency_key]['state'] = state._replace(
                    last_downloading=time.time(),
                    size_bytes=bytes_downloaded,
                    message=f"Downloading dependency: {str(bytes_downloaded)} downloaded",
                )

            dependency_path = os.path.join(self.dependencies_dir, dependency_state.path)
            logger.debug('Downloading dependency %s', dependency_state.dependency_key)

            attempt = 0
            while attempt < self._download_dependencies_max_retries:
                try:
                    # Start async download to the fileobj
                    target_type = self._bundle_service.get_bundle_info(
                        dependency_state.dependency_key.parent_uuid,
                        dependency_state.dependency_key.parent_path,
                    )["type"]
                    fileobj = self._bundle_service.get_bundle_contents(
                        dependency_state.dependency_key.parent_uuid,
                        dependency_state.dependency_key.parent_path,
                    )
                    with closing(fileobj):
                        # "Bug" the fileobj's read function so that we can keep
                        # track of the number of bytes downloaded so far.
                        original_read_method = fileobj.read
                        bytes_downloaded = [0]

                        def interruptable_read(*args, **kwargs):
                            data = original_read_method(*args, **kwargs)
                            bytes_downloaded[0] += len(data)
                            update_state_and_check_killed(bytes_downloaded[0])
                            return data

                        fileobj.read = interruptable_read

                        # Start copying the fileobj to filesystem dependency path
                        # Note: Overwrites if something already exists at dependency_path, such as when
                        #       another worker partially downloads a dependency and then goes offline.
                        self._store_dependency(dependency_path, fileobj, target_type)

                    logger.debug(
                        'Finished downloading %s dependency %s to %s',
                        target_type,
                        dependency_state.dependency_key,
                        dependency_path,
                    )
                    self._downloading[dependency_state.dependency_key]['success'] = True

                except Exception as e:
                    attempt += 1
                    if attempt >= self._download_dependencies_max_retries:
                        self._downloading[dependency_state.dependency_key]['success'] = False
                        self._downloading[dependency_state.dependency_key][
                            'failure_message'
                        ] = f"Dependency download failed: {e} "
                    else:
                        logger.warning(
                            f'Failed to download {dependency_state.dependency_key} after {attempt} attempt(s) '
                            f'due to {e}. Retrying up to {self._download_dependencies_max_retries} times...',
                            exc_info=True,
                        )
                else:
                    # Break out of the retry loop if no exceptions were thrown
                    break

        # Start downloading if either:
        # 1. No other dependency manager is downloading the dependency
        # 2. There was a dependency manager downloading a dependency, but it has been longer than
        #    DEPENDENCY_DOWNLOAD_TIMEOUT_SECONDS since it last downloaded anything for the particular dependency.
        now = time.time()
        if not dependency_state.downloading_by or (
            dependency_state.downloading_by
            and now - dependency_state.last_downloading
            >= DependencyManager.DEPENDENCY_DOWNLOAD_TIMEOUT_SECONDS
        ):
            if not dependency_state.downloading_by:
                logger.info(
                    f"{self._id} will start downloading dependency: {dependency_state.dependency_key}."
                )
            else:
                logger.info(
                    f"{dependency_state.downloading_by} stopped downloading "
                    f"dependency: {dependency_state.dependency_key}. {self._id} will restart downloading."
                )

            self._downloading.add_if_new(
                dependency_state.dependency_key, threading.Thread(target=download, args=[])
            )
            self._downloading[dependency_state.dependency_key]['state'] = dependency_state
            dependency_state = dependency_state._replace(downloading_by=self._id)

        # If there is already another worker downloading the dependency,
        # just return the dependency state as downloading is in progress.
        if dependency_state.downloading_by != self._id:
            logger.debug(
                f"Waiting for {dependency_state.downloading_by} "
                f"to download dependency: {dependency_state.dependency_key}"
            )
            return dependency_state

        if (
            dependency_state.dependency_key in self._downloading
            and self._downloading[dependency_state.dependency_key].is_alive()
        ):
            logger.debug(
                f"This dependency manager ({dependency_state.downloading_by}) "
                f"is downloading dependency: {dependency_state.dependency_key}"
            )
            state = self._downloading[dependency_state.dependency_key]['state']
            # Copy over the values of the non-critical fields of the state in memory
            # that is being updated by the download thread.
            return dependency_state._replace(
                last_downloading=state.last_downloading,
                size_bytes=state.size_bytes,
                message=state.message,
            )

        # At this point, no thread is downloading the dependency, but the dependency is still
        # assigned to the current worker. Check if the download finished.
        success: bool = self._downloading[dependency_state.dependency_key]['success']
        failure_message: str = self._downloading[dependency_state.dependency_key]['failure_message']

        dependency_state = dependency_state._replace(downloading_by=None)
        self._downloading.remove(dependency_state.dependency_key)
        logger.info(
            f"Download complete. Removing downloading thread for {dependency_state.dependency_key}."
        )

        if success:
            return dependency_state._replace(
                stage=DependencyStage.READY, message="Download complete"
            )
        else:
            self._paths.remove(dependency_state.path)
            logger.error(
                f"Dependency {dependency_state.dependency_key} download failed: {failure_message}"
            )
            return dependency_state._replace(stage=DependencyStage.FAILED, message=failure_message)
