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
from typing import Dict, Set, Union

from flufl.lock import Lock, AlreadyLockedError

import codalab.worker.pyjson
from .bundle_service_client import BundleServiceClient
from codalab.lib.formatting import size_str
from codalab.worker.file_util import remove_path
from codalab.worker.un_tar_directory import un_tar_directory
from codalab.worker.fsm import DependencyStage
from codalab.worker.worker_thread import ThreadDict
from codalab.worker.bundle_state import DependencyKey
from codalab.worker.dependency_manager import (
    DownloadAbortedException,
    DependencyManager,
)

logger = logging.getLogger(__name__)

DependencyState = namedtuple(
    'DependencyState',
    'stage downloading_by dependency_key path size_bytes dependents last_used last_downloading message killed',
)


class NFSLock(threading._RLock):
    def __init__(self, path):
        super().__init__()

        # Specify the path to a file that will be used to synchronize the lock.
        # Per the flufl.lock documentation, use a file that does not exist.
        self._lock = Lock(path)
        # Locks have a lifetime (default 15 seconds) which is the period of time that the process expects
        # to keep the lock once it has been acquired. We set the lifetime to be 1 hour as we expect
        # all operations that require locks to be completed within that time.
        self._lock.lifetime = timedelta(hours=1)

    @property
    def is_locked(self):
        return self._lock.is_locked

    def acquire(self, blocking=True, timeout=-1):
        try:
            # Errors when attempting to acquire the lock more than once in the same process
            self._lock.lock()
        except AlreadyLockedError as error:
            pass

    def release(self):
        self._lock.unlock(unconditionally=True)

    __enter__ = acquire

    def __exit__(self, t, v, tb):
        self.release()


class NFSDependencyManager(DependencyManager):
    """
    NFS-safe version of the DependencyManager.
    """

    _DEPENDENCY_DOWNLOAD_TIMEOUT_SECONDS = 60 * 60

    def __init__(
        self,
        commit_file: str,
        bundle_service: BundleServiceClient,
        worker_dir: str,
        max_cache_size_bytes: int,
        download_dependencies_max_retries: int,
    ):
        super(NFSDependencyManager, self).__init__(
            commit_file,
            bundle_service,
            worker_dir,
            max_cache_size_bytes,
            download_dependencies_max_retries,
        )
        self._id: str = "worker-dependency-manager-{}".format(uuid.uuid4().hex[:8])

        # Locks for concurrency
        self._dependency_locks: Dict[DependencyKey, threading.RLock] = dict()
        self._global_lock = self._create_lock('global')  # Used for add/remove actions

        # DependencyKey -> WorkerThread(thread, success, failure_message)
        self._downloading = ThreadDict(fields={'success': False, 'failure_message': None})

        # Sync states between dependency-state.json and dependency directories on the local file system.
        self._sync_state()

        self._stop = False
        self._main_thread = None

    def _create_lock(self, name):
        path = os.path.join(self.dependencies_dir, f'{name}.lock')
        return NFSLock(path)

    def _sync_state(self):
        """
        Synchronize dependency states between dependencies-state.json and the local file system as follows:
        1. self._dependency_locks, dependencies and paths: populated from dependencies-state.json
        2. directories on the local file system: the bundle contents
        This function forces the 1 and 2 to be in sync by taking the intersection (e.g., deleting bundles from the
        local file system that don't appear in the dependencies-state.json and vice-versa)
        """
        with self._global_lock:
            # Load states from dependencies-state.json, which contains information about bundles (e.g., state,
            # dependencies, last used, etc.) and create a lock for each dependency.
            state = self._state_committer.load(default={'dependencies': {}, 'paths': set()})

            paths = state['paths']
            dependencies = {}
            dependency_locks = {}
            for dep, dep_state in state['dependencies'].items():
                dependencies[dep] = dep_state
                dependency_locks[dep] = self._create_lock(dep.parent_uuid)
            self._dependency_locks = dependency_locks

            logger.info(
                'Found {} dependencies, {} paths from cache.'.format(len(dependencies), len(paths))
            )

            # Get the paths that exist in dependency state, loaded path and
            # the local file system (the dependency directories under self.dependencies_dir)
            local_directories = set(os.listdir(self.dependencies_dir))
            paths_in_loaded_state = [dep_state.path for dep_state in dependencies.values()]
            paths = paths.intersection(paths_in_loaded_state).intersection(local_directories)

            # Remove the orphaned dependencies and self._dependency_locks if they don't exist in
            # paths (intersection of paths in dependency state, loaded paths and the paths on the local file system)
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
                del self._dependency_locks[dep]

            # Remove the orphaned directories from the local file system
            directories_to_remove = local_directories - paths
            for directory in directories_to_remove:
                full_path = os.path.join(self.dependencies_dir, directory)
                logger.info(
                    "Remove orphaned directory {} from the local file system.".format(full_path)
                )
                remove_path(full_path)

            # Save the current synced state back to the state file: dependency-state.json as
            # the current state might have been changed during the state syncing phase
            self._commit_state(dependencies, paths)

    def _fetch_dependencies(self) -> Dict[DependencyKey, DependencyState]:
        """
        Fetch state from dependencies JSON file stored on disk.
        Not thread safe. Caller should acquire self._global_lock before calling this method.
        """
        state = self._state_committer.load(default={'dependencies': {}, 'paths': set()})
        return state['dependencies']

    def _fetch_paths(self) -> Set[str]:
        """
        Fetch normalized paths from JSON file stored on disk.
        Not thread safe. Caller should acquire self._global_lock before calling this method.
        """
        state = self._state_committer.load(default={'dependencies': {}, 'paths': set()})
        return state['paths']

    def _commit_dependencies(self, dependencies: Dict[DependencyKey, DependencyState]):
        """
        Update state in dependencies JSON file stored on disk.
        Not thread safe. Caller should acquire self._global_lock before calling this method.
        """
        state = self._state_committer.load(default={'dependencies': {}, 'paths': set()})
        state['dependencies'] = dependencies
        self._state_committer.commit(state)

    def _commit_paths(self, paths: Set[str]):
        """
        Update paths in JSON file stored on disk.
        Not thread safe. Caller should acquire self._global_lock before calling this method.
        """
        state = self._state_committer.load(default={'dependencies': {}, 'paths': set()})
        state['paths'] = paths
        self._state_committer.commit(state)

    def _commit_state(self, dependencies: Dict[DependencyKey, DependencyState], paths: Set[str]):
        """
        Update state in dependencies JSON file stored on disk.
        Not thread safe. Caller should acquire self._global_lock before calling this method.
        """
        state: Dict[str, Union[Dict[DependencyKey, DependencyState], Set[str]]] = dict()
        state['dependencies'] = dependencies
        state['paths'] = paths
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

        # Release all locks
        self._release_all_locks()
        self._global_lock.release()
        logger.info('Stopped local dependency manager.')

    def _transition_dependencies(self):
        with self._global_lock:
            dependencies: Dict[DependencyKey, DependencyState] = self._fetch_dependencies()
            for dep_key, dep_state in dependencies.items():
                with self._dependency_locks[dep_key]:
                    dependencies[dep_key] = self.transition(dep_state)
            self._commit_dependencies(dependencies)

    def _prune_failed_dependencies(self):
        """
        Prune failed dependencies older than DEPENDENCY_FAILURE_COOLDOWN seconds so that further runs
        get to retry the download. Without pruning, any future run depending on a
        failed dependency would automatically fail indefinitely.
        """
        with self._global_lock:
            dependencies: Dict[DependencyKey, DependencyState] = self._fetch_dependencies()
            paths: Set[str] = self._fetch_paths()

            for dep_key, dep_state in dependencies.items():
                if (
                    dep_state.stage == DependencyStage.FAILED
                    and time.time() - dep_state.last_used
                    > DependencyManager.DEPENDENCY_FAILURE_COOLDOWN
                ):
                    if self._acquire_if_exists(dep_key):
                        self._delete_dependency(dep_key, dependencies, paths)
            self._commit_state(dependencies, paths)

    def _cleanup(self):
        """
        Prune failed dependencies older than DEPENDENCY_FAILURE_COOLDOWN seconds.
        Limit the disk usage of the dependencies (both the bundle files and the serialized state file size)
        Deletes oldest failed dependencies first and then oldest finished dependencies.
        Doesn't touch downloading dependencies.
        """
        self._prune_failed_dependencies()
        # With all the locks (should be fast if no cleanup needed, otherwise make sure nothing is corrupted
        while True:
            with self._global_lock:
                dependencies: Dict[DependencyKey, DependencyState] = self._fetch_dependencies()
                paths: Set[str] = self._fetch_paths()

                self._acquire_all_locks()
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
                        self._release_all_locks()
                        break
                    if dep_key_to_remove:
                        self._delete_dependency(dependencies, paths, dep_key_to_remove)
                        self._commit_state(dependencies, paths)
                else:
                    self._release_all_locks()
                    break

    def _delete_dependency(self, dep_key, dependencies, paths):
        """
        Remove the given dependency from the manager's state
        Also delete any known files on the filesystem if any exist
        """
        if self._acquire_if_exists(dep_key):
            try:
                path_to_remove = dependencies[dep_key].path
                paths.remove(path_to_remove)
                # Deletes dependency content from disk
                remove_path(path_to_remove)
            except Exception:
                pass
            finally:
                del dependencies[dep_key]
                self._dependency_locks[dep_key].release()

    def has(self, dependency_key):
        """
        Takes a DependencyKey
        Returns true if the manager has processed this dependency
        """
        with self._global_lock:
            dependencies: Dict[DependencyKey, DependencyState] = self._fetch_dependencies()
            return dependency_key in dependencies

    def get(self, uuid: str, dependency_key: DependencyKey) -> DependencyState:
        """
        Request the dependency for the run with uuid, registering uuid as a dependent of this dependency
        """
        with self._global_lock:
            dependencies: Dict[DependencyKey, DependencyState] = self._fetch_dependencies()

            now = time.time()
            if not self._acquire_if_exists(
                dependency_key
            ):  # add dependency state if it does not exist
                self._dependency_locks[dependency_key] = self._create_lock(
                    dependency_key.parent_uuid
                )
                self._dependency_locks[dependency_key].acquire()
                dependencies[dependency_key] = DependencyState(
                    stage=DependencyStage.DOWNLOADING,
                    downloading_by=None,
                    dependency_key=dependency_key,
                    path=self._assign_path(dependency_key),
                    size_bytes=0,
                    dependents=set([uuid]),
                    last_used=now,
                    last_downloading=now,
                    message="Starting download",
                    killed=False,
                )

            # update last_used as long as it isn't in FAILED
            if dependencies[dependency_key].stage != DependencyStage.FAILED:
                dependencies[dependency_key].dependents.add(uuid)
                dependencies[dependency_key] = dependencies[dependency_key]._replace(last_used=now)
            self._commit_dependencies(dependencies)
            self._dependency_locks[dependency_key].release()
            return dependencies[dependency_key]

    def release(self, uuid, dependency_key):
        """
        Register that the run with uuid is no longer dependent on this dependency
        If no more runs are dependent on this dependency, kill it
        """
        if self._acquire_if_exists(dependency_key):
            with self._global_lock:
                dependencies: Dict[DependencyKey, DependencyState] = self._fetch_dependencies()
                dep_state = dependencies[dependency_key]
                if uuid in dep_state.dependents:
                    dep_state.dependents.remove(uuid)
                if not dep_state.dependents:
                    dep_state = dep_state._replace(killed=True)
                    dependencies[dependency_key] = dep_state
                self._commit_dependencies(dependencies)
                self._dependency_locks[dependency_key].release()

    def _acquire_if_exists(self, dependency_key):
        """
        Safely acquires a lock for the given dependency if it exists
        Returns True if dependency exists, False otherwise
        Callers should remember to release the lock
        """
        with self._global_lock:
            dependencies: Dict[DependencyKey, DependencyState] = self._fetch_dependencies()
            if dependency_key in dependencies:
                self._dependency_locks[dependency_key].acquire()
                return True
            else:
                return False

    def _acquire_all_locks(self):
        """
        Acquires all dependency locks in the thread it's called from
        """
        with self._global_lock:
            for dependency, lock in self._dependency_locks.items():
                lock.acquire()

    def _release_all_locks(self):
        """
        Releases all dependency locks in the thread it's called from
        """
        with self._global_lock:
            for dependency, lock in self._dependency_locks.items():
                lock.release()

    def _assign_path(self, dependency_key):
        """
        Normalize the path for the dependency by replacing / with _, avoiding conflicts
        """
        if dependency_key.parent_path:
            path = os.path.join(dependency_key.parent_uuid, dependency_key.parent_path)
        else:
            path = dependency_key.parent_uuid
        path = path.replace(os.path.sep, '_')

        # You could have a conflict between, for example a/b_c and
        # a_b/c. We have to avoid those.
        with self._global_lock:
            paths: Set[str] = self._fetch_paths()
            while path in paths:
                path = path + '_'
            paths.add(path)
            self._commit_paths(paths)
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
    def all_dependencies(self):
        with self._global_lock:
            dependencies: Dict[DependencyKey, DependencyState] = self._fetch_dependencies()
            return list(dependencies.keys())

    def _transition_from_DOWNLOADING(self, dependency_state):
        def download():
            """
            Runs in a separate thread. Only one worker should be running this in a thread at a time.
            """

            def update_state_and_check_killed(bytes_downloaded):
                """
                Callback method for bundle service client updates dependency state and
                raises DownloadAbortedException if download is killed by dep. manager
                """
                with self._dependency_locks[dependency_state.dependency_key], self._global_lock:
                    dependencies: Dict[DependencyKey, DependencyState] = self._fetch_dependencies()
                    state = dependencies[dependency_state.dependency_key]
                    if state.killed:
                        raise DownloadAbortedException("Aborted by user")
                    dependencies[dependency_state.dependency_key] = state._replace(
                        size_bytes=bytes_downloaded,
                        message="Downloading dependency: %s downloaded"
                        % size_str(bytes_downloaded),
                        last_downloading=time.time(),
                    )
                    self._commit_dependencies(dependencies)

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
                        old_read_method = fileobj.read
                        bytes_downloaded = [0]

                        def interruptable_read(*args, **kwargs):
                            data = old_read_method(*args, **kwargs)
                            bytes_downloaded[0] += len(data)
                            update_state_and_check_killed(bytes_downloaded[0])
                            return data

                        fileobj.read = interruptable_read

                        # Start copying the fileobj to filesystem dependency path
                        # Note: overwrites if something already exists at dependency_path
                        self._store_dependency(dependency_path, fileobj, target_type)

                    logger.debug(
                        'Finished downloading %s dependency %s to %s',
                        target_type,
                        dependency_state.dependency_key,
                        dependency_path,
                    )
                    with self._dependency_locks[dependency_state.dependency_key]:
                        self._downloading[dependency_state.dependency_key]['success'] = True

                except Exception as e:
                    attempt += 1
                    if attempt >= self._download_dependencies_max_retries:
                        with self._dependency_locks[dependency_state.dependency_key]:
                            self._downloading[dependency_state.dependency_key]['success'] = False
                            self._downloading[dependency_state.dependency_key][
                                'failure_message'
                            ] = "Dependency download failed: %s " % str(e)
                    else:
                        logger.warning(
                            f'Failed to download {dependency_state.dependency_key} after {attempt} attempt(s) '
                            f'due to {str(e)}. Retrying up to {self._download_dependencies_max_retries} times...'
                        )
                else:
                    # Break out of the retry loop if no exceptions were thrown
                    break

        # Start downloading if:
        # 1. No other dependency manager is downloading the dependency
        # 2. There was a dependency manager downloading a dependency, but it has been longer than
        #    DEPENDENCY_DOWNLOAD_TIMEOUT_SECONDS since it last downloaded anything for the particular dependency.
        now = time.time()
        if not dependency_state.downloading_by or (
            dependency_state.downloading_by
            and now - dependency_state.last_downloading
            >= NFSDependencyManager._DEPENDENCY_DOWNLOAD_TIMEOUT_SECONDS
        ):
            self._downloading.add_if_new(
                dependency_state.dependency_key, threading.Thread(target=download, args=[])
            )
            dependency_state = dependency_state._replace(downloading_by=self._id)

        # If there is already a thread or another dependency manager downloading the dependency,
        # just return the dependency state as downloading is in progress.
        if (
            dependency_state.downloading_by and dependency_state.downloading_by != self._id
        ) or self._downloading[dependency_state.dependency_key].is_alive():
            return dependency_state

        # At this point, no thread is downloading the dependency. Check the status of the download.
        success = self._downloading[dependency_state.dependency_key]['success']
        failure_message = self._downloading[dependency_state.dependency_key]['failure_message']

        if dependency_state.downloding_by == self._id:
            dependency_state = dependency_state._replace(downloading_by=None)
            self._downloading.remove(dependency_state.dependency_key)

        if success:
            return dependency_state._replace(
                stage=DependencyStage.READY, message="Download complete"
            )
        else:
            with self._global_lock:
                paths: Set[str] = self._fetch_paths()
                paths.remove(dependency_state.path)
                self._commit_paths(paths)

            logger.error(failure_message)
            return dependency_state._replace(stage=DependencyStage.FAILED, message=failure_message)
