from contextlib import closing
from collections import namedtuple
import logging
import os
import threading
import traceback
import time
import shutil
from typing import Dict

from codalab.lib.formatting import size_str
from codalab.worker.file_util import remove_path
from codalab.worker.un_tar_directory import un_tar_directory
from codalab.worker.fsm import BaseDependencyManager, DependencyStage, StateTransitioner
import codalab.worker.pyjson
from codalab.worker.worker_thread import ThreadDict
from codalab.worker.state_committer import JsonStateCommitter
from codalab.worker.bundle_state import DependencyKey

logger = logging.getLogger(__name__)

DependencyState = namedtuple(
    'DependencyState', 'stage dependency_key path size_bytes dependents last_used message killed'
)


class DownloadAbortedException(Exception):
    """

    Exception raised by the download if a download is killed before it is complete
    """

    def __init__(self, message):
        super(DownloadAbortedException, self).__init__(message)


class DependencyManager(StateTransitioner, BaseDependencyManager):
    """
    This dependency manager downloads dependency bundles from Codalab server
    to the local filesystem. It caches all downloaded dependencies but cleans up the
    old ones if the disk use hits the given threshold

    For this class dependencies are uniquely identified by DependencyKey
    """

    DEPENDENCIES_DIR_NAME = 'dependencies'
    DEPENDENCY_FAILURE_COOLDOWN = 10
    # TODO(bkgoksel): The server writes these to the worker_dependencies table, which stores the dependencies
    # json as a SqlAlchemy LargeBinary, which defaults to MySQL BLOB, which has a size limit of
    # 65K. For now we limit this value to about 58K to avoid any issues but we probably want to do
    # something better (either specify MEDIUMBLOB in the SqlAlchemy definition of the table or change
    # the data format of how we store this)
    MAX_SERIALIZED_LEN = 60000

    def __init__(self, commit_file, bundle_service, worker_dir, max_cache_size_bytes):
        super(DependencyManager, self).__init__()
        self.add_transition(DependencyStage.DOWNLOADING, self._transition_from_DOWNLOADING)
        self.add_terminal(DependencyStage.READY)
        self.add_terminal(DependencyStage.FAILED)

        self._state_committer = JsonStateCommitter(commit_file)
        self._bundle_service = bundle_service
        self._max_cache_size_bytes = max_cache_size_bytes
        self.dependencies_dir = os.path.join(worker_dir, DependencyManager.DEPENDENCIES_DIR_NAME)
        if not os.path.exists(self.dependencies_dir):
            logger.info('{} doesn\'t exist, creating.'.format(self.dependencies_dir))
            os.makedirs(self.dependencies_dir, 0o770)

        # Locks for concurrency
        self._dependency_locks = dict()  # type: Dict[DependencyKey, threading.RLock]
        self._global_lock = threading.RLock()  # Used for add/remove actions
        self._paths_lock = threading.RLock()  # Used for path name computations

        # File paths that are currently being used to store dependencies. Used to prevent conflicts
        self._paths = set()
        # DependencyKey -> DependencyState
        self._dependencies = dict()
        # DependencyKey -> WorkerThread(thread, success, failure_message)
        self._downloading = ThreadDict(fields={'success': False, 'failure_message': None})
        self._load_state()
        # Sync states between dependency-state.json and dependency directories on the local file system.
        self._sync_state()

        self._stop = False
        self._main_thread = None

    def _save_state(self):
        with self._global_lock, self._paths_lock:
            self._state_committer.commit({'dependencies': self._dependencies, 'paths': self._paths})

    def _load_state(self):
        """
        Load states from dependencies-state.json, which contains information about bundles (e.g., state, dependencies,
        last used, etc.) and populates values for self._dependencies, self._dependency_locks, and self._paths
        """
        state = self._state_committer.load(default={'dependencies': {}, 'paths': set()})

        dependencies = {}
        dependency_locks = {}

        for dep, dep_state in state['dependencies'].items():
            dependencies[dep] = dep_state
            dependency_locks[dep] = threading.RLock()

        with self._global_lock, self._paths_lock:
            self._dependencies = dependencies
            self._dependency_locks = dependency_locks
            self._paths = state['paths']

        logger.info(
            'Loaded {} dependencies, {} paths from cache.'.format(
                len(self._dependencies), len(self._paths)
            )
        )

    def _sync_state(self):
        """
        Synchronize dependency states between dependencies-state.json and the local file system as follows:
        1. self._dependencies, self._dependency_locks, and self._paths: populated from dependencies-state.json
            in function _load_state()
        2. directories on the local file system: the bundle contents
        This function forces the 1 and 2 to be in sync by taking the intersection (e.g., deleting bundles from the
        local file system that don't appear in the dependencies-state.json and vice-versa)
        """
        # Get the paths that exist in dependency state, loaded path and
        # the local file system (the dependency directories under self.dependencies_dir)
        local_directories = set(os.listdir(self.dependencies_dir))
        paths_in_loaded_state = [dep_state.path for dep_state in self._dependencies.values()]
        self._paths = self._paths.intersection(paths_in_loaded_state).intersection(
            local_directories
        )

        # Remove the orphaned dependencies from self._dependencies and
        # self._dependency_locks if they don't exist in self._paths (intersection of paths in dependency state,
        # loaded paths and the paths on the local file system)
        dependencies_to_remove = [
            dep
            for dep, dep_state in self._dependencies.items()
            if dep_state.path not in self._paths
        ]
        for dep in dependencies_to_remove:
            logger.info(
                "Dependency {} in dependency state but its path {} doesn't exist on the local file system. "
                "Removing it from dependency state.".format(
                    dep, os.path.join(self.dependencies_dir, self._dependencies[dep].path)
                )
            )
            del self._dependencies[dep]
            del self._dependency_locks[dep]

        # Remove the orphaned directories from the local file system
        directories_to_remove = local_directories - self._paths
        for dir in directories_to_remove:
            full_path = os.path.join(self.dependencies_dir, dir)
            logger.info(
                "Remove orphaned directory {} from the local file system.".format(full_path)
            )
            remove_path(full_path)

        # Save the current synced state back to the state file: dependency-state.json as
        # the current state might have been changed during the state syncing phase
        self._save_state()

    def start(self):
        logger.info('Starting local dependency manager')

        def loop(self):
            while not self._stop:
                try:
                    self._process_dependencies()
                    self._save_state()
                    self._cleanup()
                    self._save_state()
                except Exception:
                    traceback.print_exc()
                time.sleep(1)

        self._main_thread = threading.Thread(target=loop, args=[self])
        self._main_thread.start()

    def stop(self):
        logger.info('Stopping local dependency manager')
        self._stop = True
        self._downloading.stop()
        self._main_thread.join()
        logger.info('Stopped local dependency manager')

    def _process_dependencies(self):
        for dep_key, dep_state in self._dependencies.items():
            with self._dependency_locks[dep_key]:
                self._dependencies[dep_key] = self.transition(dep_state)

    def _prune_failed_dependencies(self):
        """
        Prune failed dependencies older than DEPENDENCY_FAILURE_COOLDOWN seconds so that further runs
        get to retry the download. Without pruning, any future run depending on a
        failed dependency would automatically fail indefinitely.
        """
        with self._global_lock:
            self._acquire_all_locks()
            failed_deps = {
                dep_key: dep_state
                for dep_key, dep_state in self._dependencies.items()
                if dep_state.stage == DependencyStage.FAILED
                and time.time() - dep_state.last_used
                > DependencyManager.DEPENDENCY_FAILURE_COOLDOWN
            }
            for dep_key, dep_state in failed_deps.items():
                self._delete_dependency(dep_key)
            self._release_all_locks()

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
                self._acquire_all_locks()
                bytes_used = sum(dep_state.size_bytes for dep_state in self._dependencies.values())
                serialized_length = len(codalab.worker.pyjson.dumps(self._dependencies))
                if (
                    bytes_used > self._max_cache_size_bytes
                    or serialized_length > DependencyManager.MAX_SERIALIZED_LEN
                ):
                    logger.debug(
                        '%d dependencies in cache, disk usage: %s (max %s), serialized size: %s (max %s)',
                        len(self._dependencies),
                        size_str(bytes_used),
                        size_str(self._max_cache_size_bytes),
                        size_str(serialized_length),
                        DependencyManager.MAX_SERIALIZED_LEN,
                    )
                    ready_deps = {
                        dep_key: dep_state
                        for dep_key, dep_state in self._dependencies.items()
                        if dep_state.stage == DependencyStage.READY and not dep_state.dependents
                    }
                    failed_deps = {
                        dep_key: dep_state
                        for dep_key, dep_state in self._dependencies.items()
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
                            'Dependency quota full but there are only downloading dependencies, not cleaning up until downloads are over'
                        )
                        self._release_all_locks()
                        break
                    if dep_key_to_remove:
                        self._delete_dependency(dep_key_to_remove)
                    self._release_all_locks()
                else:
                    self._release_all_locks()
                    break

    def _delete_dependency(self, dependency_key):
        """
        Remove the given dependency from the manager's state
        Also delete any known files on the filesystem if any exist
        """
        if self._acquire_if_exists(dependency_key):
            try:
                path_to_remove = self._dependencies[dependency_key].path
                self._paths.remove(path_to_remove)
                remove_path(path_to_remove)
            except Exception:
                pass
            finally:
                del self._dependencies[dependency_key]
                self._dependency_locks[dependency_key].release()

    def has(self, dependency_key):
        """
        Takes a DependencyKey
        Returns true if the manager has processed this dependency
        """
        with self._global_lock:
            return dependency_key in self._dependencies

    def get(self, uuid, dependency_key):
        """
        Request the dependency for the run with uuid, registering uuid as a dependent of this dependency
        """
        now = time.time()
        if not self._acquire_if_exists(dependency_key):  # add dependency state if it does not exist
            with self._global_lock:
                self._dependency_locks[dependency_key] = threading.RLock()
                self._dependency_locks[dependency_key].acquire()
                self._dependencies[dependency_key] = DependencyState(
                    stage=DependencyStage.DOWNLOADING,
                    dependency_key=dependency_key,
                    path=self._assign_path(dependency_key),
                    size_bytes=0,
                    dependents=set([uuid]),
                    last_used=now,
                    message="Starting download",
                    killed=False,
                )

        # update last_used as long as it isn't in FAILED
        if self._dependencies[dependency_key].stage != DependencyStage.FAILED:
            self._dependencies[dependency_key].dependents.add(uuid)
            self._dependencies[dependency_key] = self._dependencies[dependency_key]._replace(
                last_used=now
            )
        self._dependency_locks[dependency_key].release()
        return self._dependencies[dependency_key]

    def release(self, uuid, dependency_key):
        """
        Register that the run with uuid is no longer dependent on this dependency
        If no more runs are dependent on this dependency, kill it
        """
        if self._acquire_if_exists(dependency_key):
            dep_state = self._dependencies[dependency_key]
            if uuid in dep_state.dependents:
                dep_state.dependents.remove(uuid)
            if not dep_state.dependents:
                dep_state = dep_state._replace(killed=True)
                self._dependencies[dependency_key] = dep_state
            self._dependency_locks[dependency_key].release()

    def _acquire_if_exists(self, dependency_key):
        """
        Safely acquires a lock for the given dependency if it exists
        Returns True if depedendency exists, False otherwise
        Callers should remember to release the lock
        """
        with self._global_lock:
            if dependency_key in self._dependencies:
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
        # TODO(Ashwin): make this not fs-specific.
        if dependency_key.parent_path:
            path = os.path.join(dependency_key.parent_uuid, dependency_key.parent_path)
        else:
            path = dependency_key.parent_uuid
        path = path.replace(os.path.sep, '_')

        # You could have a conflict between, for example a/b_c and
        # a_b/c. We have to avoid those.
        with self._paths_lock:
            while path in self._paths:
                path = path + '_'
            self._paths.add(path)
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
            return list(self._dependencies.keys())

    def _transition_from_DOWNLOADING(self, dependency_state):
        def download():
            def update_state_and_check_killed(bytes_downloaded):
                """
                Callback method for bundle service client updates dependency state and
                raises DownloadAbortedException if download is killed by dep. manager
                """
                with self._dependency_locks[dependency_state.dependency_key]:
                    state = self._dependencies[dependency_state.dependency_key]
                    if state.killed:
                        raise DownloadAbortedException("Aborted by user")
                    self._dependencies[dependency_state.dependency_key] = state._replace(
                        size_bytes=bytes_downloaded,
                        message="Downloading dependency: %s downloaded"
                        % size_str(bytes_downloaded),
                    )

            # TODO(Ashwin): make this not fs-specific.
            dependency_path = os.path.join(self.dependencies_dir, dependency_state.path)
            logger.debug('Downloading dependency %s', dependency_state.dependency_key)
            try:
                # Start async download to the fileobj
                fileobj, target_type = self._bundle_service.get_bundle_contents(
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
                with self._dependency_locks[dependency_state.dependency_key]:
                    self._downloading[dependency_state.dependency_key]['success'] = False
                    self._downloading[dependency_state.dependency_key][
                        'failure_message'
                    ] = "Dependency download failed: %s " % str(e)

        self._downloading.add_if_new(
            dependency_state.dependency_key, threading.Thread(target=download, args=[])
        )

        if self._downloading[dependency_state.dependency_key].is_alive():
            return dependency_state

        success = self._downloading[dependency_state.dependency_key]['success']
        failure_message = self._downloading[dependency_state.dependency_key]['failure_message']

        self._downloading.remove(dependency_state.dependency_key)
        if success:
            return dependency_state._replace(
                stage=DependencyStage.READY, message="Download complete"
            )
        else:
            with self._paths_lock:
                self._paths.remove(dependency_state.path)
            return dependency_state._replace(stage=DependencyStage.FAILED, message=failure_message)
