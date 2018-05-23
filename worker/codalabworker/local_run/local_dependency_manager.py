from contextlib import closing
from collections import namedtuple
import json
import logging
import os
import threading
import traceback
import time
import shutil

from ..file_util import get_path_size, remove_path, un_tar_directory
from ..formatting import size_str
from ..fsm import (
    BaseDependencyManager,
    JsonStateCommitter,
    DependencyStage,
    StateTransitioner
)

logger = logging.getLogger(__name__)

DependencyState = namedtuple('DependencyState', 'stage dependency path size_bytes dependents last_used message killed')

class DownloadAbortedException(Exception):
    """
    Exception raised by the download if a download is killed before it is complete
    """
    def __init__(self, message):
        super(DownloadAbortedException, self).__init__(message)

class LocalFileSystemDependencyManager(StateTransitioner, BaseDependencyManager):
    def __init__(self, state_committer, bundle_service,
            work_dir, max_cache_size_bytes, max_serialized_length):

        super(LocalFileSystemDependencyManager, self).__init__()
        self.add_transition(DependencyStage.DOWNLOADING, self._transition_from_DOWNLOADING)
        self.add_transition(DependencyStage.READY, self._transition_from_READY)
        self.add_transition(DependencyStage.FAILED, self._transition_from_FAILED)

        self._state_committer = state_committer
        self._bundle_service = bundle_service
        self._max_cache_size_bytes = max_cache_size_bytes
        self._max_serialized_length = max_serialized_length or float('inf')
        self._work_dir = work_dir
        self._bundles_dir = os.path.join(work_dir, 'bundles')

        self._lock = threading.RLock()

        self._paths = set()
        self._dependencies = dict()
        self._downloading = dict()
        self._load_state()

        self._stop = False
        self._main_thread = None

    def _save_state(self):
        with self._lock:
            self._state_committer.commit(self._dependencies)

    def _load_state(self):
        with self._lock:
            self._dependencies = self._state_committer.load()
            assert(isinstance(self._dependencies, dict))
            logger.error(self._dependencies)
            logger.info('{} dependencies in cache.'.format(len(self._dependencies)))

    def start(self):
        def loop(self):
            while not self._stop:
                try:
                    self._process_dependencies()
                    self._save_state()
                    self._cleanup()
                    self._save_state()
                except Exception:
                    traceback.print_exc()
                time.sleep(self._cleanup_sleep_secs)
        self._main_thread = threading.Thread(target=loop, args=[self])
        self._main_thread.start()

    def stop(self):
        self._stop = True
        self._main_thread.join()

    def _process_dependencies(self):
        with self._lock:
            for entry in self._dependencies.keys():
                dependency_state = self._dependencies[entry]
                self._dependencies[entry] = self.transition(dependency_state)

    def _cleanup(self):
        while True:
            with self._lock:
                bytes_used = sum(dep.size_bytes for dep in self._dependencies.values())
                serialized_dependencies = {'{}+{}'.format(*k): v for k, v in self._dependencies.items()}
                serialized_length = len(json.dumps(serialized_dependencies))
                if bytes_used > self._max_cache_size_bytes or serialized_length > self._max_serialized_length:
                    logger.debug('%d dependencies in cache, disk usage: %s (max %s), serialized size: %s (max %s)',
                                  len(self._dependencies),
                                  size_str(bytes_used),
                                  size_str(self._max_cache_size_bytes),
                                  size_str(serialized_length),
                                  size_str(self._max_serialized_length))
                    failed_deps = {dep: state for dep, state in self._dependencies.items() if dep.stage == DependencyStage.FAILED}
                    ready_deps = {dep: state for dep, state in self._dependencies.items() if dep.stage == DependencyStage.READY and not state.dependents}
                    if failed_deps:
                        dep_to_remove = min(failed_deps, key=lambda i: failed_deps[i].last_used)
                    elif ready_deps:
                        dep_to_remove = min(ready_deps, key=lambda i: ready_deps[i].last_used)
                    else:
                        # TODO: What do we do if there are only downloading deps but together they are bigger than the quota
                        break
                    try:
                        self._paths.remove(self._dependencies[dep_to_remove].path)
                    finally:
                        del self._dependencies[dep_to_remove]
                else:
                    break



    def has(self, dependency): # dependency = (parent_uuid, parent_path)
        with self._lock:
            return (dependency in self._dependencies)

    def get(self, uuid, dependency):
        """
        Request the dependency for the run with uuid, registering uuid as a dependent of this dependency
        """
        now = time.time()
        with self._lock:
            if not self.has(dependency): # add dependency state if it does not exist
                self._dependencies[dependency] = DependencyState(stage=DependencyStage.DOWNLOADING,
                        dependency=dependency, path=self._assign_path(dependency), size_bytes=0,
                        dependents=set(uuid), last_used=now, message="Starting download", killed=False)

            # update last_used as long as it isn't in FAILED
            if self._dependencies[dependency].stage != DependencyStage.FAILED:
                self._dependencies[dependency].dependents.add(uuid)
                self._dependencies[dependency] = self._dependencies[dependency]._replace(last_used=now)
                logger.debug('Touched dependency %s at %f, added dependent %s', dependency, now, uuid)
            return self._dependencies[dependency]

    def release(self, uuid, dependency):
        """
        Register that the run with uuid is no longer dependent on this dependency
        """
        with self._lock:
            if self.has(dependency) and uuid in self._dependencies[dependency].dependents:
                self._dependencies[dependency].dependents.remove(uuid)

    def _assign_path(self, dependency):
        parent_uuid, parent_path = dependency
        if parent_path:
            path = os.path.join(parent_uuid, parent_path)
        else:
            path = parent_uuid
        path = path.replace(os.path.sep, '_')

        # You could have a conflict between, for example a/b_c and
        # a_b/c. We have to avoid those.
        with self._lock:
            while path in self._paths:
                path = path + '_'
            self._paths.add(path)
        return path

    def _store_dependency(self, dependency_path, fileobj, target_type):
        try:
            if target_type == 'directory':
                un_tar_directory(fileobj, dependency_path, 'gz')
            else:
                with open(dependency_path, 'wb') as f:
                    shutil.copyfileobj(fileobj, f)
        except :
            raise

    @property
    def all_dependencies(self):
        with self._lock:
            return list(self._dependencies.keys())

    def get_run_path(self, uuid):
        return os.path.join(self._bundles_dir, uuid)

    def _transition_from_DOWNLOADING(self, dependency_state):
        def download():
            def update_state_and_check_killed(bytes_downloaded):
                with self._lock:
                    state = self._dependencies[dependency]
                    if state.killed:
                        raise DownloadAbortedException("Aborted by user")
                    self._dependencies[dependency] = state._replace(size_bytes=bytes_downloaded, message="Downloading dependency: %s downloaded" % size_str(bytes_downloaded))

            dependency_path = os.path.join(self._bundles_dir, dependency_state.path)
            logger.debug('Downloading dependency %s/%s', parent_uuid, parent_path)
            try:
                fileobj, target_type = (
                    self._bundle_service.get_bundle_contents(parent_uuid, parent_path))
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

                    self._store_dependency(dependency_path, fileobj, target_type)

                logger.debug('Finished downloading dependency %s/%s', parent_uuid, parent_path)
                with self._lock:
                    self._downloading[dependency]['success'] = True

            except Exception as e:
                with self._lock:
                    self._downloading[dependency]['success'] = False
                    self._downloading[dependency]['failure_message'] = "Depdendency download failed: %s " % str(e)

        dependency = dependency_state.dependency
        parent_uuid, parent_path = dependency
        if dependency not in self._downloading:
            self._downloading[dependency] = {
                'thread': threading.Thread(target=download, args=[]),
                'success': False,
                'failure_message': None
            }
            self._downloading[dependency]['thread'].start()

        if self._downloading[dependency]['thread'].is_alive():
            return dependency_state

        success = self._downloading[dependency]['success']
        failure_message = self._downloading[dependency]['failure_message']

        del self._downloading[dependency]
        if success:
            return dependency_state._replace(stage=DependencyStage.READY, message="Download complete")
        else:
            self._paths.remove(dependency_state.path)
            return dependency_state._replace(stage=DependencyStage.FAILED, message=failure_message)

    def _transition_from_READY(self, dependency_state):
        return dependency_state

    def _transition_from_FAILED(self, dependency_state):
        return dependency_state


class SharedFileSystemDependencyManager(BaseDependencyManager):
    def __init__(self, state_manager):
        self._state_manager = state_manager

    def run(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def has(self, dependency):
        raise NotImplementedError

    def get(self, dependency, blocking=True):
        raise NotImplementedError

    @property
    def all_dependencies(self):
        raise NotImplementedError
