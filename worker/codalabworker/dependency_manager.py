from collections import namedtuple
import json
import logging
import os
import threading
import time

from file_util import get_path_size, remove_path
from docker_image_manager import DockerImageManager
from formatting import size_str
from synchronized import synchronized
from fsm import (
    BaseDependencyManager,
    JsonStateCommitter,
    DependencyStatus,
    BaseStateHandler,
)

logger = logging.getLogger(__name__)

DependencyState = namedtuple('DependencyState', 'status dependency path size_bytes last_used')

class LocalFileSystemDependencyManager(BaseDependencyManager):
    def __init__(self, state_committer, work_dir, max_cache_size_bytes, max_serialized_length):

        self._state_committer = state_committer
        self._max_cache_size_bytes = max_cache_size_bytes
        self._max_serialized_length = max_serialized_length or float('inf')
        self._work_dir = work_dir
        self._bundles_dir = os.path.join(work_dir, 'bundles')

        self._paths = set()
        self._dependencies = {}
        self._downloading = {}
        self._load_state()

        self._stop = False
        self._cleanup_sleep_secs = 10
        self._main_thread = None

    def _save_state(self):
        with synchronized(self):
            self._state_committer.commit(self._dependencies)

    def _load_state(self):
        with synchronized(self):
            self._dependencies = self._state_committer.load()
            self.reset()
            logger.info('{} dependencies in cache.'.format(len(self._dependencies)))

    def reset(self):
        with synchronized(self):
            for entry in self._dependencies.keys():
                dependency_state = self._dependencies[entry]
                self._dependencies[entry] = self._reset_dependency_state(dependency_state)

    def run(self):
        def loop(self):
            while not self._stop:
                try:
                    self._process_dependencies()
                    self._save_state()
                    # cleanup
                except Exception:
                    traceback.print_exc()
                time.sleep(self._cleanup_sleep_secs)
        self._main_thread = threading.Thread(target=loop, args=[self])
        self._main_thread.start()

    def stop(self):
        self._stop_cleanup = True
        self._main_thread.join()

    def _process_dependencies(self):
        with synchronized(self):
            for entry in self._dependencies.keys():
                dependency_state = self._dependencies[entry]
                self._dependencies[entry] = self._transition_dependency_state(dependency_state)

    def _reset_dependency_state(self, dependency_state):
        status = dependency_state.status
        fns = [val for key, val in vars().items() if key == '_reset_dependency_state_from_' + status]
        return fns[0]

    def _transition_dependency_state(self, dependency_state):
        status = dependency_state.status
        fns = [val for key, val in vars().items() if key == '_transition_dependency_state_from_' + status]
        return fns[0]

    def has(self, dependency): # dependency = (parent_uuid, parent_path)
        with synchronized(self):
            return (dependency in self._dependencies)

    def get(self, dependency):
        with synchronized(self):
            if not self.has(dependency):
                self._dependencies[dependency] = DependencyState(
                        DependencyStatus.STARTING, dependency, None, 0, None)
            return self._dependencies[dependency]

    def _assign_path(self, dependency):
        parent_uuid, parent_path = dependency
        if parent_path:
            path = os.path.join(parent_uuid, parent_path)
        else:
            path = parent_uuid
        path = path.replace(os.path.sep, '_')

        # You could have a conflict between, for example a/b_c and
        # a_b/c. We have to avoid those.
        with synchronized(self):
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
        except:
            remove_path(dependency_path)
            raise

    def list_all(self):
        with synchronized(self):
            return list(self._dependencies.keys())

    def get_run_path(self, uuid):
        return os.path.join(self._bundles_dir, uuid)

    def _reset_dependency_state_from_STARTING(self, dependency_state):
        return dependency_state

    def _transition_dependency_state_from_STARTING(self, dependency_state):
        dependency = dependency_state.dependency
        return dependency_state._replace(
                status=RunStatus.DOWNLOADING, path=self._assign_path(dependency))

    def _reset_dependency_state_from_DOWNLOADING(self, dependency_state):
        return dependency_state

    def _transition_dependency_state_from_DOWNLOADING(self, dependency_state):
        def download():
            def update_state_and_check_killed(bytes_downloaded):
                with synchronized(self):
                    state = self._dependencies[target]
                    self._dependencies[target] = state._replace(size_bytes=bytes_downloaded)
                #check_killed()

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
                        loop_callback(bytes_downloaded[0])
                        return data
                    fileobj.read = interruptable_read

                    self._store_dependency(dependency_path, fileobj, target_type)
            finally:
                logger.debug('Finished downloading dependency %s/%s', parent_uuid, parent_path)
                with synchronized(self):
                    self._downloading[dependency]['success'] = True

        dependency = dependency_state.dependency
        parent_uuid, parent_path = dependency
        if dependency not in self._downloading:
            self._downloading[dependency] = {
                'thread': threading.Thread(target=download, args=[]),
                'success': False
            }
            self._downloading[dependency]['thread'].start()

        if self._downloading[dependency]['thread'].is_alive():
            return dependency_state

        success = self._downloading[dependency]['success']
        del self._downloading[dependency]
        if success:
            return dependency_state._replace(status=DependencyStatus.READY, last_used=time.time())
        else:
            self._paths.remove(dependency_state.path)
            del self._dependencies[target]
            return dependency_state._replace(status=DependencyStatus.FAILED)

    def _reset_dependency_state_from_READY(self, dependency_state):
        return dependency_state

    def _transition_dependency_state_from_READY(self, dependency_state):
        return dependency_state

    def _reset_dependency_state_from_FAILED(self, dependency_state):
        return dependency_state

    def _transition_dependency_state_from_FAILED(self, dependency_state):
        return dependency_state


class SharedFileSystemDependencyManager(BaseDependencyManager):
    def __init__(self, state_manager):
        self._state_manager = state_manager

    def has(self, dependency):
        pass

    def get(self, dependency, blocking=True):
        if self.has(dependency):
            return dependency
        else:
            # should never get to this point: panic!
            raise Exception("SharedFileSystemDependencyManager: dependency {} not found!".format(dependency))

    def dependencies(self):
        return list() # for a shared file system, return an empty list
