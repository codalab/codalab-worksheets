from contextlib import closing
from collections import namedtuple
import json
import logging
import os
import threading
import traceback
import time
import shutil

from ..file_util import get_path_size, remove_path
from ..formatting import size_str
from ..fsm import (
    BaseDependencyManager,
    JsonStateCommitter,
    DependencyStage,
    StateTransitioner
)

logger = logging.getLogger(__name__)

DependencyState = namedtuple('DependencyState', 'stage dependency path size_bytes last_used')

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
        self._dependencies = {}
        self._downloading = {}
        self._load_state()

        self._stop = False
        self._cleanup_sleep_secs = 10
        self._main_thread = None

    def _save_state(self):
        with self._lock:
            dependencies = {'{}+{}'.format(*k): v for k, v in self._dependencies.items()}
            self._state_committer.commit(dependencies)

    def _load_state(self):
        with self._lock:
            dependencies = self._state_committer.load()
            self._dependencies = {tuple(k.split('+')): v for k, v in dependencies.items()}
            logger.error(self._dependencies)
            logger.info('{} dependencies in cache.'.format(len(self._dependencies)))

    def start(self):
        def loop(self):
            while not self._stop:
                try:
                    self._process_dependencies()
                    self._save_state()
                    # TODO: cleanup
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

    def has(self, dependency): # dependency = (parent_uuid, parent_path)
        with self._lock:
            return (dependency in self._dependencies)

    def get(self, dependency):
        now = time.time()
        with self._lock:
            if not self.has(dependency): # add dependency state if it does not exist
                self._dependencies[dependency] = DependencyState(DependencyStage.DOWNLOADING,
                        dependency, self._assign_path(dependency), 0, now)

            # update last_used as long as it isn't in FAILED
            if self._dependencies[dependency].stage != DependencyStage.FAILED:
                self._dependencies[dependency] = self._dependencies[dependency]._replace(last_used=now)
                logger.debug('Touched dependency %s at %f', dependency, now)
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
        except:
            remove_path(dependency_path)
            raise

    def list_all(self):
        with self._lock:
            return list(self._dependencies.keys())

    def get_run_path(self, uuid):
        return os.path.join(self._bundles_dir, uuid)

    def _transition_from_DOWNLOADING(self, dependency_state):
        def download():
            def update_state_and_check_killed(bytes_downloaded):
                # TODO: check killed
                with self._lock:
                    state = self._dependencies[dependency]
                    self._dependencies[dependency] = state._replace(size_bytes=bytes_downloaded)

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

            except Exception as e: # TODO: get some error message back
                with self._lock:
                    self._downloading[dependency]['success'] = False

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
            return dependency_state._replace(stage=DependencyStage.READY)
        else:
            self._paths.remove(dependency_state.path)
            return dependency_state._replace(stage=DependencyStage.FAILED)

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

    def list_all(self):
        raise NotImplementedError
