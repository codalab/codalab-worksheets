import json
import os
import threading
import time

from file_util import get_path_size, remove_path
from docker_image_manager import DockerImageManager


class DependencyManager(object):
    """
    Manages the dependencies available on the worker, and ensures that no two
    runs download the same dependency at the same time. Ensures that the total
    size of all the dependencies doesn't exceed the given limit.
    """
    STATE_FILENAME = 'state.json'

    def __init__(self, work_dir, max_work_dir_size_bytes):
        self._work_dir = work_dir
        self._max_work_dir_size_bytes = max_work_dir_size_bytes
        self._state_file = os.path.join(work_dir, self.STATE_FILENAME)
        self._lock = threading.Lock()
        self._stop_cleanup = False
        self._cleanup_thread = None
        self._cleanup_sleep_secs = 10
        self._dependencies = {}
        self._paths = set()

        if os.path.exists(self._state_file):
            self._load_state()
        else:
            remove_path(work_dir)
            os.makedirs(work_dir, 0770)
            self._save_state()

    def _load_state(self):
        with open(self._state_file, 'r') as f:
            loaded_state = json.loads(f.read())

        # Initialize self._dependencies.
        for dependency in loaded_state:
            dep = self._dependencies[tuple(dependency['target'])] = (
                Dependency.load(dependency, self._lock))
            self._paths.add(dep.path)

        # Remove paths that aren't complete (e.g. interrupted downloads and runs).
        for path in set(os.listdir(self._work_dir)) - self._paths - \
                {DependencyManager.STATE_FILENAME, DockerImageManager.STATE_FILENAME}:
            remove_path(os.path.join(self._work_dir, path))

    def _save_state(self):
        """
        Should be called with the lock held.
        """
        state = []
        for target, dependency in self._dependencies.iteritems():
            if not dependency.downloading:
                dumped_dependency = dependency.dump()
                dumped_dependency['target'] = target
                state.append(dumped_dependency)

        with open(self._state_file, 'w') as f:
            f.write(json.dumps(state))

    def start_cleanup_thread(self):
        self._cleanup_thread = threading.Thread(target=DependencyManager._do_cleanup, args=[self])
        self._cleanup_thread.start()

    def _do_cleanup(self):
        while not self._should_stop_cleanup():
            while True:
                # If the total size of all dependencies exceeds
                # self._max_work_dir_size_bytes, remove the oldest unused
                # dependency. Otherwise, break out of the loop.
                total_size_bytes = 0
                first_used_time = float('inf')
                first_used_target = None
                self._lock.acquire()
                for target, dependency in self._dependencies.items():
                    if dependency.downloading:
                        continue

                    # We compute the size of dependencies here to keep the code
                    # that adds new bundles to the dependency manager simpler.
                    if dependency.size_bytes is None:
                        self._lock.release()
                        size_bytes = get_path_size(os.path.join(self._work_dir,
                                                                dependency.path))
                        self._lock.acquire()
                        dependency.size_bytes = size_bytes
                        self._save_state()

                    total_size_bytes += dependency.size_bytes
                    if (not dependency.has_children() and
                        dependency.last_used < first_used_time):
                        first_used_time = dependency.last_used
                        first_used_target = target
                self._lock.release()

                if (total_size_bytes > self._max_work_dir_size_bytes and
                    first_used_target is not None):
                    with self._lock:
                        dependency = self._dependencies[first_used_target]
                        if dependency.has_children():
                            # Since we released the lock there could be new
                            # children.
                            continue
                        del self._dependencies[first_used_target]
                        self._paths.remove(dependency.path)
                        self._save_state()
                        remove_path(os.path.join(self._work_dir, dependency.path))
                else:
                    break

            # Sleep for 10 seconds, allowing interruptions every second.
            for _ in xrange(0, self._cleanup_sleep_secs):
                time.sleep(1)
                if self._should_stop_cleanup():
                    break

    def stop_cleanup_thread(self):
        with self._lock:
            self._stop_cleanup = True
        self._cleanup_thread.join()

    def _should_stop_cleanup(self):
        with self._lock:
            return self._stop_cleanup

    def dependencies(self):
        """
        Returns tuple of UUID, path of all the dependencies stored on the worker.
        """
        with self._lock:
            return self._dependencies.keys()

    def add_dependency(self, parent_uuid, parent_path, uuid):
        """
        Reports that the bundle with UUID uuid is starting to run and
        has the path parent_path of bundle with UUID parent_uuid as a
        dependency.

        Returns a tuple containing the path to the dependency and whether
        the dependency needs to be downloaded. finish_download should be called
        once the download is finished, whether successful or not.

        Note that if multiple runs need to download the same dependency at the
        same time, add_dependency will return True for only one of them, and
        will block the others until the download has finished.
        """
        target = (parent_uuid, parent_path)
        with self._lock:
            while True:
                if target in self._dependencies:
                    dependency = self._dependencies[target]
                    if dependency.downloading:
                        # Wait for download to finish. Then, check again (i.e.
                        # go through the loop again) just in case the download
                        # failed.
                        #
                        # Note, wait releases self._lock. However, since the
                        # condition uses self._lock when wait returns the thread
                        # will again hold self._lock.
                        dependency.wait_on_download()
                    else:
                        # Already downloaded.
                        dependency.add_child(uuid)
                        return os.path.join(self._work_dir, dependency.path), False
                else:
                    if parent_path:
                        path = os.path.join(parent_uuid, parent_path)
                    else:
                        path = parent_uuid
                    path = path.replace(os.path.sep, '_')

                    # You could have a conflict between, for example a/b_c and
                    # a_b/c. We have to avoid those.
                    while path in self._paths:
                        path = path + '_'

                    dependency = self._dependencies[target] = (
                        Dependency(path, True, self._lock))
                    self._paths.add(path)
                    dependency.add_child(uuid)
                    return os.path.join(self._work_dir, path), True

    def finish_download(self, uuid, path, success):
        """
        Reports that the download of dependency with UUID uuid and path path
        has finished.
        """
        target = (uuid, path)
        with self._lock:
            dependency = self._dependencies[target]
            if success:
                dependency.finish_download()
                self._save_state()
            else:
                del self._dependencies[target]
                self._paths.remove(dependency.path)

            # All threads currently waiting for the download would receive the
            # notification. However, they would unblock only as soon as they are
            # able to grab self._lock.
            dependency.notify_download_finished()

    def remove_dependency(self, parent_uuid, parent_path, uuid):
        """
        Reports that the bundle with UUID uuid has finished running and
        no longer needs the path parent_path of bundle with UUID parent_uuid as
        a dependency.
        """
        target = (parent_uuid, parent_path)
        with self._lock:
            if target in self._dependencies:
                self._dependencies[target].remove_child(uuid)
                self._save_state()

    def get_run_path(self, uuid):
        return os.path.join(self._work_dir, uuid)

    def finish_run(self, uuid):
        """
        Reports that the bundle with UUID can now be used by other running
        bundles as a dependency.
        """
        target = (uuid, '')
        with self._lock:
            self._dependencies[target] = Dependency(uuid, False, self._lock)
            self._paths.add(uuid)
            self._save_state()


class Dependency(object):
    """
    Keeps track of the state of a single dependency.
    """
    def __init__(self, path, downloading, lock):
        self.path = path
        self.downloading = downloading
        self._download_condition = threading.Condition(lock)
        self.size_bytes = None
        self._children = set()
        self.last_used = time.time()

    @staticmethod
    def load(dumped_dependency, lock):
        dependency = Dependency(dumped_dependency['path'], False, lock)
        dependency.size_bytes = dumped_dependency['size_bytes']
        dependency.last_used = dumped_dependency['last_used']
        return dependency

    def dump(self):
        return {
            'path': self.path,
            'size_bytes': self.size_bytes,
            'last_used': self.last_used,
        }

    def add_child(self, uuid):
        self._children.add(uuid)

    def remove_child(self, uuid):
        if uuid in self._children:
            self._children.remove(uuid)

        self.last_used = time.time()

    def has_children(self):
        return bool(self._children)

    def wait_on_download(self):
        self._download_condition.wait()

    def notify_download_finished(self):
        self._download_condition.notify_all()

    def finish_download(self):
        self.downloading = False
        self.last_used = time.time()
