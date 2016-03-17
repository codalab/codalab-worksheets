import threading


class DependencyManager(object):
    """
    Manages the dependencies available on the worker, and ensures that no two
    runs download the same dependency at the same time.
    """
    # TODO(klopyrev): Delete dependencies if they are using too much disk space.
    # TODO(klopyrev): Allow downloading only specific subpaths within a
    #                 dependency.
    def __init__(self):
        self._lock = threading.Lock()
        self._dependencies = {}

    def dependencies(self):
        """
        Returns the UUIDs of all the dependencies stored on the worker.
        """
        with self._lock:
            return self._dependencies.keys()

    def add_dependee(self, uuid, dependee_uuid):
        """
        Reports that the bundle with UUID dependee_uuid is starting to run and
        has the bundle with UUID uuid as a dependency.

        Returns True if the dependency needs to be downloaded. finish_download
        should be called once the download is finished, whether successful or
        not.

        Note that if multiple runs need to download the same dependency at the
        same time, add_dependee will return True for only one of them, and will
        block the others until the download has finished.
        """
        with self._lock:
            while True:
                if uuid in self._dependencies:
                    dependency = self._dependencies[uuid]
                    if dependency.downloading:
                        # Wait for download to finish. Then, check again just
                        # in case the download failed.
                        #
                        # Note, wait releases self._lock. However, since the
                        # condition uses self._lock when wait returns the thread
                        # will again hold self._lock.
                        dependency.download_condition.wait()
                    else:
                        # Already downloaded.
                        dependency.dependees.add(dependee_uuid)
                        return False
                else:
                    dependency = self._dependencies[uuid] = (
                        DependencyState(self._lock))
                    dependency.downloading = True
                    dependency.dependees.add(dependee_uuid)
                    return True

    def finish_download(self, uuid, success):
        """
        Reports that the download of dependency with UUID uuid has finished.
        """
        with self._lock:
            dependency = self._dependencies[uuid]
            if success:
                dependency.downloading = False
            else:
                del self._dependencies[uuid]

            # All threads currently waiting for the download would receive the
            # notification. However, they would unblock only as soon as they are
            # able to grab self._lock.
            dependency.download_condition.notify_all()

    def remove_dependee(self, uuid, dependee_uuid):
        """
        Reports that the bundle with UUID dependee_uuid has finished running and
        no longer needs the bundle with UUID uuid as a dependency.
        """
        with self._lock:
            if uuid in self._dependencies:
                dependees = self._dependencies[uuid].dependees
                if dependee_uuid in dependees:
                    dependees.remove(dependee_uuid)

    def finish_run(self, uuid):
        """
        Reports that the bundle with UUID can now be used by other running
        bundles as a dependency.
        """
        with self._lock:
            dependency = self._dependencies[uuid] = DependencyState(self._lock)
            dependency.downloading = False


class DependencyState(object):
    """
    Keeps track of a single dependency, all runs that depend on the
    dependency and whether the dependency is downloading.
    """
    def __init__(self, lock):
        self.dependees = set()
        self.downloading = None
        self.download_condition = threading.Condition(lock)
