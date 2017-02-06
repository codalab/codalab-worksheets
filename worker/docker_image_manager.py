import threading
import os
import time
import traceback
import logging
import json

from formatting import size_str

logger = logging.getLogger(__name__)


class DockerImageManager(object):
    """
    Manages the Docker images available on the worker, and ensures that they
    do not fill up the disk.

    The last-used dates are only tracked for images on which
    `DockerImageManager.touch_image()` has been called. All other images will
    be left untouched by the cleanup thread.

    DependencyManager should be instantiated before DockerImageManager, to
    ensure that the work directory already exists.
    """
    STATE_FILENAME = 'images-state.json'

    def __init__(self, docker, work_dir, min_disk_free_bytes):
        """
        :param docker: DockerClient
        :param work_dir: worker scratch directory, where the state file lives
        :param min_disk_free_bytes: minimum bytes that should be free on the disk
        """
        self._docker = docker
        self._work_dir = work_dir
        self._cleanup_thread = None
        self._stop_cleanup = False
        self._lock = threading.Lock()
        self._last_used = None
        self._state_file = os.path.join(work_dir, self.STATE_FILENAME)
        self._min_disk_free_bytes = min_disk_free_bytes
        self._docker_root_dir = docker.get_root_dir()

        if os.path.exists(self._state_file):
            self._load_state()
        else:
            self._save_state()

    def _load_state(self):
        with open(self._state_file, 'r') as f:
            state = json.load(f)
        self._last_used = state['last_used']

    def _save_state(self):
        # In case we're initializing the state for the first time
        if self._last_used is None:
            self._last_used = {}
        state = {
            'last_used': self._last_used,
        }
        with open(self._state_file, 'w') as f:
            json.dump(state, f)

    def touch_image(self, image_id):
        """
        Update the last-used date of an image to be the current date.
        """
        # Writes must be exclusive with other reads and writes
        with self._lock:
            now = time.time()
            self._last_used[image_id] = now
            self._save_state()
        logger.debug('touched image %s at %f' % (image_id, now))

    def start_cleanup_thread(self):
        self._stop_cleanup = False
        self._cleanup_thread = threading.Thread(target=DockerImageManager._do_cleanup, args=[self])
        self._cleanup_thread.start()

    def stop_cleanup_thread(self):
        if self._cleanup_thread is None:
            return
        with self._lock:
            self._stop_cleanup = True
        self._cleanup_thread.join()
        self._cleanup_thread = None

    def _should_stop_cleanup(self):
        with self._lock:
            return self._stop_cleanup

    def _get_free_bytes(self):
        """
        Return bytes free on the disk containing the Docker images.
        """
        # FIXME: this doesn't work on worker nodes since don't have permissions on docker dir
        fd = os.open(self._docker_root_dir, os.O_RDONLY)
        stat = os.fstatvfs(fd)
        os.close(fd)
        return stat.f_bavail * stat.f_frsize

    def _do_cleanup(self):
        """
        Periodically clean up the oldest images when the disk is close to full.
        """
        logging.debug('image cleanup thread started')
        while not self._should_stop_cleanup():
            # Start disk usage reduction loop
            # Iteratively try to remove the oldest images until the disk usage
            # is within the limit.
            try:
                while True:
                    free_bytes = self._get_free_bytes()
                    if len(self._last_used) > 0 and free_bytes < self._min_disk_free_bytes:
                        logging.info('disk free: %s (min %s)',
                                     size_str(free_bytes),
                                     size_str(self._min_disk_free_bytes))
                        with self._lock:
                            # Pick the stalest image
                            image_id = min(self._last_used,
                                           key=lambda id_: self._last_used[id_])

                            # Remove that image
                            self._docker.remove_image(image_id)
                            del self._last_used[image_id]
                            self._save_state()
                            logging.info('removed image %s', image_id)
                    else:
                        # Break out of the loop when disk usage is normal.
                        break
            except Exception:
                # Print the error then go to sleep and try again next time.
                traceback.print_exc()

            # Allow chance to be interrupted before going to sleep.
            if self._should_stop_cleanup():
                break

            time.sleep(1)
