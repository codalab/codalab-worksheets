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

    def __init__(self, docker, work_dir, max_images_bytes):
        """
        :param docker: DockerClient
        :param work_dir: worker scratch directory, where the state file lives
        :param max_images_bytes: maximum bytes that images should use
        """
        self._docker = docker
        self._work_dir = work_dir
        self._cleanup_thread = None
        self._stop_cleanup = False
        self._lock = threading.Lock()
        self._last_used = {}
        self._state_file = os.path.join(work_dir, self.STATE_FILENAME)
        self._max_images_bytes = max_images_bytes

        if os.path.exists(self._state_file):
            self._load_state()
        else:
            # When using shared filesystem, work_dir might not exist yet
            if not os.path.exists(work_dir):
                os.makedirs(work_dir, 0770)
            self._save_state()

    def _load_state(self):
        with open(self._state_file, 'r') as f:
            state = json.load(f)
        self._last_used = state['last_used']

    def _save_state(self):
        # In case we're initializing the state for the first time
        state = {
            'last_used': self._last_used,
        }
        with open(self._state_file, 'w') as f:
            json.dump(state, f)

    def touch_image(self, digest):
        """
        Update the last-used date of an image to be the current date.
        """
        with self._lock:
            now = time.time()
            self._last_used[digest] = now
            self._save_state()
        logger.debug('touched image %s at %f', digest, now)

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

    def _remove_stalest_image(self):
        with self._lock:
            digest = min(self._last_used, key=lambda i: self._last_used[i])
            try:
                self._docker.remove_image(digest)
            finally:
                del self._last_used[digest]
                self._save_state()

    def _do_cleanup(self):
        """
        Periodically clean up the oldest images when the disk is close to full.
        """
        logger.info('Image cleanup thread started.')
        while not self._should_stop_cleanup():
            # Start disk usage reduction loop
            # Iteratively try to remove the oldest images until the disk usage
            # is within the limit.
            try:
                while True:
                    total_bytes, reclaimable_bytes = self._docker.get_disk_usage()
                    if total_bytes > self._max_images_bytes and len(self._last_used) > 0 and reclaimable_bytes > 0:
                        logger.debug('Docker images disk usage: %s (max %s)',
                                      size_str(total_bytes),
                                      size_str(self._max_images_bytes))
                        self._remove_stalest_image()
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
