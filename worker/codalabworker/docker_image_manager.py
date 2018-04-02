from collections import namedtuple
import threading
import os
import time
import traceback
import logging
import json

from formatting import size_str
from synchronized import synchronized
from fsm import (
    BaseDependencyManager,
    JsonStateCommitter,
    BaseStateHandler,
)

logger = logging.getLogger(__name__)

DockerImageState = namedtuple('DockerImageState', 'status digest last_used message')

class DockerImageManager(BaseDependencyManager):

    def __init__(self, docker, state_committer, max_images_bytes):
        self._state_committer = state_committer
        self._docker = docker
        self._images = {} # digest -> DockerImageState
        self._max_images_bytes = max_images_bytes

        self._stop = False
        self._cleanup_sleep_secs = 10
        self._main_thread = None

    def _save_state(self):
        with synchronized(self):
            self._state_committer.commit(self._images)

    def _load_state(self):
        with synchronized(self):
            self._images = self._state_committer.load()
            self.reset()

    def run(self):
        def loop(self):
            while not self._stop:
                try:
                    self._process_images()
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

    def _process_images(self):
        with synchronized(self):
            for entry in self._images.keys():
                image_state = self._images[entry]
                self._images[entry] = self._transition_image_state(image_state)

    def _reset_image_state(self, image_state):
        status = image_state.status
        fns = [val for key, val in vars().items() if key == '_reset_image_state_from_' + status]
        return fns[0]

    def _transition_image_state(self, image_state):
        status = image_state.status
        fns = [val for key, val in vars().items() if key == '_transition_image_state_from_' + status]
        return fns[0]

    def reset(self):
        with synchronized(self):
            for entry in self._images.keys():
                image_state = self._images[entry]
                self._images[entry] = self._reset_image_state(image_state)

    def touch_image(self, digest): #TODO
        """
        Update the last-used date of an image to be the current date.
        """
        with synchronized(self):
            now = time.time()
            self._images[digest] = self._images[digest]._replace(last_used=now)
            self._save_state()
        logger.debug('Touched image digest=%s at %f', digest, now)

    def has(self, digest):
        with synchronized(self):
            return (digest in self._images)

    def get(self, digest):
        with synchronized(self):
            if not self.has(digest):
                self._images[digest] = DockerImageState(DependencyStatus.STARTING, digest, None, "")
            return self._images[digest]

    def list_all(self):
        with synchronized(self):
            return list(self._images.keys())

    def _reset_image_state_from_STARTING(self, image_state):
        return image_state

    def _transition_dependency_state_from_STARTING(self, image_state):
        return image_state._replace(status=RunStatus.DOWNLOADING)

    def _reset_dependency_state_from_DOWNLOADING(self, image_state):
        return image_state

    def _transition_dependency_state_from_DOWNLOADING(self, image_state):
        def download():
            def update_status_message_and_check_killed(status_message):
                with synchronized(self):
                    image_state = self.get(digest)
                    self._images[digest] = image_state._replace(message=status_message)
                #check_killed()

            try:
                self._docker.download_image(digest, update_status_message_and_check_killed)
                with synchronized(self):
                    self._downloading[digest]['success'] = True
            finally:
                logger.debug('Finished downloading image %s', digest) #TODO?

        digest = image_state.digest
        if digest not in self._downloading:
            self._downloading[digest] = {
                'thread': threading.Thread(target=download, args=[]),
                'success': False
            }
            self._downloading[digest]['thread'].start()

        if self._downloading[digest]['thread'].is_alive():
            return image_state

        success = self._downloading[digest]['success']
        del self._downloading[digest]
        if success:
            return image_state._replace(status=DependencyStatus.READY)
        else:
            return image_state._replace(status=DependencyStatus.FAILED)

    def _reset_dependency_state_from_READY(self, image_state):
        return image_state

    def _transition_dependency_state_from_READY(self, image_state):
        return image_state

    def _reset_dependency_state_from_FAILED(self, image_state):
        return image_state

    def _transition_dependency_state_from_FAILED(self, image_state):
        return image_state
