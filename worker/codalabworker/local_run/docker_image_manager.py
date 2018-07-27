from collections import namedtuple
import threading
import time
import traceback
import logging

from codalabworker.formatting import size_str
from codalabworker.docker_client import DockerException
from codalabworker.fsm import (
    BaseDependencyManager,
    DependencyStage,
    StateTransitioner,
)
from codalabworker.state_committer import JsonStateCommitter
from codalabworker.worker_thread import ThreadDict

logger = logging.getLogger(__name__)

DockerImageState = namedtuple('DockerImageState', 'stage digest killed last_used message')


class DockerImageManager(StateTransitioner, BaseDependencyManager):

    def __init__(self, docker, commit_file, max_images_bytes, max_age_failed_seconds=60):
        super(DockerImageManager, self).__init__()
        self.add_transition(DependencyStage.DOWNLOADING, self._transition_from_DOWNLOADING)
        self.add_terminal(DependencyStage.READY)
        self.add_terminal(DependencyStage.FAILED)

        self._state_committer = JsonStateCommitter(commit_file)
        self._docker = docker
        self._images = {}  # digest -> DockerImageState
        # digest -> {'thread': Thread, 'success': bool}
        self._downloading = ThreadDict(fields={'success': False})
        self._max_images_bytes = max_images_bytes
        self._max_age_failed_seconds = max_age_failed_seconds
        self._lock = threading.RLock()

        self._stop = False
        self._sleep_secs = 10
        self._main_thread = None

        self._load_state()

    def _save_state(self):
        with self._lock:
            self._state_committer.commit(self._images)

    def _load_state(self):
        with self._lock:
            self._images = self._state_committer.load()

    def start(self):
        def loop(self):
            while not self._stop:
                try:
                    self._process_images()
                    self._save_state()
                    self._cleanup()
                    self._save_state()
                except Exception:
                    traceback.print_exc()
                time.sleep(self._sleep_secs)
        self._main_thread = threading.Thread(target=loop, args=[self])
        self._main_thread.start()

    def stop(self):
        logger.info("Stopping docker image manager")
        self._stop = True
        self._downloading.stop()
        self._main_thread.join()
        logger.info("Stopped docker image manager. Exiting. Exiting.")

    def _process_images(self):
        """
        Transition image states. Also remove FAILED states that are too old.
        """
        now = time.time()
        with self._lock:
            for digest, state in self._images.items():
                if state.stage == DependencyStage.FAILED:
                    if now - state.last_used > self._max_age_failed_seconds:
                        del self._images[digest]

            for digest, state in self._images.items():
                self._images[digest] = self.transition(state)

    def _cleanup(self):
        """
        If Docker's disk usage is higher than the quota given at initialization, clean up old images
        until disk usage is under the quota again. Remove images in this order:
            - FAILED images, least recently touched first
            - READY images, least recently touched first
        For now, if all the images are DOWNLOADING, log but don't delete. Images will be deleted after
        download completes

        """
        while True:
            total_bytes, reclaimable_bytes = self._docker.get_disk_usage()
            if self._max_images_bytes and total_bytes > self._max_images_bytes and len(self._images) > 0 and reclaimable_bytes > 0:
                logger.debug('Docker images disk usage: %s (max %s)',
                             size_str(total_bytes),
                             size_str(self._max_images_bytes))
                with self._lock:
                    failed_images = {digest: image for digest, image in self._images.items() if image.stage == DependencyStage.FAILED}
                    ready_images = {digest: image for digest, image in self._images.items() if image.stage == DependencyStage.READY}
                    if failed_images:
                        digest_to_remove = min(failed_images, key=lambda i: failed_images[i].last_used)
                    elif ready_images:
                        digest_to_remove = min(ready_images, key=lambda i: ready_images[i].last_used)
                    else:
                        logger.debug('Docker image manager disk quota is full but there are only downloading images. Waiting for downloads to finishe before cleanup.')
                        break
                    try:
                        self._docker.remove_image(digest_to_remove)
                    finally:
                        del self._images[digest_to_remove]
            else:
                break

    def remove(self, digest):
        """
        Set the image to be removed. This will fail the active downloads
        """
        if not self.has(digest):
            return
        with self._lock:
            self._images[digest] = self._images[digest]._replace(killed=True)

    def has(self, digest):
        with self._lock:
            return (digest in self._images)

    def get(self, digest):
        now = time.time()
        with self._lock:
            if not self.has(digest):
                self._images[digest] = DockerImageState(stage=DependencyStage.DOWNLOADING, digest=digest, killed=False, last_used=now, message="")

            # update last_used as long as it isn't in FAILED
            if self._images[digest].stage != DependencyStage.FAILED:
                self._images[digest] = self._images[digest]._replace(last_used=now)
            return self._images[digest]

    @property
    def all_images(self):
        with self._lock:
            return list(self._images.keys())

    def _transition_from_DOWNLOADING(self, image_state):
        def download():
            def update_status_message_and_check_killed(status_message):
                with self._lock:
                    image_state = self.get(digest)
                    if self._stop or image_state.killed:
                        return False  # should_resume = False
                    else:
                        self._images[digest] = image_state._replace(message=status_message)
                        return True  # should_resume = True

            try:
                self._docker.download_image(digest, update_status_message_and_check_killed)
                with self._lock:
                    self._downloading[digest]['success'] = True
                logger.debug('Finished downloading image %s', digest)
            except DockerException as err:
                with self._lock:
                    image_state = self.get(digest)
                    self._images[digest] = image_state._replace(message=str(err))

        digest = image_state.digest
        self._downloading.add_if_new(digest, threading.Thread(target=download, args=[]))

        if self._downloading[digest].is_alive():
            return image_state

        new_stage = DependencyStage.READY if self._downloading[digest]['success'] else DependencyStage.FAILED
        self._downloading.remove(digest)
        return image_state._replace(stage=new_stage)
