import logging
import threading
import time
import traceback
from collections import namedtuple
from typing import Optional

from codalab.lib.formatting import size_str
from codalab.worker.fsm import DependencyStage
from codalab.worker.worker_thread import ThreadDict

logger = logging.getLogger(__name__)

# Stores the download state of any image (also may includee the digest being pulled, digest string,
# DependencyStage and relevant status message from the download)
ImageAvailabilityState = namedtuple('ImageAvailabilityState', ['digest', 'stage', 'message'])


class ImageManager:
    """
    ImageManager is the base interface for all image managers (docker, singularity, etc.)
    This is an abstract class.
    An Image Manager manages instances of images, not dependent on the container runtime.
    It does this in the start and stop cleanup loops.
    Subclasses need to implement the get and _cleanup methods.
    """

    def __init__(self, max_image_size: int, max_image_cache_size: int):
        """
        Args:
            max_image_size: maximum image size in bytes of any given image
            max_image_cache_size: max number of bytes the image cache will hold at any given time.
        """
        self._max_image_size = max_image_size
        self._max_image_cache_size = max_image_cache_size
        self._stop = False
        self._sleep_secs = 10
        self._downloading = ThreadDict(
            fields={'success': False, 'status': 'Download starting'}, lock=True
        )
        self._cleanup_thread = None  # type: Optional[threading.Thread]

    def start(self) -> None:
        """
        Start the image manager.
        If the _max_cache_image_size argument is defined, the image manager will
            clean up the cache where images are held.

        Returns: None

        """
        logger.info("Starting image manager")
        if self._max_image_cache_size:

            def cleanup_loop(self):
                while not self._stop:
                    try:
                        self._cleanup()
                    except Exception:
                        traceback.print_exc()
                    time.sleep(self._sleep_secs)

            self._cleanup_thread = threading.Thread(target=cleanup_loop, args=[self])
            self._cleanup_thread.start()
        pass

    def stop(self) -> None:
        """
        stop will stop the running cleanup loop, and therefore, stop the image manager.
        Returns: None
        """
        logger.info("Stopping image manager")
        self._stop = True
        logger.debug("Stopping image manager: stop the downloads threads")
        self._downloading.stop()
        if self._cleanup_thread:
            logger.debug("Stopping image manager: stop the cleanup thread")
            self._cleanup_thread.join()
        logger.info("Stopped image manager")
        pass

    def get(self, image_spec: str) -> ImageAvailabilityState:
        """
        Always request the newest image from the cloud if it's not in downloading thread and return the current
        downloading status(READY, FAILED, or DOWNLOADING).
        When the requested image in the following states:
        1. If it's not available on the platform, we download the image and return DOWNLOADING status.
        2. If another thread is actively downloading it, we return DOWNLOADING status.
        3. If another thread was downloading it but not active by the time the request was sent, we return the following status:
            * READY if the image was downloaded successfully.
            * FAILED if the image wasn't able to be downloaded due to any reason.
        Args:
            image_spec: the image that the requester needs.
                The caller will need to determine the type of image they need before calling this function.
                It is usually safe to prefix the image with the type of image.
                For example, the docker image go would be docker://go

        Returns:
            ImageAvailabilityState of the image requested.
        """
        try:
            if image_spec in self._downloading:
                with self._downloading[image_spec]['lock']:
                    if self._downloading[image_spec].is_alive():
                        return ImageAvailabilityState(
                            digest=None,
                            stage=DependencyStage.DOWNLOADING,
                            message=self._downloading[image_spec]['status'],
                        )
                    else:
                        if self._downloading[image_spec]['success']:
                            status = self._image_availability_state(
                                image_spec,
                                success_message='Image ready',
                                failure_message='Image {} was downloaded successfully, '
                                'but it cannot be found locally due to unhandled error %s'.format(
                                    image_spec
                                ),
                            )
                        else:
                            status = self._image_availability_state(
                                image_spec,
                                success_message='Image {} can not be downloaded from the cloud '
                                'but it is found locally'.format(image_spec),
                                failure_message=self._downloading[image_spec]['message'] + ": %s",
                            )
                        self._downloading.remove(image_spec)
                        return status
            else:
                if self._max_image_size:
                    try:
                        try:
                            image_size_bytes = self._image_size_without_pulling(image_spec)
                        except NotImplementedError:
                            failure_msg = (
                                "Could not query size of {} from container runtime hub. "
                                "Skipping size precheck.".format(image_spec)
                            )
                            logger.info(failure_msg)
                            image_size_bytes = 0
                        if image_size_bytes > self._max_image_size:
                            failure_msg = (
                                "The size of "
                                + image_spec
                                + ": {} exceeds the maximum image size allowed {}.".format(
                                    size_str(image_size_bytes), size_str(self._max_image_size)
                                )
                            )
                            logger.error(failure_msg)
                            return ImageAvailabilityState(
                                digest=None, stage=DependencyStage.FAILED, message=failure_msg
                            )
                    except Exception as ex:
                        failure_msg = "Cannot fetch image size before pulling Docker image: {} from Docker Hub: {}.".format(
                            image_spec, ex
                        )
                        logger.error(failure_msg)
                        return ImageAvailabilityState(
                            digest=None, stage=DependencyStage.FAILED, message=failure_msg
                        )

            self._downloading.add_if_new(
                image_spec, threading.Thread(target=self._download, args=[image_spec])
            )
            return ImageAvailabilityState(
                digest=None,
                stage=DependencyStage.DOWNLOADING,
                message=self._downloading[image_spec]['status'],
            )
        except Exception as ex:
            logger.error(ex)
            return ImageAvailabilityState(
                digest=None, stage=DependencyStage.FAILED, message=str(ex)
            )

    def _cleanup(self):
        """
        Prune and clean up images in accordance with the image cache and
            how much space the images take up. The logic will vary per container runtime.
            Therefore, this function should be implemented in the subclasses.
        """
        raise NotImplementedError

    def _image_availability_state(
        self, image_spec: str, success_message: str, failure_message: str
    ) -> ImageAvailabilityState:
        """
        Try to get the image specified by image_spec from host machine.
        The message field of the ImageAvailabiltyState will contain success_message or failure_message and the respective
        stages of DependencyStage.READY or DependencyStage.FAILED depending on whether or not the image can be found locally.
        If the image can be found locally, the image is considered "ready", and "failed" if not.
        Args:
            image_spec: container-runtime specific spec
            success_message: message to return in the ImageAvailabilityState if the image was successfully found
            failure_message: message to return if there were any issues in getting the image.

        Returns: ImageAvailabilityState

        """
        raise NotImplementedError

    def _download(self, image_spec: str) -> None:
        """
        Download the container image from the cloud to the host machine.
        This function needs to be implemented specific to the container runtime.
        For instance:
            - _download's docker image implementation should pull from DockerHub.
            - _downloads's singularity image implementation shoudl pull from the singularity hub or
                sylab's cloud hub, based on the image scheme.
        This function will update the _downloading ThreadDict with the status and progress of the
            download.
        Args:
            image_spec: container-runtime specific image specification

        Returns: None

        """
        raise NotImplementedError

    def _image_size_without_pulling(self, image_spec: str):
        """
        Attempt to query the requested image's size, based on the container runtime.
        Args:
            image_spec: image specification

        Returns: None if the image size cannot be queried, or the size of the image(float).

        """
        raise NotImplementedError
