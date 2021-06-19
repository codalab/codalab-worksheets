import logging
import threading
import time
import traceback
from collections import namedtuple

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
        Args:
            image_spec: the image that the requester needs.
                The caller will need to determine the type of image they need before calling this function.
                It is usually safe to prefix the image with the type of image.
                For example, the docker image go would be docker://go

        Returns:
            ImageAvailabilityState of the image requested.
        """
        raise NotImplementedError

    def _cleanup(self):
        """
        _cleanup should prune and clean up images in accordance with the image cache and
            how much space the images take up. The logic will vary per container runtime.
            Therefore, this function should be implemented in the subclasses.
        """
        raise NotImplementedError
