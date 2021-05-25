import logging
import threading
import time
import traceback

from codalab.worker.docker_image_manager import ImageAvailabilityState
from codalab.worker.worker_thread import ThreadDict

logger = logging.getLogger(__name__)

class ImageManager:
    """
    ImageManager is the base interface for all image managers (docker, singularity, etc.)
    No actual logic is encapsulated in this class.
    """

    def __init__(self, max_image_size: int, max_image_cache_size: int):
        self._max_image_size = max_image_size
        self._max_image_cache_size = max_image_cache_size
        self._stop = False
        self._sleep_secs = 10
        self._cleanup_thread = None
        self._downloading = ThreadDict(
            fields={'success': False, 'status': 'Download starting'}, lock=True
        )

    def start(self) -> None:
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
        raise NotImplementedError

    # should be implemented in subclasses
    def cleanup(self):
        raise NotImplementedError
