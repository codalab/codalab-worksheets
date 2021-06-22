import logging
import os
import threading

from codalab.lib.formatting import size_str
from codalab.worker.image_manager import ImageAvailabilityState
from codalab.worker.fsm import DependencyStage
from codalab.worker.image_manager import ImageManager
from codalab.worker.singularity_utils import get_singularity_container_size
from spython.main import Client

logger = logging.getLogger(__name__)


class SingularityImageManager(ImageManager):
    def __init__(self, max_image_size: int, max_image_cache_size: int, image_folder: str):
        """

        Args:
            max_image_size: Maximum image size of any pulled singularity image.
            max_image_cache_size: The maximum size of the image cache (folder images are pulled into)
            image_folder: folder images are pulled into.
        """
        super().__init__(max_image_size, max_image_cache_size)
        if not os.path.isdir(image_folder):
            raise ValueError("image_folder %s is not a directory" % image_folder)
        if not os.path.isabs(image_folder):
            raise ValueError("image_folder %s needs to be an absolute path" % image_folder)
        self.image_folder = image_folder

    def cleanup(self):
        files = os.listdir(self.image_folder)
        for f in files:
            os.remove(os.path.join(self.image_folder, f))

    def get(self, image_spec):
        """
        This will for now be built without caching - that's a little more complicated.
        image_spec is in singularity format:
        - Docker: 'docker://<image>'
        - Sylabs Cloud hub: 'library://<image>'
        - Singularity Hub (deprecated): 'shub://<image>'
        """
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
                        status = ImageAvailabilityState(
                            digest=None, stage=DependencyStage.READY, message="Image ready",
                        )
                    else:
                        # in the future, we would check if the image exists locally before erroring,
                        # but for now, if we cannot download from singuarity/docker/oci hub, just error
                        status = ImageAvailabilityState(
                            digest=None,
                            stage=DependencyStage.READY,
                            # the error should default to something
                            message="Image could not be downloaded: %s"
                            % self._downloading[image_spec]['message'],
                        )
                    self._downloading.remove(image_spec)
                    return status
        else:

            # Check docker image size before pulling from Docker Hub.
            # Do not download images larger than self._max_image_size
            # Download images if size cannot be obtained
            if self._max_image_size:
                try:
                    image_size_bytes = get_singularity_container_size(image_spec)
                    if image_size_bytes is None:
                        failure_msg = (
                            "Unable to find image: {} "
                            "Skipping image size precheck.".format(image_spec)
                        )
                        logger.info(failure_msg)
                    elif image_size_bytes > self._max_image_size:
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
                    failure_msg = "Cannot fetch image size before pulling image: {} from Container Registry: {}.".format(
                        image_spec, ex
                    )
                    logger.error(failure_msg)
                    return ImageAvailabilityState(
                        digest=None, stage=DependencyStage.FAILED, message=failure_msg
                    )

            self._downloading.add_if_new(image_spec, threading.Thread(target=self._download, args=[]))
            return ImageAvailabilityState(
                digest=None,
                stage=DependencyStage.DOWNLOADING,
                message=self._downloading[image_spec]['status'],
            )

    def _download(self, image_spec):
        logger.debug('Downloading image %s', image_spec)
        try:
            # stream=True for singularity doesnt return progress or anything really - for now no progress
            self._downloading[image_spec]['message'] = "Starting Download"
            img, puller = Client.pull(
                image_spec, pull_folder=self.image_folder, stream=True
            )
            # maybe add img to a list and in cleanup rm the files in the list
            logger.debug('Download for image %s complete to %s', image_spec, img)
            self._downloading[image_spec]['success'] = True
            self._downloading[image_spec]['message'] = "Downloaded image"
        except Exception as ex:
            logger.debug('Download for Singularity image %s failed: %s', image_spec, ex)
            self._downloading[image_spec]['success'] = False
            self._downloading[image_spec]['message'] = "Can't download image: {}".format(ex)

    def _image_availability_state(self, image_spec, success_message, failure_message) -> ImageAvailabilityState:
        """
        We know that the images are stored in a folder we know about and know exists
        But, how do we do versioning?
        golang:latest stored at one point in time will not be the same as golang:latest later, but if the user
        requests that, the latest one will not be stored.
        Maybe if the image_spec is the latest we pull and overwrite regardless?
        Args:
            image_spec:
            success_message:
            failure_message:

        Returns:

        """