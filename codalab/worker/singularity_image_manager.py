import logging
import os

from codalab.worker.image_manager import ImageAvailabilityState
from codalab.worker.fsm import DependencyStage
from codalab.worker.image_manager import ImageManager
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

    def _cleanup(self):
        files = os.listdir(self.image_folder)
        for f in files:
            os.remove(os.path.join(self.image_folder, f))

    def get(self, image_spec):
        """
        image_spec is in singularity format:
        - Docker: 'docker://<image>'
        - Sylabs Cloud hub: 'library://<image>'
        - Singularity Hub (deprecated): 'shub://<image>'
        Images will be assumed to be docker if no prefix is included.
        """
        if len(image_spec.split("://")) <= 1:
            # prefix docker, no scheme exists
            image_spec = "docker://" + image_spec
        ImageManager.get(image_spec)

    def _download(self, image_spec):
        logger.debug('Downloading image %s', image_spec)
        try:
            # stream=True for singularity doesnt return progress or anything really - for now no progress
            self._downloading[image_spec]['message'] = "Starting Download"
            img, puller = Client.pull(image_spec, pull_folder=self.image_folder, stream=True)
            # maybe add img to a list and in cleanup rm the files in the list
            logger.debug('Download for image %s complete to %s', image_spec, img)
            self._downloading[image_spec]['success'] = True
            self._downloading[image_spec]['message'] = "Downloaded image"
        except Exception as ex:
            logger.debug('Download for Singularity image %s failed: %s', image_spec, ex)
            self._downloading[image_spec]['success'] = False
            self._downloading[image_spec]['message'] = "Can't download image: {}".format(ex)

    def _image_availability_state(
        self, image_spec, success_message, failure_message
    ) -> ImageAvailabilityState:
        """
        Returns the state of a said image on the codalab singularity image folder.
        Should be called after the image is said to be downloaded.
        Assumes image_spec has a version associated (in format image:version).
        Args:
            image_spec: image specification that has a version associated (format image:version)
            success_message: message to store in the ImageAvailabilityState if the image exists
            failure_message: message to store in the ImageAvailabilityState if the image does not exist

        Returns:

        """
        if len(image_spec.split(":")) <= 1:
            # error, we should have a version
            raise ValueError
        img = image_spec.split(":")[0]
        version = image_spec.split(":")[-1]
        hypofile = os.path.join(self.image_folder, img + "_" + version)
        if os.path.isfile(hypofile):
            return ImageAvailabilityState(digest=None, stage=DependencyStage.READY, message=success_message)
        return ImageAvailabilityState(
            digest=None,
            stage=DependencyStage.FAILED,
            message=failure_message % "image file {} should exist but does not".format(hypofile),
        )

    def _image_size_without_pulling(self, image_spec):
        """
        no-op.
        As of right now, neither singularity hub or sylabs cloud hub support the querying of image
        sizes as dockerhub does. Until then, this will remain a no-op.
        """
        return None
