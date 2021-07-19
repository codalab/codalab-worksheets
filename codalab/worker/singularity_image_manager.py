import logging
import os

from codalab.worker.file_util import sha256
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
        self.image_folder = image_folder

    def _cleanup(self):
        files = os.listdir(self.image_folder)
        for f in files:
            os.remove(os.path.join(self.image_folder, f))

    def get(self, image_spec: str):
        """
        image_spec is in singularity format:
        - Docker: 'docker://<image>'
        - Sylabs Cloud hub: 'library://<image>'
        - Singularity Hub (deprecated): 'shub://<image>'
        Images will be assumed to be docker if no prefix is included.
        """
        if "://" not in image_spec:
            # prefix docker, no scheme exists
            image_spec = "docker://" + image_spec
        _, rest = image_spec.split("://")
        if ":" not in rest:
            image_spec += ":latest"
        return super().get(image_spec)

    def _download(self, image_spec: str):
        logger.debug('Downloading image %s', image_spec)
        try:
            # stream=True for singularity doesnt return progress or anything really - for now no progress
            self._downloading[image_spec]['message'] = "Starting download"
            img = Client.pull(image_spec, pull_folder=self.image_folder)
            logger.debug('Download for image %s complete to %s', image_spec, img)
            self._downloading[image_spec]['success'] = True
            self._downloading[image_spec]['message'] = "Downloaded image"
        except Exception as ex:
            logger.debug('Download for Singularity image %s failed: %s', image_spec, ex)
            self._downloading[image_spec]['success'] = False
            self._downloading[image_spec]['message'] = "Can't download image: {}".format(ex)

    def _image_availability_state(
        self, image_spec: str, success_message: str, failure_message: str
    ) -> ImageAvailabilityState:
        """
        Returns the state of a specified image on the codalab singularity image folder.
        Should be called after the image is said to be downloaded.
        Assumes image_spec has a version associated (in format image:version).
        Args:
            image_spec: image specification that has a version associated (format image:version)
            success_message: message to store in the ImageAvailabilityState if the image exists
            failure_message: message to store in the ImageAvailabilityState if the image does not exist

        Returns:

        """
        # the singularity image is stored in the format of <image name>_<version>,sif
        image_path = os.path.join(self.image_folder, "{}.sif".format(image_spec.split(":")))
        if os.path.isfile(image_path):
            # for singularity, the digest of an image is just the sha256 hash of the image file.
            return ImageAvailabilityState(
                digest=sha256(image_path), stage=DependencyStage.READY, message=success_message
            )
        return ImageAvailabilityState(
            digest=None,
            stage=DependencyStage.FAILED,
            message=failure_message % "image file {} should exist but does not".format(image_path),
        )

    def _image_size_without_pulling(self, image_spec: str):
        """
        no-op.
        As of right now, neither singularity hub or sylabs cloud hub support the querying of image
        sizes as dockerhub does. Until then, this will remain a no-op.
        """
        return None
