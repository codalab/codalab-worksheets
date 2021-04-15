from collections import namedtuple
import threading
import time
import traceback
import logging

import docker
from docker import DockerClient

from codalab.lib.telemetry_util import capture_exception, using_sentry
import codalab.worker.docker_utils as docker_utils

from .docker_utils import DEFAULT_DOCKER_TIMEOUT
from codalab.worker.fsm import DependencyStage
from codalab.worker.state_committer import JsonStateCommitter
from codalab.worker.worker_thread import ThreadDict
from codalab.lib.formatting import size_str


logger = logging.getLogger(__name__)

# Stores the download state of a Docker image (also includes the digest being pulled, digest string,
# DependencyStage and relevant status message from the download)
ImageAvailabilityState = namedtuple('ImageAvailabilityState', ['digest', 'stage', 'message'])
# Stores information relevant about caching about docker images
ImageCacheEntry = namedtuple(
    'ImageCacheEntry', ['id', 'digest', 'last_used', 'virtual_size', 'marginal_size']
)


class DockerImageManager:

    CACHE_TAG = 'codalab-image-cache/last-used'

    def __init__(self, commit_file, max_image_cache_size, max_image_size):
        """
        Initializes a DockerImageManager
        :param commit_file: String path to where the state file should be committed
        :param max_image_cache_size: Total size in bytes that the image cache can use
        :param max_image_size: Total size in bytes that the image can have
        """
        self._state_committer = JsonStateCommitter(commit_file)  # type: JsonStateCommitter
        self._docker = docker.from_env(timeout=DEFAULT_DOCKER_TIMEOUT)  # type: DockerClient
        self._downloading = ThreadDict(
            fields={'success': False, 'status': 'Download starting'}, lock=True
        )
        self._max_image_cache_size = max_image_cache_size
        self._max_image_size = max_image_size

        self._stop = False
        self._sleep_secs = 10
        self._cleanup_thread = None

    def start(self):
        logger.info("Starting docker image manager")

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

    def stop(self):
        logger.info("Stopping docker image manager")
        self._stop = True
        logger.debug("Stopping docker image manager: stop the downloads threads")
        self._downloading.stop()
        if self._cleanup_thread:
            logger.debug("Stopping docker image manager: stop the cleanup thread")
            self._cleanup_thread.join()
        logger.info("Stopped docker image manager")

    def _get_cache_use(self):
        return sum(
            float(image.attrs['VirtualSize']) for image in self._docker.images.list(self.CACHE_TAG)
        )

    def _cleanup(self):
        """
        Prunes the image cache for runs.
        1. Only care about images we (this DockerImageManager) downloaded and know about.
        2. We also try to prune any dangling docker images on the system.
        3. We use sum of VirtualSize's, which is an upper bound on the disk use of our images:
            in case no images share any intermediate layers, this will be the real disk use,
            however if images share layers, the virtual size will count that layer's size for each
            image that uses it, even though it's stored only once in the disk. The 'Size' field
            accounts for the marginal size each image adds on top of the shared layers, but summing
            those is not accurate either since the shared base layers need to be counted once to get
            the total size. (i.e. summing marginal sizes would give us a lower bound on the total disk
            use of images). Calling df gives us an accurate disk use of ALL the images on the machine
            but because of (1) we don't want to use that.
        """
        # Sort the image cache in LRU order
        def last_used(image):
            for tag in image.tags:
                if tag.split(":")[0] == self.CACHE_TAG:
                    return float(tag.split(":")[1])

        cache_use = self._get_cache_use()
        if cache_use > self._max_image_cache_size:
            logger.info(
                'Disk use (%s) > max cache size (%s): starting image pruning',
                cache_use,
                self._max_image_cache_size,
            )
            all_images = self._docker.images.list(self.CACHE_TAG)
            all_images_sorted = sorted(all_images, key=last_used)
            logger.info("Cached docker images: {}".format(all_images_sorted))
            for image in all_images_sorted:
                # We re-list all the images to get an updated total size since we may have deleted some
                cache_use = self._get_cache_use()
                if cache_use > self._max_image_cache_size:
                    image_tag = (
                        image.attrs['RepoTags'][-1]
                        if len(image.attrs['RepoTags']) > 0
                        else '<none>'
                    )
                    logger.info(
                        'Disk use (%s) > max cache size (%s), pruning image: %s',
                        cache_use,
                        self._max_image_cache_size,
                        image_tag,
                    )
                    try:
                        self._docker.images.remove(image.id, force=True)
                    except docker.errors.APIError as err:
                        # Two types of 409 Client Error can be thrown here:
                        # 1. 409 Client Error: Conflict ("conflict: unable to delete <image_id> (cannot be forced)")
                        #   This happens when an image either has a running container or has multiple child dependents.
                        # 2. 409 Client Error: Conflict ("conflict: unable to delete <image_id> (must be forced)")
                        #   This happens when an image is referenced in multiple repositories.
                        # We can only remove images in 2rd case using force=True, but not the 1st case. So after we
                        # try to remove the image using force=True, if it failed, then this indicates that we were
                        # trying to remove images in 1st case. Since we can't do much for images in 1st case, we
                        # just continue with our lives, hoping it will get deleted once it's no longer in use and
                        # the cache becomes full again
                        logger.warning(
                            "Cannot forcibly remove image %s from cache: %s", image_tag, err
                        )
            logger.debug("Stopping docker image manager cleanup")

    def get(self, image_spec):
        """
        Always request the newest docker image from Dockerhub if it's not in downloading thread and return the current
        downloading status(READY, FAILED, or DOWNLOADING).
        When the requested image in the following states:
        1. If it's not available on the platform, we download the image and return DOWNLOADING status.
        2. If another thread is actively downloading it, we return DOWNLOADING status.
        3. If another thread was downloading it but not active by the time the request was sent, we return the following status:
            * READY if the image was downloaded successfully.
            * FAILED if the image wasn't able to be downloaded due to any reason.
        :param image_spec: Repo image_spec of docker image being requested
        :returns: A DockerAvailabilityState object with the state of the docker image
        """

        def image_availability_state(image_spec, success_message, failure_message):
            """
            Try to get the image specified by image_spec from host machine.
            Return ImageAvailabilityState.
            """
            try:
                image = self._docker.images.get(image_spec)
                digests = image.attrs.get('RepoDigests', [image_spec])
                digest = digests[0] if len(digests) > 0 else None
                new_timestamp = str(time.time())
                image.tag(self.CACHE_TAG, tag=new_timestamp)
                for tag in image.tags:
                    tag_label, timestamp = tag.split(":")
                    # remove any other timestamp but not the current one
                    if tag_label == self.CACHE_TAG and timestamp != new_timestamp:
                        try:
                            self._docker.images.remove(tag)
                        except docker.errors.NotFound as err:
                            # It's possible that we get a 404 not found error here when removing the image,
                            # since another worker on the same system has already done so. We just
                            # ignore this 404, since any extraneous tags will be removed during the next iteration.
                            logger.warning(
                                "Attempted to remove image %s from cache, but image was not found: %s",
                                tag,
                                err,
                            )

                return ImageAvailabilityState(
                    digest=digest, stage=DependencyStage.READY, message=success_message
                )
            except Exception as ex:
                if using_sentry():
                    capture_exception()
                return ImageAvailabilityState(
                    digest=None, stage=DependencyStage.FAILED, message=failure_message % ex
                )

        if ':' not in image_spec:
            # Both digests and repo:tag kind of specs include the : character. The only case without it is when
            # a repo is specified without a tag (like 'latest')
            # When this is the case, different images API methods act differently:
            # - pull pulls all tags of the image
            # - get tries to get `latest` by default
            # That means if someone requests a docker image without a tag, and the image does not have a latest
            # tag pushed to Dockerhub, pull will succeed since it will pull all other tags, but later get calls
            # will fail since the `latest` tag won't be found on the system.
            # We don't want to assume what tag the user wanted so we want the pull step to fail if no tag is specified
            # and there's no latest tag on dockerhub.
            # Hence, we append the latest tag to the image spec if there's no tag specified otherwise at the very beginning
            image_spec += ':latest'
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
                            status = image_availability_state(
                                image_spec,
                                success_message='Image ready',
                                failure_message='Image {} was downloaded successfully, '
                                'but it cannot be found locally due to unhandled error %s'.format(
                                    image_spec
                                ),
                            )
                        else:
                            status = image_availability_state(
                                image_spec,
                                success_message='Image {} can not be downloaded from DockerHub '
                                'but it is found locally'.format(image_spec),
                                failure_message=self._downloading[image_spec]['message'] + ": %s",
                            )
                        self._downloading.remove(image_spec)
                        return status
            else:

                def download():
                    logger.debug('Downloading Docker image %s', image_spec)
                    try:
                        self._docker.images.pull(image_spec)
                        logger.debug('Download for Docker image %s complete', image_spec)
                        self._downloading[image_spec]['success'] = True
                        self._downloading[image_spec]['message'] = "Downloading image"
                    except (docker.errors.APIError, docker.errors.ImageNotFound) as ex:
                        logger.debug('Download for Docker image %s failed: %s', image_spec, ex)
                        self._downloading[image_spec]['success'] = False
                        self._downloading[image_spec][
                            'message'
                        ] = "Can't download image: {}".format(ex)

                # Check docker image size before pulling from Docker Hub.
                # Do not download images larger than self._max_image_size
                # Download images if size cannot be obtained
                if self._max_image_size:
                    try:
                        image_size_bytes = docker_utils.get_image_size_without_pulling(image_spec)
                        if image_size_bytes is None:
                            failure_msg = (
                                "Unable to find Docker image: {} from Docker HTTP Rest API V2. "
                                "Skipping Docker image size precheck.".format(image_spec)
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

                self._downloading.add_if_new(image_spec, threading.Thread(target=download, args=[]))
                return ImageAvailabilityState(
                    digest=None,
                    stage=DependencyStage.DOWNLOADING,
                    message=self._downloading[image_spec]['status'],
                )
        except Exception as ex:
            return ImageAvailabilityState(
                digest=None, stage=DependencyStage.FAILED, message=str(ex)
            )
