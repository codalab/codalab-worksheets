from collections import namedtuple
import time
import logging

import docker
import requests
from docker import DockerClient

import codalab.worker.docker_utils as docker_utils

from .docker_utils import DEFAULT_DOCKER_TIMEOUT, URI_PREFIX
from codalab.worker.fsm import DependencyStage
from codalab.worker.state_committer import JsonStateCommitter
from .image_manager import ImageManager, ImageAvailabilityState
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Stores information relevant about caching about docker images
ImageCacheEntry = namedtuple(
    'ImageCacheEntry', ['id', 'digest', 'last_used', 'virtual_size', 'marginal_size']
)


class DockerImageManager(ImageManager):

    CACHE_TAG = 'codalab-image-cache/last-used'

    def __init__(self, commit_file: str, max_image_cache_size: int, max_image_size: int):
        """
        Initializes a DockerImageManager
        :param commit_file: String path to where the state file should be committed
        :param max_image_cache_size: Total size in bytes that the image cache can use
        :param max_image_size: Total size in bytes that the image can have
        """
        super().__init__(max_image_size, max_image_cache_size)
        self._state_committer = JsonStateCommitter(commit_file)  # type: JsonStateCommitter
        self._docker = docker.from_env(timeout=DEFAULT_DOCKER_TIMEOUT)  # type: DockerClient

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

    def get(self, image_spec: str):
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
            # Hence, we append the latest tag to the image spec
            # if there's no tag specified otherwise at the very beginning
            image_spec += ':latest'
        return super().get(image_spec)

    def _download(self, image_spec: str) -> None:
        """
        Download the container image from DockerHub to the host machine.
        Args:
            image_spec: docker image (just image, no prefix docker://)

        Returns: None
        """
        logger.debug('Downloading Docker image %s', image_spec)
        try:
            for line in self._docker.api.pull(image_spec, stream=True, decode=True):
                if line['status'] == 'Downloading' or line['status'] == 'Extracting':
                    progress = docker_utils.parse_image_progress(line)
                    self._downloading[image_spec]['status'] = '%s %s' % (line['status'], progress)
                else:
                    self._downloading[image_spec]['status'] = ''

            logger.debug('Download for Docker image %s complete', image_spec)
            self._downloading[image_spec]['success'] = True
            self._downloading[image_spec]['message'] = "Downloading image"
        except (docker.errors.APIError, docker.errors.ImageNotFound) as ex:
            logger.debug('Download for Docker image %s failed: %s', image_spec, ex)
            self._downloading[image_spec]['success'] = False
            self._downloading[image_spec]['message'] = "Can't download image: {}".format(ex)

    @docker_utils.wrap_exception('Unable to get image size without pulling from Docker Hub')
    def _image_size_without_pulling(self, image_spec: str):
        """
        Get the compressed size of a docker image without pulling it from Docker Hub. Note that since docker-py doesn't
        report the accurate compressed image size, e.g. the size reported from the RegistryData object, we then switch
        to use Docker Registry HTTP API V2
        :param image_spec: image_spec can have two formats as follows:
                1. "repo:tag": 'codalab/default-cpu:latest'
                2. "repo@digest": studyfang/hotpotqa@sha256:f0ee6bc3b8deefa6bdcbb56e42ec97b498befbbca405a630b9ad80125dc65857
        :return: 1. when fetching from Docker rest API V2 succeeded, return the compressed image size in bytes
                 2. when fetching from Docker rest API V2 failed, return None
        """
        logger.info("Downloading tag information for {}".format(image_spec))

        # Both types of image_spec have the ':' character. The '@' character is unique in the type 1.
        image_tag = None
        image_digest = None
        if '@' in image_spec:
            image_name, image_digest = image_spec.split('@')
        else:
            image_name, image_tag = image_spec.split(":")
        # Example URL:
        # 1. image with namespace: https://hub.docker.com/v2/repositories/<namespace>/<image_name>/tags/?page=<page_number>
        #       e.g. https://hub.docker.com/v2/repositories/codalab/default-cpu/tags/?page=1
        # 2. image without namespace: https://hub.docker.com/v2/repositories/library/<image_name>/tags/?page=<page_number>
        #       e.g. https://hub.docker.com/v2/repositories/library/ubuntu/tags/?page=1
        # Each page will return at most 10 tags
        # URI prefix of an image without namespace will be adjusted to https://hub.docker.com/v2/repositories/library
        uri_prefix_adjusted = URI_PREFIX + '/library/' if '/' not in image_name else URI_PREFIX
        request = uri_prefix_adjusted + image_name + '/tags/?page='
        image_size_bytes = None
        page_number = 1

        requests_session = requests.Session()
        # Retry 5 times, sleeping for [0.1s, 0.2s, 0.4s, ...] between retries.
        retries = Retry(
            total=5, backoff_factor=0.1, status_forcelist=[413, 429, 500, 502, 503, 504]
        )
        requests_session.mount('https://', HTTPAdapter(max_retries=retries))

        while True:
            response = requests_session.get(url=request + str(page_number))
            data = response.json()
            if len(data['results']) == 0:
                break
            # Get the size information from the matched image
            if image_tag:
                for result in data['results']:
                    if result['name'] == image_tag:
                        image_size_bytes = result['full_size']
                        return image_size_bytes
            if image_digest:
                for result in data['results']:
                    for image in result['images']:
                        if image_digest in image['digest']:
                            image_size_bytes = result['full_size']
                            return image_size_bytes

            page_number += 1

        return image_size_bytes

    def _image_availability_state(
        self, image_spec: str, success_message: str, failure_message: str
    ) -> ImageAvailabilityState:
        """
        Try to get the image specified by image_spec from host machine.
        Args:
            image_spec of the image
            success_message to be returned if the image is available
            failure_message to be returned if the image is not available (needs to be formatted with %s).
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
            logger.error(ex)
            return ImageAvailabilityState(
                digest=None, stage=DependencyStage.FAILED, message=failure_message % ex
            )
