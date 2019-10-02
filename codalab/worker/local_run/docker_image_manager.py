from collections import namedtuple
import threading
import time
import traceback
import logging

import docker

from codalab.worker.fsm import DependencyStage
from codalab.worker.state_committer import JsonStateCommitter
from codalab.worker.worker_thread import ThreadDict

logger = logging.getLogger(__name__)

# Stores the download state of a Docker image (also includes the digest being pulled, digest string, DependencyStage and relevant status message from the download)
ImageAvailabilityState = namedtuple('ImageAvailabilityState', ['digest', 'stage', 'message'])
# Stores information relevant about caching about docker images
ImageCacheEntry = namedtuple(
    'ImageCacheEntry', ['id', 'digest', 'last_used', 'virtual_size', 'marginal_size']
)


class DockerImageManager:
    def __init__(self, commit_file, max_image_cache_size):
        """
        Initializes a DockerImageManager
        :param commit_file: String path to where the state file should be committed
        :param max_image_cache_size: Total size in bytes that the image cache can use
        """
        self._state_committer = JsonStateCommitter(commit_file)  # type: JsonStateCommitter
        self._docker = docker.from_env()  # type: DockerClient
        self._image_cache = {}  # type: Dict[str, ImageCacheEntry]
        self._downloading = ThreadDict(
            fields={'success': False, 'status': 'Download starting.'}, lock=True
        )
        self._max_image_cache_size = max_image_cache_size
        self._lock = threading.RLock()

        self._stop = False
        self._sleep_secs = 10
        self._cleanup_thread = None

        self._load_state()

    def _save_state(self):
        with self._lock:
            self._state_committer.commit(self._image_cache)

    def _load_state(self):
        with self._lock:
            self._image_cache = self._state_committer.load()

    def start(self):
        logger.info("Starting docker image manager")
        if self._max_image_cache_size:

            def cleanup_loop(self):
                while not self._stop:
                    try:
                        self._cleanup()
                        self._save_state()
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

    def _cleanup(self):
        """
        Prunes the image cache for runs.
        1. Only care about images we (this DockerImageManager) downloaded and know about
        2. We use sum of VirtualSize's, which is an upper bound on the disk use of our images:
            in case no images share any intermediate layers, this will be the real disk use,
            however if images share layers, the virtual size will count that layer's size for each
            image that uses it, even though it's stored only once in the disk. The 'Size' field
            accounts for the marginal size each image adds on top of the shared layers, but summing
            those is not accurate either since the shared base layers need to be counted once to get
            the total size. (i.e. summing marginal sizes would give us a lower bound on the total disk
            use of images). Calling df gives us an accurate disk use of ALL the images on the machine
            but because of (1) we don't want to use that.
        """
        while not self._stop:
            deletable_entries = set(self._image_cache.values())
            disk_use = sum(cache_entry.virtual_size for cache_entry in deletable_entries)
            while disk_use > self._max_image_cache_size:
                entry_to_remove = min(deletable_entries, key=lambda entry: entry.last_used)
                logger.info(
                    'Disk use (%s) > max cache size (%s), pruning image: %s',
                    disk_use,
                    self._max_image_cache_size,
                    entry_to_remove.digest,
                )
                try:
                    image_to_delete = self._docker.images.get(entry_to_remove.id)
                    tags_to_delete = image_to_delete.tags
                    for tag in tags_to_delete:
                        self._docker.images.remove(tag)
                    # if we successfully removed the image also remove its cache entry
                    del self._image_cache[entry_to_remove.digest]
                except docker.errors.NotFound:
                    # image doesn't exist anymore for some reason, stop tracking it
                    del self._image_cache[entry_to_remove.digest]
                except docker.errors.APIError as err:
                    # Maybe we can't delete this image because its container is still running
                    # (think a run that takes 4 days so this is the oldest image but still in use)
                    # In that case we just continue with our lives, hoping it will get deleted once
                    # it's no longer in use and the cache becomes full again
                    logger.error(
                        "Cannot remove image %s from cache: %s", entry_to_remove.digest, err
                    )
                deletable_entries.remove(entry_to_remove)
                disk_use = sum(entry.virtual_size for entry in deletable_entries)
        logger.debug("Stopping docker image manager cleanup")

    def get(self, image_spec):
        """
        Request the docker image, starting its download if it's not available on the system
        :param image_spec: Repo image_spec of docker image being requested
        :returns: A DockerAvailabilityState object with the state of the docker image
        """
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
            image = self._docker.images.get(image_spec)
            digests = image.attrs.get('RepoDigests', [image_spec])
            if len(digests) == 0:
                return ImageAvailabilityState(
                    digest=None,
                    stage=DependencyStage.FAILED,
                    message='No digest available for {}, probably because it was built locally; delete the Docker image on the worker and try again'.format(
                        image_spec
                    ),
                )
            digest = digests[0]
            with self._lock:
                self._image_cache[digest] = ImageCacheEntry(
                    id=image.id,
                    digest=digest,
                    last_used=time.time(),
                    virtual_size=image.attrs['VirtualSize'],
                    marginal_size=image.attrs['Size'],
                )
            # We can remove the download thread if it still exists
            if image_spec in self._downloading:
                self._downloading.remove(image_spec)
            return ImageAvailabilityState(
                digest=digest, stage=DependencyStage.READY, message='Image ready'
            )
        except docker.errors.ImageNotFound:
            return self._pull_or_report(image_spec)  # type: DockerAvailabilityState
        except Exception as ex:
            return ImageAvailabilityState(
                digest=None, stage=DependencyStage.FAILED, message=str(ex)
            )

    def _pull_or_report(self, image_spec):
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
                        digest = self._docker.images.get(image_spec).attrs.get(
                            'RepoDigests', [image_spec]
                        )[0]
                        status = ImageAvailabilityState(
                            digest=digest,
                            stage=DependencyStage.READY,
                            message=self._downloading[image_spec]['message'],
                        )
                    else:
                        status = ImageAvailabilityState(
                            digest=None,
                            stage=DependencyStage.FAILED,
                            message=self._downloading[image_spec]['message'],
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
                    self._downloading[image_spec]['message'] = "Can't download image: {}".format(ex)

            self._downloading.add_if_new(image_spec, threading.Thread(target=download, args=[]))
            return ImageAvailabilityState(
                digest=None,
                stage=DependencyStage.DOWNLOADING,
                message=self._downloading[image_spec]['status'],
            )
