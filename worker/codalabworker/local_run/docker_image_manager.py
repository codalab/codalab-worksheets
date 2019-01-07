from collections import namedtuple
import threading
import time
import traceback
import logging

import docker

from codalabworker.fsm import DependencyStage
from codalabworker.state_committer import JsonStateCommitter
from codalabworker.worker_thread import ThreadDict

logger = logging.getLogger(__name__)

# Stores the download state of a Docker image (also includes the digest being pulled, digest string, DependencyStage and relevant status message from the download)
ImageAvailabilityState = namedtuple('ImageAvailabilityState', ['digest', 'stage', 'message'])
# Stores information relevant about caching about docker images
ImageCacheEntry = namedtuple(
    'ImageCacheEntry', ['id', 'digest', 'dependents', 'last_used', 'virtual_size', 'marginal_size']
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
        logger.info("Stopped docker image manager.")

    def _cleanup(self):
        """
        Prunes the image cache for runs
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
            time.sleep(self._sleep_secs)
            with self._lock:
                all_entries = set(self._image_cache.values())
                total_disk_used = sum(cache_entry.virtual_size for cache_entry in all_entries)
                # Only delete entries that don't have dependents (ie not in use)
                deletable_entries = filter(lambda x: not x.dependents, all_entries)
                deletable_disk_used = sum(
                    cache_entry.virtual_size for cache_entry in deletable_entries
                )
                if total_disk_used - deletable_disk_used > self._max_image_cache_size:
                    logger.error(
                        'Size of docker images in use (%d) greater than docker disk use quota (%d)',
                        total_disk_used - deletable_disk_used,
                        self._max_image_cache_size,
                    )
                while deletable_disk_used > self._max_image_cache_size:
                    entry_to_remove = min(deletable_entries, key=lambda entry: entry.last_used)
                    logger.info(
                        'Deletable disk use (%s) > max cache size (%s), pruning image: %s',
                        deletable_disk_used,
                        self._max_image_cache_size,
                        entry_to_remove.digest,
                    )
                    try:
                        self._docker.images.remove(entry_to_remove.id, force=True)
                        # if we successfully removed the image also remove its cache entry
                        del self._image_cache[entry_to_remove.digest]
                    except docker.errors.APIError as err:
                        # We should not really hit this case so log it
                        logger.error(
                            "Cannot remove image %s from cache: %s", entry_to_remove.digest, err
                        )
                    deletable_entries.remove(entry_to_remove)
                    deletable_disk_used = sum(entry.virtual_size for entry in deletable_entries)
        logger.debug("Stopping docker image manager cleanup")

    def get(self, uuid, image_spec):
        """
        Request the docker image for the run with uuid, registering uuid as a dependent of this docker image
        :param uuid: UUID of the run that needs this docker image
        :param image_spec: Repo image_spec of docker image being requested
        :returns: A DockerAvailabilityState object with the state of the docker image
        """
        try:
            image = self._docker.images.get(image_spec)
            digest = image.attrs.get('RepoDigests', [image_spec])[0]
            with self._lock:
                if digest in self._image_cache:
                    old_entry = self._image_cache[digest]
                    new_entry = old_entry._replace(last_used=time.time())
                    new_entry.dependents.add(uuid)
                else:
                    new_entry = ImageCacheEntry(
                        id=image.id,
                        digest=digest,
                        dependents=set([uuid]),
                        last_used=time.time(),
                        virtual_size=image.attrs['VirtualSize'],
                        marginal_size=image.attrs['Size'],
                    )
                self._image_cache[digest] = new_entry
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

    def release(self, uuid, image_spec):
        """
        Register that the run with uuid doesn't need the docker image anymore
        :param uuid: UUID of the run that doesn't need this docker image anymore
        :param image_spec: Repo image_spec of docker image being requested
        """
        try:
            image = self._docker.images.get(image_spec)
            digest = image.attrs.get('RepoDigests', [image_spec])[0]
            with self._lock:
                try:
                    self._image_cache[digest].dependents.remove(uuid)
                except KeyError:
                    # Don't have this image in cache anymore, log and pass
                    logger.error(
                        "Image (%s) that was in use by run (%s) was not found in cache at release time.",
                        image_spec,
                        uuid,
                    )
                except ValueError:
                    # This bundle wasn't in dependents, log and pass
                    logger.error(
                        "Run (%s) was not found in image (%s)'s dependents at release time even though it used it.",
                        uuid,
                        image_spec,
                    )
        except docker.errors.ImageNotFound:
            # We don't have the image so no need to do anything
            pass

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
                            message=self._downloading[image_spec]['status'],
                        )
                    else:
                        status = ImageAvailabilityState(
                            digest=None,
                            stage=DependencyStage.FAILED,
                            message=self._downloading[image_spec]['status'],
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
                except docker.errors.APIError as ex:
                    logger.debug('Download for Docker image %s failed: %s', image_spec, ex)
                    self._downloading[image_spec]['success'] = False
                    self._downloading[image_spec]['message'] = "Can't download image: {}".format(ex)

            self._downloading.add_if_new(image_spec, threading.Thread(target=download, args=[]))
            return ImageAvailabilityState(
                digest=None,
                stage=DependencyStage.DOWNLOADING,
                message=self._downloading[image_spec]['status'],
            )
