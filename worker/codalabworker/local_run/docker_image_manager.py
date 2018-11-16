from collections import namedtuple
import threading
import time
import traceback
import logging

import docker

from codalabworker.formatting import size_str
from codalabworker.fsm import DependencyStage
from codalabworker.state_committer import JsonStateCommitter
from codalabworker.worker_thread import ThreadDict

logger = logging.getLogger(__name__)

# Stores the download state of a Docker image (as DependencyStage and relevant status message from the download
ImageAvailabilityState = namedtuple('ImageAvailabilityState', ['stage', 'info'])
# Stores information relevant about caching about docker images
ImageCacheEntry = namedtuple(
    'ImageCacheEntry', ['id', 'digest', 'last_used', 'virtual_size', 'marginal_size']
)


class DockerImageManager:
    def __init__(self, commit_file, max_image_cache_size):  # type: str  # type: int  # type: int
        """
        Initializes a DockerImageManager
        :param commit_file: String path to where the state file should be committed
        :param max_image_cache_size: Total size in bytes that the image cache can use
        """
        self._state_committer = JsonStateCommitter(commit_file)  # type: JsonStateCommitter
        self._docker = docker.from_env()  # type: DockerClient
        self._image_cache = {}  # type: Dict[str, ImageCacheEntry]
        self._downloading = ThreadDict(
            fields={'success': False, 'status': 'Download starting'}, lock=True
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
        self._downloading.stop()
        if self._cleanup_thread:
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
        while True:
            time.sleep(self._sleep_secs)
            deletable_entries = set(self._image_cache.values())
            disk_use = sum(cache_entry.virtual_size for cache_entry in deletable_entries)
            while disk_use > self._max_image_cache_size:
                entry_to_remove = min(deletable_entries, key=lambda entry: entry.last_used)
                try:
                    self._docker.images.remove(entry_to_remove.id)
                    # if we successfully removed the image also remove its cache entry
                    del self._image_cache[entry_to_remove.digest]
                except docker.errors.APIError:
                    # Maybe we can't delete this image because its container is still running
                    # (think a run that takes 4 days so this is the oldest image but still in use)
                    # In that case we just continue with our lives, hoping it will get deleted once
                    # it's no longer in use and the cache becomes full again
                    pass
                deletable_entries.remove(entry_to_remove)
                disk_use = sum(entry.virtual_size for entry in deletable_entries)

    def get(self, image_spec):
        """
        Request the docker image for the run with uuid, registering uuid as a dependent of this docker image
        :param image_spec: Repo image_spec of docker image being requested
        :returns: A DockerAvailabilityState object with the state of the docker image
        """
        try:
            image = self._docker.images.get(image_spec)
            digest = image.attrs.get('RepoDigests', [image_spec])[0]
            with self._lock:
                self._image_cache[digest] = ImageCacheEntry(
                    id=image.id,
                    digest=digest,
                    last_used=time.time(),
                    virtual_size=image.attrs['VirtualSize'],
                    marginal_size=image.attrs['Size'],
                )
            return ImageAvailabilityState(stage=DependencyStage.READY, info='Image ready')
        except docker.errors.ImageNotFound:
            return self._pull_or_report(image_spec)  # type: DockerAvailabilityState
        except Exception as ex:
            return ImageAvailabilityState(stage=DependencyStage.FAILED, info=str(ex))

    def _pull_or_report(self, image_spec):
        if image_spec in self._downloading:
            with self._downloading[image_spec]['lock']:
                if self._downloading[image_spec].is_alive():
                    return ImageAvailabilityState(
                        stage=DependencyStage.DOWNLOADING, info=self._downloading[image_spec].status
                    )
                else:
                    if self._downloading[image_spec]['success']:
                        status = ImageAvailabilityState(
                            stage=DependencyStage.READY, info=self._downloading[image_spec].status
                        )
                    else:
                        status = ImageAvailabilityState(
                            stage=DependencyStage.FAILED, info=self._downloading[image_spec].status
                        )
                    with self._downloading['image_spec']['lock']:
                        self._downloading.remove(image_spec)
                    return status
        else:

            def download():
                logger.debug('Downloading Docker image %s', image_spec)
                output = self._docker.images.pull(image_spec, stream=True, decode=True)
                for status_dict in output:
                    if 'error' in status_dict:
                        with self._downloading[image_spec]['lock']:
                            self._downloading[image_spec]['status'] = status_dict['error']
                            return
                    new_status = status_dict.get('status', '')
                    try:
                        new_status += ' (%s / %s)' % (
                            size_str(status_dict['progressDetail']['current']),
                            size_str(status_dict['progressDetail']['total']),
                        )
                    except KeyError:
                        pass
                    with self._downloading[image_spec]['lock']:
                        self._downloading[image_spec].status = new_status
                with self._downloading[image_spec]['lock']:
                    self._downloading[image_spec]['success'] = True
                    self._downloading[image_spec]['status'] = 'Download complete'

            self._downloading.add_if_new(image_spec, threading.Thread(target=download, args=[]))
