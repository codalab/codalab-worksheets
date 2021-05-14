from codalab.worker.image_manager import ImageManager
from codalab.worker.state_committer import JsonStateCommitter
from spython.main import Client

class SingularityImageManager(ImageManager):

    def __init__(self, max_image_size: int, max_image_cache_size: int):
        super().__init__(max_image_size, max_image_cache_size)

