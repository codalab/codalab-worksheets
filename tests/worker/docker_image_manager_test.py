import tempfile
import time
import unittest
from mock import Mock

from codalabworker.docker_client import DockerClient
from codalabworker.docker_image_manager import DockerImageManager
from codalabworker.file_util import remove_path


class DockerImageManagerTest(unittest.TestCase):
    def setUp(self):
        self.work_dir = tempfile.mkdtemp()
        self.docker = Mock(spec=DockerClient)
        self.manager = DockerImageManager(self.docker, self.work_dir,
                                          max_images_bytes=100)

    def tearDown(self):
        remove_path(self.work_dir)

    def _run_cleanup(self):
        self.manager.start_cleanup_thread()
        time.sleep(0.1)
        self.manager.stop_cleanup_thread()

    def test_cleanup(self):
        # Add image B after image A
        A = 'image:A'
        B = 'image:B'
        self.manager.touch_image(A)
        time.sleep(0.1)
        self.manager.touch_image(B)

        # Simulate the limit NOT being exceeded
        self.docker.get_disk_usage.return_value = (10, 10)
        self._run_cleanup()

        # Simulate reclaimable disk usage being zero despite disk usage
        # exceeding limit
        self.docker.get_disk_usage.return_value = (120, 0)
        self._run_cleanup()

        # Should not do anything to the images in either case
        self.assertIn(A, self.manager._last_used)
        self.assertIn(B, self.manager._last_used)
        self.docker.remove_image.assert_not_called()

        # Simulate the limit beind exceeded, and then the disk usage decreasing
        # below the limit after removing image A, the stalest image.
        self.docker.get_disk_usage.return_value = (120, 120)
        def increment_free_bytes(*args, **kwargs):
            t, r = self.docker.get_disk_usage.return_value
            self.docker.get_disk_usage.return_value = (t - 50, r - 50)
        self.docker.remove_image.side_effect = increment_free_bytes
        self._run_cleanup()

        # Only image A should be removed
        self.assertNotIn(A, self.manager._last_used)
        self.assertIn(B, self.manager._last_used)
        self.docker.remove_image.assert_called_with(A)

        # Simulate the limit being exceeded again, and then the disk usage never
        # decreasing below the limit
        self.docker.get_disk_usage.return_value = (120, 120)
        self._run_cleanup()

        # Image B should be removed now, and things shouldn't crash
        self.assertNotIn(A, self.manager._last_used)
        self.docker.remove_image.assert_called_with(B)
