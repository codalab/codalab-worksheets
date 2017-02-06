import tempfile
import time
import unittest
from mock import Mock

from worker.docker_client import DockerClient
from worker.docker_image_manager import DockerImageManager
from worker.file_util import remove_path


class DockerImageManagerTest(unittest.TestCase):
    def setUp(self):
        self.work_dir = tempfile.mkdtemp()
        self.docker = Mock(spec=DockerClient)
        self.manager = DockerImageManager(self.docker, self.work_dir,
                                          min_disk_free_bytes=100)

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

        # Simulate the available bytes being enough
        self.manager._get_free_bytes = lambda: 1000
        self._run_cleanup()

        # Should not do anything to the images
        self.assertIn(A, self.manager._last_used)
        self.assertIn(B, self.manager._last_used)
        self.docker.remove_image.assert_not_called()

        # Simulate the available bytes being too small
        # and increasing with each call to remove image
        free_bytes = [50]  # allow update from inside closure
        self.manager._get_free_bytes = lambda: free_bytes[0]
        def increment_free_bytes(*args, **kwargs):
            free_bytes[0] += 100
        self.docker.remove_image.side_effect = increment_free_bytes
        self._run_cleanup()

        # Only image A should be removed
        self.assertNotIn(A, self.manager._last_used)
        self.assertIn(B, self.manager._last_used)
        self.docker.remove_image.assert_called_with(A)

        # Simulate available bytes being too small
        # and never reaching the minimum free bytes
        self.manager._get_free_bytes = lambda: 10
        self._run_cleanup()

        # Image B should be removed now, and things shouldn't crash
        self.assertNotIn(A, self.manager._last_used)
        self.docker.remove_image.assert_called_with(B)
