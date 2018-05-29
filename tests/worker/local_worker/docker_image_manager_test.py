import time
import unittest
from mock import Mock

from codalabworker.docker_client import DockerClient
from codalabworker.local_run.docker_image_manager import DockerImageManager
from codalabworker.fsm import JsonStateCommitter, DependencyStage
from codalabworker.file_util import remove_path


class DockerImageManagerTest(unittest.TestCase):
    def setUp(self):
        self.state_committer = Mock(spec=JsonStateCommitter)
        self.docker = Mock(spec=DockerClient)
        self.manager = DockerImageManager(self.docker, self.state_committer, max_images_bytes=100)

    def test_normal_workflow(self):
        test_digest = 'image:A'
        self.assertFalse(self.manager.has(test_digest))
        def download_checker(digest, cb):
            # Make sure image manager reports as having this image
            self.assertTrue(self.manager.has(digest))
            digest_from_manager = self.manager.get(digest)
            # Make sure image manager reports status as DOWNLOADING
            self.assertEqual(digest_from_manager.stage, DependencyStage.DOWNLOADING)
        self.docker.download_image.side_effect = download_checker
        self.manager.get(test_digest)
        self.docker.download_image.assert_called_with(test_digest, Mock.Any)
        # Download done, make sure manager has the digest
        self.assertTrue(self.manager.has(test_digest))
        # Make sure its stage is set to READY
        digest_from_manager = self.manager.get(test_digest)
        self.assertEqual(digest_from_manager.stage, DependencyStage.READY)

    def test_concurrent_requests(self):
        test_digest = 'image:A'
        def request_while_downloading(digest, cb):
            # Make sure we don't create a new image for the same digest
            num_old_digests = len(self.manager._images)
            second_request = self.manager.get(digest)
            num_new_digests = len(self.manager._images)
            self.assertEqual(num_old_digests, num_new_digests)
        self.docker.download_image.side_effect = request_while_downloading
        self.manager.get(test_digest)
        # Make sure download image is only called once
        self.docker.download_image.assert_called_once_with(test_digest, Mock.Any)

    def test_state_management(self):
        # TODO: Test saving state correctly and loading state correctly
        # Including resuming dependency downloads
        pass

    def test_download_failure(self):
        # TODO: Test what happens if DockerException is thrown by docker client
        pass

    def test_download_timeout(self):
        # TODO: Test a download timeout
        pass

    def test_very_large_image(self):
        # TODO: Test downloading an image larger than disk limit
        pass

    def test_remove_while_downloading(self):
        # TODO: Test killing an image while being downloaded, should cancel download
        pass

    def test_remove_after_downloading(self):
        # TODO: Test removing an image when READY, should just work
        pass

    def test_cleanup_failed_first(self):
        # TODO: Make sure failed dependencies are cleared first
        pass

    def test_cleanup_oldest_first(self):
        # TODO: Test oldest dependencies are cleaned first
        pass

    def test_cleanup_multiple_images(self):
        # TODO: Test cleanup can clear multiple dependencies
        pass

    def test_cleanup_no_breaking_downloads(self):
        # TODO: Make sure cleanup doesn't break downloading images randomly
        pass

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
