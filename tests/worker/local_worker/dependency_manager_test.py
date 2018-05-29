import os
import time
import unittest
from mock import Mock
import fake_filesystem_unittest

from codalabworker.local_run.local_dependency_manager import LocalFileSystemDependencyManager
from codalabworker.fsm import JsonStateCommitter, DependencyStage
from codalabworker.file_util import remove_path
from codalabworker.bundle_service_client import BundleServiceClient


class DockerImageManagerTest(fake_filesystem_unittest.TestCase):
    def setUp(self):
        """
        Dependency Manager assumes the work dir already exists but it should
        create the bundles directory upon initialization
        """
        self.setUpPyfakefs()
        self.work_dir = '/test/work-dir'
        self.bundles_dir = os.path.join(self.work_dir, 'bundles')
        self.fs.create_dir(self.work_dir)
        self.state_committer = Mock(spec=JsonStateCommitter)
        self.bundle_service = Mock(spec=BundleServiceClient)
        self.max_cache_size_bytes = 1024
        self.max_serialized_length = 1024
        self.manager = LocalFileSystemDependencyManager(self.state_committer,
                                                        self.bundle_service,
                                                        self.work_dir,
                                                        self.max_cache_size_bytes,
                                                        self.max_serialized_length)

    def test_bundles_dir_created(self):
        """
        Make sure dependency manager creates the bundles dir
        """
        self.assertTrue(os.path.exists(self.work_dir))
        self.assertTrue(os.path.exists(self.bundles_dir))

    def test_assign_path(self):
        """
        TODO: Avoid path conflicts betweeen names like a/b_c a_b/c
        """
        pass

    def test_store_dependency(self):
        """
        TODO: Make sure directory and file dependencies are stored properly
        """
        pass

    def test_download_success(self):
        """
        TODO: Make sure downloading is handled correctly
        """
        pass

    def test_download_failure(self):
        """
        TODO: Make sure a failing download is handled correctly
            - State should be failed
            - All filesytem traces should be cleared
        """
        pass

    def test_download_timeout(self):
        """
        TODO: Time out for long downloads
        """
        pass

    def test_multiple_requests(self):
        """
        TODO: Make sure dependency added once if requested multiple times
        """
        pass

    def test_release(self):
        """
        TODO: Make sure depending bundles can release their dependencies
        """
        pass

    def test_mid_download_kill(self):
        """
        TODO: Make sure dependency downloads are killable
        """
        pass

    def test_kill_after_last_release(self):
        """
        TODO: Make sure a dependency download is killed if it has no more dependents
        """
        pass

    def test_cleanup_failing_first(self):
        """
        TODO: Make sure failed dependencies are cleaned up first
        """
        pass

    def test_cleanup_oldest_first(self):
        """
        TODO: Make sure oldest dependencies are cleaned up first
        """
        pass

    def test_cleanup_keep_depended(self):
        """
        TODO: Make sure dependencies actively being used are not cleaned up
        """
        pass
