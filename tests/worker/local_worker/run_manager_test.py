import unittest
from mock import Mock

from codalabworker.local_run.local_run_manager import LocalRunManager
from codalabworker.local_run.local_run_state import LocalRunStage, LocalRunStateMachine
from codalabworker.local_run.docker_image_manager import DockerImageManager
from codalabworker.local_run.local_dependency_manager import LocalFileSystemDependencyManager
from codalabworker.fsm import JsonStateCommitter
from codalabworker import Worker
from codalabworker.docker_client import DockerClient

class LocalRunStateMachineTest(unittest.TestCase):
    def setUp(self):
        self.worker = Mock(spec=Worker)
        self.docker = Mock(spec=DockerClient)
        self.image_manager = Mock(spec=DockerImageManager)
        self.dependency_manager = Mock(spec=LocalFileSystemDependencyManager)
        self.state_committer = Mock(spec=JsonStateCommitter)
        self.bundles_dir = '/test/bundles'
        self.cpuset = None  # TODO: What's a cpuset
        self.gpuset = None  # TODO: What's a gpuset
        self.docker_network_prefix = 'test_network'
        # TODO: Patch LocalReader and LocalRunStateMachine
        self.run_manager = LocalRunManager(self.worker,
                                           self.docker,
                                           self.image_manager,
                                           self.dependency_manager,
                                           self.state_committer,
                                           self.bundles_dir,
                                           self.cpuset,
                                           self.gpuset,
                                           self.docker_network_prefix)

    def test_full_workflow(self):
        """
        """
        pass
