import tempfile
import time
import unittest
from mock import Mock

from codalabworker.local_run.local_dependency_manager import LocalFileSystemDependencyManager
from codalabworker.fsm import JsonStateCommitter, DependencyStage
from codalabworker.file_util import remove_path


class DockerImageManagerTest(unittest.TestCase):
    def setUp(self):
        self.state_committer = Mock(spec=JsonStateCommitter)
        self.docker = Mock(spec=DockerClient)
        self.manager = DockerImageManager(self.docker, self.state_committer, max_images_bytes=100)

