import unittest
from mock import Mock
from codalabworker.local_run.local_run_state import LocalRunStage, LocalRunStateMachine
from codalabworker.local_run.local_run_manager import LocalRunManager

class LocalRunStateMachineTest(unittest.TestCase):
    def setUp(self):
        self.run_manager = Mock(spec=LocalRunManager)
        self.rsm = LocalRunStateMachine(self.run_manager)

    def test_full_workflow(self):
        """
        """
        pass

    def test_get_all_dependencies(self):
        """
        TODO: Make sure all dependencies of a run state are requested from the dependency manager
        """
        pass

    def test_get_docker_image(self):
        """
        TODO: Make sure the docker image of a run state are requested from the docker manager
        """
        pass

    def test_fail_on_failed_dependency(self):
        """
        TODO: Make sure bundle fails if a dependency download fails
        """
        pass

    def test_fail_on_failed_docker(self):
        """
        TODO: Make sure bundle fails if a docker download fails
        """
        pass

    def test_kill_during_dependency_download(self):
        """
        TODO: Make sure bundle can be killed during a dependency download
        """
        pass

    def test_kill_during_docker_download(self):
        """
        TODO: Make sure bundle can be killed during a docker download
        """
        pass

    def test_symlinks(self):
        """
        TODO: Make sure symlinks are created properly before bundle is run
        """
        pass
 
    def test_docker_container_start(self):
        """
        TODO: Make sure docker container started correctly
        """
        pass
 
    def test_docker_container_start_failure(self):
        """
        TODO: Make sure docker container start failures are handled correctly
        """
        pass

    def test_running_state_reporting(self):
        """
        TODO: Make sure state is updated correctly while docker container is running
        """
        pass

    def test_running_docker_failure(self):
        """
        TODO: Make sure docker container failures are dealt with
        """
        pass

    def test_kill_during_docker_run(self):
        """
        TODO: Make sure killing a run while a docker container is running kills the docker container and handles state fine
        """
        pass

    def test_upload_results(self):
        """
        TODO: Make sure results are uploaded correctly
        """
        pass

    def test_kill_during_upload_results(self):
        """
        TODO: Make sure uploads can be interrupted by kills
        """
        pass

    def test_finalize(self):
        """
        TODO: Make sure the correct finalize bundle call is made to bundle service
        """
        pass

    def test_finalize_fail(self):
        """
        TODO: Fail the finalize call make sure cleanup is still done
        """
