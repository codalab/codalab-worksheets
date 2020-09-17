import unittest
from mock import Mock

from codalab.server.bundle_manager import BundleManager
from codalab.worker.bundle_state import BundleCheckinState
from codalab.lib.codalab_manager import CodaLabManager
from codalab.lib.spec_util import generate_uuid


BASE_METADATA = {
    "docker_image": "sckoo/bird-brain@sha256:5076a236533caf8bea3410dcfaa10ef2dab506a3505cd33bce5190951d99af84",
    "time": 1830.8628242,
    "started": 1495784349,
    "request_network": False,
    "request_cpus": 0,
    "request_memory": "0",
    "request_time": "0",
    "request_priority": 0,
    "description": "",
    "request_queue": "",
    "name": "run-python",
    "exitcode": 137,
    "data_size": 601111,
    "created": 1495784349,
    "allow_failed_dependencies": False,
    "actions": ["kill"],
    "request_docker_image": "sckoo/bird-brain:v3",
    "memory_max": 0,
    "tags": [],
    "run_status": "Finished",
    "last_updated": 1495786180,
    "failure_message": "Kill requested",
    "request_disk": "",
    "request_gpus": 0,
    "remote": "vm-clws-prod-worker-3",
    "exclude_patterns": [],
}

BASE_METADATA_MAKE_BUNDLE = {
    "description": "",
    "name": "run-python",
    "created": 1495784349,
    "failure_message": "Kill requested",
    "tags": [],
    "allow_failed_dependencies": False,
}

BASE_METADATA_DATASET_BUNDLE = {
    "description": "",
    "name": "run-python",
    "created": 1495784349,
    "failure_message": "Kill requested",
    "tags": [],
    "license": "",
    "source_url": "",
}


class BaseBundleManagerTest(unittest.TestCase):
    """
    Base class for BundleManager tests with a CodaLab Manager hitting a real, in-memory database.
    """

    def setUp(self):
        self.codalab_manager = CodaLabManager()
        self.codalab_manager.config['server']['class'] = 'SQLiteModel'
        self.bundle_manager = BundleManager(self.codalab_manager)
        self.user_id = generate_uuid()
        self.bundle_manager._model.add_user(
            "username",
            "email@email.com",
            "first name",
            "last name",
            "password",
            "affiliation",
            user_id=self.user_id
        )

    def mock_worker_checkin(
        self, cpus=0, gpus=0, memory_bytes=0, free_disk_bytes=0, tag=None, user_id=None,
    ):
        """Mock check-in a new worker."""
        # codalab-owned worker
        worker_id = generate_uuid()
        self.bundle_manager._worker_model.worker_checkin(
            user_id=user_id or self.bundle_manager._model.root_user_id,  # codalab-owned worker
            worker_id=worker_id,
            tag=tag,
            group_name=None,
            cpus=cpus,
            gpus=gpus,
            memory_bytes=memory_bytes,
            free_disk_bytes=free_disk_bytes,
            dependencies=[],
            shared_file_system=False,
            tag_exclusive=False,
            exit_after_num_runs=999999999,
            is_terminating=False,
        )
        # Mock a reply from the worker
        self.bundle_manager._worker_model.send_json_message = Mock(return_value=True)
        return worker_id

    def mock_bundle_checkin(self, bundle, worker_id, user_id=None):
        """Mock a worker checking in with the latest status of a bundle."""
        worker_run = BundleCheckinState(
            uuid=bundle.uuid,
            run_status="",
            bundle_start_time=0,
            container_time_total=0,
            container_time_user=0,
            container_time_system=0,
            docker_image="",
            state=bundle.state,
            remote="",
            exitcode=0,
            failure_message="",
        )
        self.bundle_manager._model.bundle_checkin(
            bundle, worker_run, user_id or self.user_id, worker_id
        )
