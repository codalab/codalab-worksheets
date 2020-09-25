import unittest
from mock import Mock
import os
from codalab.server.bundle_manager import BundleManager
from codalab.worker.bundle_state import BundleCheckinState, State
from codalab.lib.codalab_manager import CodaLabManager
from codalab.bundles.run_bundle import RunBundle
from codalab.bundles.make_bundle import MakeBundle
from codalab.bundles.dataset_bundle import DatasetBundle
from codalab.lib.spec_util import generate_uuid
from codalab.objects.dependency import Dependency


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
    "failure_message": "",
    "request_disk": "",
    "request_gpus": 0,
    "remote": "vm-clws-prod-worker-3",
    "exclude_patterns": [],
}

BASE_METADATA_MAKE_BUNDLE = {
    "description": "",
    "name": "make-1",
    "created": 1495784349,
    "failure_message": "",
    "tags": [],
    "allow_failed_dependencies": False,
}

BASE_METADATA_DATASET_BUNDLE = {
    "description": "",
    "name": "dataset-1",
    "created": 1495784349,
    "failure_message": "",
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
        self.download_manager = self.codalab_manager.download_manager()
        self.user_id = generate_uuid()
        self.bundle_manager._model.add_user(
            "codalab",
            "noreply@codalab.org",
            "Test",
            "User",
            "password",
            "Stanford",
            user_id=self.user_id,
        )

    def create_make_bundle(self, state=State.MAKING):
        bundle = MakeBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA_MAKE_BUNDLE,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=state,
        )
        return bundle

    def save_bundle(self, bundle):
        self.bundle_manager._model.save_bundle(bundle)

    def read_bundle(self, bundle, extra_path=""):
        return open(
            os.path.join(
                self.codalab_manager.bundle_store().get_bundle_location(bundle.uuid), extra_path
            ),
            "r",
        )

    def write_bundle(self, bundle, extra_path=""):
        location = self.codalab_manager.bundle_store().get_bundle_location(bundle.uuid)
        if extra_path:
            # Write to a directory.
            location = os.path.join(
                location, extra_path
            )
            os.makedirs(os.path.dirname(location), exist_ok=True)
        return open(
            location,
            "w+",
        )

    def create_run_bundle(self, state=State.CREATED, metadata=None):
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=dict(BASE_METADATA, **(metadata or {})),
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=state,
        )
        return bundle

    def create_bundle_single_dep(
        self, parent_state=State.READY, bundle_state=State.CREATED, bundle_type=RunBundle
    ):
        parent = self.create_run_bundle(parent_state)
        with open(self.codalab_manager.bundle_store().get_bundle_location(parent.uuid), "w+") as f:
            f.write("hello world")
        bundle = (
            self.create_run_bundle(bundle_state)
            if bundle_type == RunBundle
            else self.create_make_bundle(bundle_state)
        )
        bundle.dependencies = [
            Dependency(
                {
                    "parent_uuid": parent.uuid,
                    "parent_path": "",
                    "child_uuid": bundle.uuid,
                    "child_path": "src",
                }
            )
        ]
        return bundle, parent

    def create_bundle_two_deps(self):
        parent1 = self.create_run_bundle(state=State.READY)
        with open(self.codalab_manager.bundle_store().get_bundle_location(parent1.uuid), "w+") as f:
            f.write("hello world 1")
        parent2 = self.create_run_bundle(state=State.READY)
        with open(self.codalab_manager.bundle_store().get_bundle_location(parent2.uuid), "w+") as f:
            f.write("hello world 2")
        bundle = MakeBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA_MAKE_BUNDLE,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.STAGED,
        )
        bundle.dependencies = [
            Dependency(
                {
                    "parent_uuid": parent1.uuid,
                    "parent_path": "",
                    "child_uuid": bundle.uuid,
                    "child_path": "src1",
                }
            ),
            Dependency(
                {
                    "parent_uuid": parent2.uuid,
                    "parent_path": "",
                    "child_uuid": bundle.uuid,
                    "child_path": "src2",
                }
            ),
        ]
        return bundle, parent1, parent2

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
