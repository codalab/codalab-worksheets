import unittest
from unittest.mock import Mock
import os
from codalab.server.bundle_manager import BundleManager
from codalab.worker.bundle_state import BundleCheckinState, State
from codalab.lib.codalab_manager import CodaLabManager
from codalab.bundles.run_bundle import RunBundle
from codalab.bundles.make_bundle import MakeBundle
from codalab.lib.spec_util import generate_uuid
from codalab.objects.dependency import Dependency
from codalab.worker.worker_run_state import RunStage

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

FILE_CONTENTS_1 = "hello world 1"
FILE_CONTENTS_2 = "hello world 2"


class TestBase:
    """
    Base class for BundleManager tests with a CodaLab Manager hitting an in-memory SQLite database.
    """

    def setUp(self):
        self.codalab_manager = CodaLabManager()
        self.codalab_manager.config['server']['class'] = 'SQLiteModel'
        self.bundle_manager = BundleManager(self.codalab_manager)
        self.download_manager = self.codalab_manager.download_manager()
        self.upload_manager = self.codalab_manager.upload_manager()

        # Create a standard user
        self.user_id = generate_uuid()
        self.bundle_manager._model.add_user(
            "codalab_standard",
            "noreply+standard@worksheets.codalab.org",
            "Test",
            "User",
            "password",
            "Stanford",
            user_id=self.user_id,
        )

        # Create a root user
        self.root_user_id = self.codalab_manager.root_user_id()
        self.bundle_manager._model.add_user(
            "codalab_root",
            "noreply+root@worksheets.codalab.org",
            "Test",
            "User",
            "password",
            "Stanford",
            user_id=self.root_user_id,
        )

    def create_make_bundle(self, state=State.MAKING):
        """Creates a MakeBundle with the given state."""
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
        """Saves the given bundle to the database."""
        self.bundle_manager._model.save_bundle(bundle)

    def read_bundle(self, bundle, extra_path=""):
        """Retrieves the given bundle from the bundle store and returns
        its contents.
        Args:
            extra_path: path appended to bundle store location from which to read the file.
        Returns:
            Bundle contents
        """
        with open(
            os.path.join(
                self.codalab_manager.bundle_store().get_bundle_location(bundle.uuid), extra_path
            ),
            "r",
        ) as f:
            return f.read()

    def write_bundle(self, bundle, contents=""):
        """Writes the given contents to the location of the given bundle.
        Args:
            bundle: bundle to write
            contents: string to write
        Returns:
            None
        """
        with open(self.codalab_manager.bundle_store().get_bundle_location(bundle.uuid), "w+") as f:
            f.write(contents)

    def update_bundle(self, bundle, update):
        return self.bundle_manager._model.update_bundle(bundle, update)

    def create_run_bundle(self, state=State.CREATED, metadata=None):
        """Creates a RunBundle.
        Args:
            state: state for the new bundle
            metadata: additional metadata to add to the bundle.
        """
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=dict(BASE_METADATA, **(metadata or {})),
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=state,
        )
        bundle.is_frozen = None
        bundle.is_anonymous = False
        return bundle

    def create_bundle_single_dep(
        self, parent_state=State.READY, bundle_state=State.CREATED, bundle_type=RunBundle
    ):
        """Creates a bundle with a single dependency, which is mounted at path "src" of the
        new bundle.

        Args:
            parent_state: State of the parent bundle. Defaults to State.READY.
            bundle_state: State of the new bundle. Defaults to State.CREATED.
            bundle_type: Type of child bundle to create; valid values are RunBundle and MakeBundle. Defaults to RunBundle.

        Returns:
            (bundle, parent)
        """
        parent = self.create_run_bundle(parent_state)
        self.save_bundle(parent)
        self.write_bundle(parent, FILE_CONTENTS_1)
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
        self.save_bundle(bundle)
        return bundle, parent

    def create_bundle_two_deps(self):
        """Create a bundle with two dependencies. The first dependency is mounted at path "src1"
        and the second is mounted at path "src2" of the new bundle.

        Returns:
            (bundle, parent1, parent2)
        """
        parent1 = self.create_run_bundle(state=State.READY)
        self.save_bundle(parent1)
        self.write_bundle(parent1, FILE_CONTENTS_1)
        parent2 = self.create_run_bundle(state=State.READY)
        self.save_bundle(parent2)
        self.write_bundle(parent2, FILE_CONTENTS_2)
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
        self.save_bundle(bundle)
        return bundle, parent1, parent2

    def mock_worker_checkin(
        self, cpus=0, gpus=0, memory_bytes=0, free_disk_bytes=0, tag=None, user_id=None
    ):
        """Perform a mock check-in of a new worker."""
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
        """Mock a worker checking in with the latest state of a bundle.

        Args:
            bundle: Bundle to check in.
            worker_id ([type]): worker id of the worker that performs the checkin.
            user_id (optional): user id that performs the checkin. Defaults to the default user id.
        """
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
            cpu_usage=0.0,
            memory_usage=0.0,
            bundle_profile_stats={
                RunStage.PREPARING: {'start': 15, 'end': 20, 'elapsed': 5},
                RunStage.RUNNING: {'start': 15, 'end': 20, 'elapsed': 5},
                RunStage.CLEANING_UP: {'start': 15, 'end': 20, 'elapsed': 5},
                RunStage.UPLOADING_RESULTS: {'start': 15, 'end': 20, 'elapsed': 5},
                RunStage.FINALIZING: {'start': 15, 'end': 20, 'elapsed': 5},
            },
        )
        self.bundle_manager._model.bundle_checkin(
            bundle, worker_run, user_id or self.user_id, worker_id
        )


class BaseBundleManagerTest(TestBase, unittest.TestCase):
    pass
