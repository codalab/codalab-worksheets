import unittest
from mock import Mock

from codalab.objects.metadata_spec import MetadataSpec
from codalab.server.bundle_manager import BundleManager, BUNDLE_TIMEOUT_DAYS, SECONDS_PER_DAY
from codalab.worker.bundle_state import RunResources, State, Dependency
from codalab.objects.dependency import Dependency
from codalab.bundles.run_bundle import RunBundle
from codalab.bundles.make_bundle import MakeBundle
from codalab.bundles.dataset_bundle import DatasetBundle
from codalab.lib.codalab_manager import CodaLabManager
from codalab.lib.spec_util import generate_uuid
from collections import namedtuple
import os
import tempfile
import time
from freezegun import freeze_time


class BundleManagerMockedManagerTest(unittest.TestCase):
    """
    Unit tests with a mocked-out CodalabManager.
    """

    def setUp(self):
        self.codalab_manager = Mock(CodaLabManager)
        self.codalab_manager.config = {
            'workers': {
                'default_cpu_image': 'codalab/default-cpu:latest',
                'default_gpu_image': 'codalab/default-gpu:latest',
            }
        }
        self.bundle_manager = BundleManager(self.codalab_manager)
        self.bundle = Mock(spec=RunBundle, metadata=Mock(spec=MetadataSpec))
        self.bundle.metadata.request_queue = None
        self.bundle_resources = RunResources(
            cpus=0, gpus=0, docker_image='', time=100, memory=1000, disk=1000, network=False
        )
        self.bundle.dependencies = []
        dep = namedtuple('dep', ['parent_uuid', 'parent_path'])
        for i in range(6):
            self.bundle.dependencies.append(dep(parent_uuid=i, parent_path=''))
        self.workers_list = self.get_sample_workers_list()

    def tearDown(self):
        del self.bundle
        del self.bundle_manager
        del self.codalab_manager
        del self.bundle_resources
        del self.workers_list

    def get_sample_workers_list(self):
        workers_list = [
            {
                'worker_id': 0,
                'cpus': 4,
                'gpus': 2,
                'memory_bytes': 4 * 1000,
                'exit_after_num_runs': 1000,
                'tag': None,
                'run_uuids': [1, 2],
                'dependencies': [(1, ''), (2, '')],
                'shared_file_system': False,
                'tag_exclusive': False,
                # An additional flag that is generated from WorkerInfoAccessor class
                'has_gpus': True,
            },
            {
                'worker_id': 1,
                'cpus': 4,
                'gpus': 1,
                'memory_bytes': 4 * 1000,
                'exit_after_num_runs': 1000,
                'tag': None,
                'run_uuids': [1, 2, 3],
                'dependencies': [(1, ''), (2, ''), (3, ''), (4, '')],
                'shared_file_system': False,
                'tag_exclusive': False,
                'has_gpus': True,
            },
            # the value of GPUs has been deducted to 0 at this point even though this worker has GPUs
            {
                'worker_id': 2,
                'cpus': 4,
                'gpus': 0,
                'memory_bytes': 4 * 1000,
                'exit_after_num_runs': 1000,
                'tag': None,
                'run_uuids': [],
                'dependencies': [(1, ''), (2, ''), (3, ''), (4, '')],
                'shared_file_system': False,
                'tag_exclusive': False,
                'has_gpus': True,
            },
            {
                'worker_id': 3,
                'cpus': 6,
                'gpus': 0,
                'memory_bytes': 4 * 1000,
                'exit_after_num_runs': 1000,
                'tag': None,
                'run_uuids': [1],
                'dependencies': [(1, '')],
                'shared_file_system': False,
                'tag_exclusive': False,
                'has_gpus': False,
            },
            {
                'worker_id': 4,
                'cpus': 6,
                'gpus': 0,
                'memory_bytes': 4 * 1000,
                'exit_after_num_runs': 1000,
                'tag': None,
                'run_uuids': [1, 2],
                'dependencies': [(1, '')],
                'shared_file_system': False,
                'tag_exclusive': False,
                'has_gpus': False,
            },
            # Tagged workers
            {
                'worker_id': 5,
                'cpus': 6,
                'gpus': 0,
                'memory_bytes': 2 * 1000,
                'exit_after_num_runs': 1000,
                'tag': 'worker_X',
                'run_uuids': [],
                'dependencies': [],
                'shared_file_system': False,
                'tag_exclusive': False,
                'has_gpus': False,
            },
            {
                'worker_id': 6,
                'cpus': 6,
                'gpus': 0,
                'memory_bytes': 2 * 1000,
                'exit_after_num_runs': 1000,
                'tag': 'worker_X',
                'run_uuids': [],
                'dependencies': [],
                'shared_file_system': False,
                'tag_exclusive': True,
                'has_gpus': False,
            },
            {
                'worker_id': 7,
                'cpus': 6,
                'gpus': 1,
                'memory_bytes': 2 * 1000,
                'exit_after_num_runs': 0,
                'tag': 'worker_X',
                'run_uuids': [],
                'dependencies': [],
                'shared_file_system': False,
                'tag_exclusive': True,
                'has_gpus': False,
            },
        ]
        return workers_list

    def test_init_should_parse_config(self):
        self.codalab_manager.config = {
            'workers': {
                'default_cpu_image': 'codalab/default-cpu:latest',
                'default_gpu_image': 'codalab/default-gpu:latest',
                'max_request_time': '50s',
                'max_request_memory': '100m',
                'min_request_memory': '10m',
                'max_request_disk': '10m',
            }
        }
        self.bundle_manager = BundleManager(self.codalab_manager)
        self.assertEqual(self.bundle_manager._max_request_time, 50)
        self.assertEqual(self.bundle_manager._max_request_memory, 100 * 1024 * 1024)
        self.assertEqual(self.bundle_manager._min_request_memory, 10 * 1024 * 1024)
        self.assertEqual(self.bundle_manager._max_request_disk, 10 * 1024 * 1024)

    def test_filter_and_sort_workers_gpus(self):
        # Only GPU workers should appear from the returning sorted worker list
        self.bundle_resources.gpus = 1
        sorted_workers_list = self.bundle_manager._filter_and_sort_workers(
            self.workers_list, self.bundle, self.bundle_resources
        )
        self.assertEqual(len(sorted_workers_list), 2)
        self.assertEqual(sorted_workers_list[0]['worker_id'], 1)
        self.assertEqual(sorted_workers_list[1]['worker_id'], 0)

    def test_filter_and_sort_workers_cpus(self):
        # CPU workers should appear on the top of the returning sorted worker list
        self.bundle_resources.cpus = 1
        sorted_workers_list = self.bundle_manager._filter_and_sort_workers(
            self.workers_list, self.bundle, self.bundle_resources
        )
        self.assertEqual(len(sorted_workers_list), 6)
        self.assertEqual(sorted_workers_list[0]['worker_id'], 3)
        self.assertEqual(sorted_workers_list[1]['worker_id'], 4)
        self.assertEqual(sorted_workers_list[-1]['worker_id'], 0)

    def test_filter_and_sort_workers_tag_exclusive(self):
        # Only non-tag_exclusive workers should appear in the returned sorted worker list.
        sorted_workers_list = self.bundle_manager._filter_and_sort_workers(
            self.workers_list, self.bundle, self.bundle_resources
        )
        self.assertEqual(len(sorted_workers_list), 6)
        for worker in sorted_workers_list:
            self.assertEqual(worker['tag_exclusive'], False)

    def test_filter_and_sort_workers_tag_exclusive_priority(self):
        # All other things being equal, tag_exclusive workers
        # should appear in the top from the returned sorted workers list.
        self.bundle.metadata.request_queue = "tag=worker_X"
        sorted_workers_list = self.bundle_manager._filter_and_sort_workers(
            self.workers_list, self.bundle, self.bundle_resources
        )
        self.assertEqual(len(sorted_workers_list), 2)
        self.assertEqual(sorted_workers_list[0]['worker_id'], 6)
        self.assertEqual(sorted_workers_list[1]['worker_id'], 5)

    def test_get_matched_workers_with_tag(self):
        self.bundle.metadata.request_queue = "tag=worker_X"
        matched_workers = BundleManager._get_matched_workers(
            self.bundle.metadata.request_queue, self.workers_list
        )
        self.assertEqual(len(matched_workers), 3)
        self.assertEqual(matched_workers[0]['worker_id'], 5)
        self.assertEqual(matched_workers[1]['worker_id'], 6)

    def test_get_matched_workers_with_bad_formatted_tag(self):
        self.bundle.metadata.request_queue = "tag="
        matched_workers = BundleManager._get_matched_workers(
            self.bundle.metadata.request_queue, self.workers_list
        )
        self.assertEqual(len(matched_workers), 0)

    def test_get_matched_workers_without_tag_prefix(self):
        self.bundle.metadata.request_queue = "worker_X"
        matched_workers = BundleManager._get_matched_workers(
            self.bundle.metadata.request_queue, self.workers_list
        )
        self.assertEqual(len(matched_workers), 3)
        self.assertEqual(matched_workers[0]['worker_id'], 5)
        self.assertEqual(matched_workers[1]['worker_id'], 6)

    def test_get_matched_workers_not_exist_tag(self):
        self.bundle.metadata.request_queue = "worker_Y"
        matched_workers = BundleManager._get_matched_workers(
            self.bundle.metadata.request_queue, self.workers_list
        )
        self.assertEqual(len(matched_workers), 0)

    def test_get_matched_workers_empty_tag(self):
        self.bundle.metadata.request_queue = ""
        matched_workers = BundleManager._get_matched_workers(
            self.bundle.metadata.request_queue, self.workers_list
        )
        self.assertEqual(len(matched_workers), 0)


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
            user_id=self.user_id,
        )

    def create_mock_worker(
        self, cpus=0, gpus=0, memory_bytes=0, free_disk_bytes=0, tag=None, user_id=None
    ):
        # codalab-owned worker
        worker_id = generate_uuid()
        self.bundle_manager._worker_model.worker_checkin(
            user_id=user_id or self.bundle_manager._model.root_user_id,  # codalab-owned worker
            worker_id=worker_id,
            tag=tag,
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


class BundleManagerStageBundlesTest(BaseBundleManagerTest):
    def test_no_bundles(self):
        self.bundle_manager._stage_bundles()

    def test_single_bundle(self):
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id='id1',
            uuid=generate_uuid(),
            state=State.CREATED,
        )
        self.bundle_manager._model.save_bundle(bundle)

        self.bundle_manager._stage_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STAGED)

    def test_with_dependency(self):
        parent = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.READY,
        )
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.CREATED,
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

        self.bundle_manager._model.save_bundle(parent)
        self.bundle_manager._model.save_bundle(bundle)

        self.bundle_manager._stage_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STAGED)

    def test_do_not_stage_with_failed_dependency(self):
        for state in (State.FAILED, State.KILLED):
            with self.subTest(state=state):
                parent = RunBundle.construct(
                    targets=[],
                    command='',
                    metadata=BASE_METADATA,
                    owner_id=self.user_id,
                    uuid=generate_uuid(),
                    state=state,
                )
                bundle = RunBundle.construct(
                    targets=[],
                    command='',
                    metadata=BASE_METADATA,
                    owner_id=self.user_id,
                    uuid=generate_uuid(),
                    state=State.CREATED,
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

                self.bundle_manager._model.save_bundle(parent)
                self.bundle_manager._model.save_bundle(bundle)

                self.bundle_manager._stage_bundles()

                bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
                self.assertEqual(bundle.state, State.FAILED)
                self.assertIn(
                    "Please use the --allow-failed-dependencies flag",
                    bundle.metadata.failure_message,
                )

    def test_allow_failed_dependencies(self):
        for state in (State.FAILED, State.KILLED):
            with self.subTest(state=state):
                parent = RunBundle.construct(
                    targets=[],
                    command='',
                    metadata=BASE_METADATA,
                    owner_id=self.user_id,
                    uuid=generate_uuid(),
                    state=state,
                )
                bundle = RunBundle.construct(
                    targets=[],
                    command='',
                    metadata=dict(BASE_METADATA, allow_failed_dependencies=True),
                    owner_id=self.user_id,
                    uuid=generate_uuid(),
                    state=State.CREATED,
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

                self.bundle_manager._model.save_bundle(parent)
                self.bundle_manager._model.save_bundle(bundle)

                self.bundle_manager._stage_bundles()

                bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
                self.assertEqual(bundle.state, State.STAGED)

    def test_missing_parent(self):
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.CREATED,
        )
        bundle.dependencies = [
            Dependency(
                {
                    "parent_uuid": generate_uuid(),
                    "parent_path": "",
                    "child_uuid": bundle.uuid,
                    "child_path": "src",
                }
            )
        ]

        self.bundle_manager._model.save_bundle(bundle)

        self.bundle_manager._stage_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.FAILED)
        self.assertIn("Missing parent bundles", bundle.metadata.failure_message)

    def test_no_permission_parents(self):
        parent = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=generate_uuid(),
            uuid=generate_uuid(),
            state=State.READY,
        )
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.CREATED,
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

        self.bundle_manager._model.save_bundle(parent)
        self.bundle_manager._model.save_bundle(bundle)

        self.bundle_manager._stage_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.FAILED)
        self.assertIn("does not have sufficient permissions", bundle.metadata.failure_message)


class BundleManagerMakeBundlesTest(BaseBundleManagerTest):
    def test_no_bundles(self):
        self.bundle_manager._make_bundles()
        self.assertFalse(self.bundle_manager._is_making_bundles())

    def test_restage_stuck_bundle(self):
        bundle = MakeBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA_MAKE_BUNDLE,
            owner_id='id1',
            uuid=generate_uuid(),
            state=State.MAKING,
        )
        self.bundle_manager._model.save_bundle(bundle)

        self.bundle_manager._make_bundles()

        self.assertTrue(self.bundle_manager._is_making_bundles())
        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.MAKING)

    def test_bundle_no_dependencies(self):
        bundle = MakeBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA_MAKE_BUNDLE,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.STAGED,
        )
        self.bundle_manager._model.save_bundle(bundle)

        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.READY)

    def test_single_dependency(self):
        parent = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.READY,
        )
        with open(self.codalab_manager.bundle_store().get_bundle_location(parent.uuid), "w+") as f:
            f.write("hello world")
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
                    "parent_uuid": parent.uuid,
                    "parent_path": "",
                    "child_uuid": bundle.uuid,
                    "child_path": "src",
                }
            )
        ]
        self.bundle_manager._model.save_bundle(parent)
        self.bundle_manager._model.save_bundle(bundle)

        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)

        self.assertEqual(bundle.state, State.READY)

        with open(
            os.path.join(
                self.codalab_manager.bundle_store().get_bundle_location(bundle.uuid), "src"
            ),
            "r",
        ) as f:
            self.assertEqual(f.read(), "hello world")

    def test_multiple_dependencies(self):
        parent1 = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.READY,
        )
        self.bundle_manager._model.save_bundle(parent1)
        with open(self.codalab_manager.bundle_store().get_bundle_location(parent1.uuid), "w+") as f:
            f.write("hello world 1")
        parent2 = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.READY,
        )
        self.bundle_manager._model.save_bundle(parent2)
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
        self.bundle_manager._model.save_bundle(bundle)

        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)

        self.assertEqual(bundle.state, State.READY)

        with open(
            os.path.join(
                self.codalab_manager.bundle_store().get_bundle_location(bundle.uuid), "src1"
            ),
            "r",
        ) as f:
            self.assertEqual(f.read(), "hello world 1")
        with open(
            os.path.join(
                self.codalab_manager.bundle_store().get_bundle_location(bundle.uuid), "src2"
            ),
            "r",
        ) as f:
            self.assertEqual(f.read(), "hello world 2")

    def test_fail_invalid_dependency_path(self):
        parent = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.READY,
        )
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
                    "parent_uuid": parent.uuid,
                    "parent_path": "",
                    "child_uuid": bundle.uuid,
                    "child_path": "src",
                }
            )
        ]
        self.bundle_manager._model.save_bundle(parent)
        self.bundle_manager._model.save_bundle(bundle)

        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)

        self.assertEqual(bundle.state, State.FAILED)
        self.assertIn("Invalid dependency", bundle.metadata.failure_message)

    def test_linked_dependency(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"hello world")
            tempfile_name = f.name
        parent = DatasetBundle.construct(
            metadata=dict(BASE_METADATA_DATASET_BUNDLE, link_url=tempfile_name),
            owner_id=self.user_id,
            uuid=generate_uuid(),
        )
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
                    "parent_uuid": parent.uuid,
                    "parent_path": "",
                    "child_uuid": bundle.uuid,
                    "child_path": "src",
                }
            )
        ]
        self.bundle_manager._model.save_bundle(parent)
        self.bundle_manager._model.save_bundle(bundle)

        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.READY)

        with open(
            os.path.join(
                self.codalab_manager.bundle_store().get_bundle_location(bundle.uuid), "src"
            ),
            "rb",
        ) as f:
            self.assertEqual(f.read(), b"hello world")
        os.remove(tempfile_name)


class BundleManagerScheduleRunBundlesTest(BaseBundleManagerTest):
    def test_no_bundles(self):
        self.bundle_manager._schedule_run_bundles()

    def test_no_workers(self):
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id='id1',
            uuid=generate_uuid(),
            state=State.CREATED,
        )
        self.bundle_manager._model.save_bundle(bundle)

        self.bundle_manager._schedule_run_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.CREATED)

    def test_stage_single_bundle(self):
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=dict(
                BASE_METADATA, request_memory="0", request_time="", request_cpus=1, request_gpus=0
            ),
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.STAGED,
        )
        self.bundle_manager._model.save_bundle(bundle)

        self.create_mock_worker(cpus=1)
        self.bundle_manager._schedule_run_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STARTING)

    def test_stage_single_bundle_request_gpu(self):
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=dict(
                BASE_METADATA, request_memory="0", request_time="", request_cpus=0, request_gpus=1
            ),
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.STAGED,
        )
        self.bundle_manager._model.save_bundle(bundle)

        self.create_mock_worker(cpus=1)
        self.bundle_manager._schedule_run_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STAGED)

        self.create_mock_worker(gpus=1)
        self.bundle_manager._schedule_run_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STARTING)


class BundleManagerFailUnresponsiveBundlesTest(BaseBundleManagerTest):
    def test_no_bundles(self):
        self.bundle_manager._fail_unresponsive_bundles()

    # TODO: switch to the newest version of freezegun with the patch in https://github.com/spulec/freezegun/pull/353,
    # so that we can use as_kwarg and thus maintain the order of parameters as (self, frozen_time).
    @freeze_time("2012-01-14", as_arg=True)
    def test_fail_bundle(frozen_time, self):
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id='id1',
            uuid=generate_uuid(),
            state=State.UPLOADING,
        )
        self.bundle_manager._model.save_bundle(bundle)

        frozen_time.move_to("2020-02-12")
        self.bundle_manager._fail_unresponsive_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.FAILED)
        self.assertIn(
            "Bundle has been stuck in uploading state for more than 60 days",
            bundle.metadata.failure_message,
        )
