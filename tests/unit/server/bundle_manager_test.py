import unittest
from unittest.mock import Mock

from codalab.objects.metadata_spec import MetadataSpec
from codalab.server.bundle_manager import BundleManager
from codalab.worker.bundle_state import RunResources
from codalab.bundles.run_bundle import RunBundle
from codalab.lib.codalab_manager import CodaLabManager
from collections import namedtuple


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
            cpus=0,
            gpus=0,
            docker_image='',
            time=100,
            memory=1000,
            disk=1000,
            network=False,
            queue=None,
            runs_left=None,
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
                'free_disk_bytes': 4 * 1000,
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
                'free_disk_bytes': 4 * 1000,
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
                'free_disk_bytes': 4 * 1000,
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
                'free_disk_bytes': 4 * 1000,
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
                'free_disk_bytes': 4 * 1000,
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
                'free_disk_bytes': 2 * 1000,
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
                'free_disk_bytes': 2 * 1000,
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
                'free_disk_bytes': 2 * 1000,
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
        self.bundle_resources.queue = "tag=worker_X"
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

    def test_worker_to_run_resources(self):
        worker = self.workers_list[0]
        worker_resources = BundleManager._worker_to_run_resources(worker)

        self.assertEqual(worker['tag'], worker_resources.queue)
        self.assertEqual(worker['cpus'], worker_resources.cpus)
        self.assertEqual(worker['gpus'], worker_resources.gpus)
        self.assertEqual(worker['memory_bytes'], worker_resources.memory)
        self.assertEqual(worker['free_disk_bytes'], worker_resources.disk)
        self.assertEqual(worker['exit_after_num_runs'], worker_resources.runs_left)

    def test_get_dominating_workers(self):
        bundle_resources = RunResources(
            queue='',
            cpus=1,
            gpus=0,
            memory=1,
            disk=1,
            docker_image='',
            runs_left=None,
            time=None,
            network=None,
        )
        dominating_workers = self.bundle_manager._get_dominating_workers(
            bundle_resources, self.workers_list
        )
        self.assertEqual(len(dominating_workers), 7)

    def test_get_dominating_workers_strict(self):
        worker = self.workers_list[0]
        worker_resources = BundleManager._worker_to_run_resources(worker)
        workers_list = [worker]
        dominating_workers = self.bundle_manager._get_dominating_workers(
            worker_resources, workers_list, True
        )
        self.assertEqual(len(dominating_workers), 0)

    def test_get_dominating_workers_none_exist(self):
        bundle_resources = RunResources(
            queue='',
            cpus=10000000,  # no workers dominate this
            gpus=0,
            memory=1,
            disk=1,
            docker_image='',
            runs_left=None,
            time=None,
            network=None,
        )
        dominating_workers = self.bundle_manager._get_dominating_workers(
            bundle_resources, self.workers_list
        )
        self.assertEqual(len(dominating_workers), 0)

    def test_get_resource_recommendations_multiple_workers(self):
        bundle_resources = RunResources(
            queue='test-queue',
            cpus=2,
            gpus=3,
            memory=800000,
            disk=900000,
            docker_image='',
            runs_left=None,
            time=None,
            network=None,
        )
        workers_list = [
            self.workers_list[0],
            self.workers_list[1],
            self.workers_list[2],
        ]
        recommendations = self.bundle_manager._get_resource_recommendations(
            bundle_resources, workers_list
        )
        expected = (
            'Available resources: No queue (test-queue requested), 2 GPUs (3 '
            'requested), 3.9k memory (781k requested), 3.9k disk (878k requested) '
            'or No queue (test-queue requested), 1 GPUs (3 requested), 3.9k memory '
            '(781k requested), 3.9k disk (878k requested) or No queue (test-queue '
            'requested), 0 GPUs (3 requested), 3.9k memory (781k requested), 3.9k '
            'disk (878k requested)'
        )
        self.assertEqual(recommendations, expected)

    def test_get_resource_recommendations_single_worker(self):
        bundle_resources = RunResources(
            queue='test-queue',
            cpus=2,
            gpus=3,
            memory=800000,
            disk=900000,
            docker_image='',
            runs_left=None,
            time=None,
            network=None,
        )
        workers_list = [self.workers_list[0]]
        recommendations = self.bundle_manager._get_resource_recommendations(
            bundle_resources, workers_list
        )
        self.assertEqual(
            recommendations,
            'Available resources: No queue (test-queue requested), 2 GPUs (3 requested), 3.9k memory (781k requested), 3.9k disk (878k requested)',
        )

    def test_get_resource_recommendations_no_workers(self):
        bundle_resources = RunResources(
            queue='test-queue',
            cpus=2,
            gpus=3,
            memory=800000,
            disk=900000,
            docker_image='',
            runs_left=None,
            time=None,
            network=None,
        )
        workers_list = []
        recommendations = self.bundle_manager._get_resource_recommendations(
            bundle_resources, workers_list
        )
        self.assertEqual(recommendations, '')
