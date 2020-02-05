import unittest
from mock import Mock

from codalab.objects.metadata_spec import MetadataSpec
from codalab.server.bundle_manager import BundleManager
from codalab.worker.bundle_state import RunResources
from codalab.bundles import RunBundle
from codalab.lib.codalab_manager import CodaLabManager


class BundleManagerTest(unittest.TestCase):
    def setUp(self):
        self.codalab_manager = Mock(CodaLabManager)
        self.codalab_manager.config = {
            "workers": {
                'default_cpu_image': 'codalab/default-cpu:latest',
                'default_gpu_image': 'codalab/default-gpu:latest',
            }
        }
        self.bundle_manager = BundleManager(self.codalab_manager)
        self.bundle = Mock(spec=RunBundle, metadata=Mock(spec=MetadataSpec))
        self.bundle.dependencies = []
        self.bundle.metadata.request_queue = None
        self.bundle_resources = RunResources(
            cpus=0, gpus=0, docker_image='', time=100, memory=1000, disk=1000, network=False
        )
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
                'gpus': 1,
                'memory_bytes': 4 * 1000,
                'tag': None,
                'run_uuids': [1, 2],
                'dependencies': [],
                'shared_file_system': False,
            },
            {
                'worker_id': 1,
                'cpus': 4,
                'gpus': 1,
                'memory_bytes': 4 * 1000,
                'tag': None,
                'run_uuids': [1, 2, 3],
                'dependencies': [],
                'shared_file_system': False,
            },
            {
                'worker_id': 2,
                'cpus': 4,
                'gpus': 1,
                'memory_bytes': 4 * 1000,
                'tag': None,
                'run_uuids': [],
                'dependencies': [],
                'shared_file_system': False,
            },
            {
                'worker_id': 3,
                'cpus': 6,
                'gpus': 0,
                'memory_bytes': 4 * 1000,
                'tag': None,
                'run_uuids': [1],
                'dependencies': [],
                'shared_file_system': False,
            },
            {
                'worker_id': 4,
                'cpus': 6,
                'gpus': 0,
                'memory_bytes': 2 * 1000,
                'tag': None,
                'run_uuids': [],
                'dependencies': [],
                'shared_file_system': False,
            },
        ]
        return workers_list

    def test__filter_and_sort_workers_gpus(self):
        # Only GPU workers should appear from the returning sorted worker list
        self.bundle_resources.gpus = 1
        sorted_workers_list = BundleManager._filter_and_sort_workers(
            self.workers_list, self.bundle, self.bundle_resources
        )
        self.assertEqual(len(sorted_workers_list), 3)
        self.assertEqual(sorted_workers_list[0]['worker_id'], 2)
        self.assertEqual(sorted_workers_list[1]['worker_id'], 0)
        self.assertEqual(sorted_workers_list[-1]['worker_id'], 1)

    def test__filter_and_sort_workers_cpus(self):
        # CPU workers should appear in the top from the returning sorted worker list
        self.bundle_resources.cpus = 1
        sorted_workers_list = BundleManager._filter_and_sort_workers(
            self.workers_list, self.bundle, self.bundle_resources
        )
        self.assertEqual(len(sorted_workers_list), 5)
        self.assertEqual(sorted_workers_list[0]['worker_id'], 4)
