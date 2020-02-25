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

    def tearDown(self):
        del self.bundle_manager
        del self.codalab_manager

    def get_sample_workers_list(self):
        workers_list = [
            {
                'worker_id': 1,
                'cpus': 6,
                'gpus': 1,
                'memory_bytes': 4 * 1000,
                'tag': None,
                'run_uuids': [],
                'dependencies': [],
                'shared_file_system': False,
            },
            {
                'worker_id': 2,
                'cpus': 4,
                'gpus': 0,
                'memory_bytes': 4 * 1000,
                'tag': None,
                'run_uuids': [],
                'dependencies': [],
                'shared_file_system': False,
            },
            {
                'worker_id': 3,
                'cpus': 4,
                'gpus': 0,
                'memory_bytes': 2 * 1000,
                'tag': None,
                'run_uuids': [],
                'dependencies': [],
                'shared_file_system': False,
            },
        ]
        return workers_list

    def test__filter_and_sort_workers(self):
        bundle = Mock(spec=RunBundle, metadata=Mock(spec=MetadataSpec))
        bundle.dependencies = []
        bundle.metadata.request_queue = None
        bundle_resources = RunResources(
            cpus=1, gpus=0, docker_image='', time=100, memory=1000, disk=1000, network=False
        )

        # gpu worker should be last in the filtered and sorted list
        sorted_workers_list = self.bundle_manager._filter_and_sort_workers(
            self.get_sample_workers_list(), bundle, bundle_resources
        )
        self.assertEqual(len(sorted_workers_list), 3)
        self.assertEqual(sorted_workers_list[0]['gpus'], 0)
        self.assertEqual(sorted_workers_list[1]['gpus'], 0)
        self.assertEqual(sorted_workers_list[-1]['gpus'], 1)

        # gpu worker should be the only worker in the filtered and sorted list
        bundle_resources.gpus = 1
        sorted_workers_list = self.bundle_manager._filter_and_sort_workers(
            self.get_sample_workers_list(), bundle, bundle_resources
        )
        self.assertEqual(len(sorted_workers_list), 1)
        self.assertEqual(sorted_workers_list[0]['gpus'], 1)
