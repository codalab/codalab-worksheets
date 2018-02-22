import json
import os
import time
import unittest
from mock import Mock

from codalab.objects.metadata_spec import MetadataSpec
from codalab.worker.bundle_manager import BundleManager
from codalab.bundles import RunBundle

class BundleManagerTest(unittest.TestCase):
    def setUp(self):
        self.manager = BundleManager()
        self.manager._default_request_cpus = 1
        self.manager._default_request_gpus = 0
        self.manager._default_request_queue = None

    def get_sample_workers_list(self):
        workers_list = [
                {
                    'worker_id': 1,
                    'cpuset': range(6),
                    'gpuset': [1],
                    'memory_bytes': 4 * 1000,
                    'tag': None,
                    'run_uuids': [],
                    'dependencies': []
                },
                {
                    'worker_id': 2,
                    'cpuset': range(4),
                    'gpuset': [],
                    'memory_bytes': 4 * 1000,
                    'tag': None,
                    'run_uuids': [],
                    'dependencies': []
                },
                {
                    'worker_id': 3,
                    'cpuset': range(4),
                    'gpuset': [],
                    'memory_bytes': 2 * 1000,
                    'tag': None,
                    'run_uuids': [],
                    'dependencies': []
                }
        ]
        return workers_list

    def tearDown(self):
        pass

    def test__filter_and_sort_workers(self):
        req_cpus, req_gpus, req_mem = 1, 0, '1000'
        bundle = Mock(spec=RunBundle, metadata=Mock(spec=MetadataSpec))
        bundle.dependencies = []
        bundle.metadata.request_cpus = req_cpus
        bundle.metadata.request_gpus = req_gpus
        bundle.metadata.request_memory = req_mem
        bundle.metadata.request_queue = None

        # gpu worker should be last in the filtered and sorted list
        sorted_workers_list = self.manager._filter_and_sort_workers(
                self.get_sample_workers_list(), bundle, req_cpus, req_gpus, req_mem)
        self.assertEqual(len(sorted_workers_list), 3)
        self.assertEqual(len(sorted_workers_list[0]['gpuset']), 0)
        self.assertEqual(len(sorted_workers_list[1]['gpuset']), 0)
        self.assertEqual(len(sorted_workers_list[-1]['gpuset']), 1)

        # gpu worker should be the only worker in the filtered and sorted list
        req_gpus = 1
        bundle.metadata.request_gpus = req_gpus
        sorted_workers_list = self.manager._filter_and_sort_workers(
                self.get_sample_workers_list(), bundle, req_cpus, req_gpus, req_mem)
        self.assertEqual(len(sorted_workers_list), 1)
        self.assertEqual(len(sorted_workers_list[0]['gpuset']), 1)


