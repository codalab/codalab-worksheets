import unittest
import argparse

from codalab.lib.metadata_util import fill_missing_metadata
from codalab.bundles.run_bundle import RunBundle


class MetadataUtilTest(unittest.TestCase):
    @property
    def test_args(self):
        args = argparse.Namespace()
        args.command = 'run'
        return args

    def test_fill_missing_metadata(self):
        metadata = fill_missing_metadata(RunBundle, self.test_args, {})
        self.assertEqual(metadata['request_memory'], '2g')
        self.assertEqual(metadata['request_cpus'], 1)
        self.assertEqual(metadata['request_gpus'], 0)
        self.assertEqual(metadata['request_network'], True)
        self.assertEqual(metadata['cpu_usage'], 0.0)
        self.assertEqual(metadata['memory_usage'], 0.0)
        self.assertEqual(metadata['exclude_patterns'], [])
        self.assertEqual(metadata['time_cleaning_up'], 0.0)
        self.assertEqual(metadata['time_uploading_results'], 0.0)
        self.assertEqual(metadata['docker_image'], '')
        self.assertEqual(metadata['remote'], '')
        self.assertEqual(metadata['on_preemptible_worker'], False)
        self.assertEqual(metadata['allow_failed_dependencies'], False)
        self.assertEqual(metadata['data_size'], 0)
