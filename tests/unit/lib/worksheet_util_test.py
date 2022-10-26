import unittest

from codalab.lib.worksheet_util import apply_func, get_editable_metadata_fields
from codalab.bundles.run_bundle import RunBundle


class WorksheetUtilTest(unittest.TestCase):
    def test_apply_func(self):
        """
        Test apply_func for rendering values in worksheets.
        """
        self.assertEqual(apply_func(None, 'hello'), 'hello')
        self.assertEqual(apply_func('[1:2]', 'hello'), 'e')
        self.assertEqual(apply_func('[:2]', 'hello'), 'he')
        self.assertEqual(apply_func('[2:]', 'hello'), 'llo')
        self.assertEqual(
            apply_func('date', '1427467247')[:10], '2015-03-27'
        )  # Don't test time because of time zones
        self.assertEqual(apply_func('duration', '63'), '1m3s')
        self.assertEqual(apply_func('size', '1024'), '1k')
        self.assertEqual(apply_func('s/a/b', 'aa'), 'bb')
        self.assertEqual(apply_func(r's/(.+)\/(.+)/\2\/\1', '3/10'), '10/3')
        self.assertEqual(apply_func('%.2f', '1.2345'), '1.23')

    def test_get_editable_metadata_fields_before_start(self):
        editable_fields = get_editable_metadata_fields(RunBundle, 'staged')

        # editable fields
        self.assertIn('request_docker_image', editable_fields)
        self.assertIn('request_time', editable_fields)
        self.assertIn('request_memory', editable_fields)
        self.assertIn('request_disk', editable_fields)
        self.assertIn('request_cpus', editable_fields)
        self.assertIn('request_gpus', editable_fields)
        self.assertIn('request_queue', editable_fields)
        self.assertIn('request_priority', editable_fields)
        self.assertIn('request_network', editable_fields)
        self.assertIn('exclude_patterns', editable_fields)
        self.assertIn('store', editable_fields)
        self.assertIn('allow_failed_dependencies', editable_fields)
        self.assertIn('name', editable_fields)
        self.assertIn('description', editable_fields)
        self.assertIn('tags', editable_fields)

        # generated fields
        self.assertNotIn('cpu_usage', editable_fields)
        self.assertNotIn('memory_usage', editable_fields)
        self.assertNotIn('actions', editable_fields)
        self.assertNotIn('time', editable_fields)
        self.assertNotIn('time_user', editable_fields)
        self.assertNotIn('time_system', editable_fields)
        self.assertNotIn('memory', editable_fields)
        self.assertNotIn('memory_max', editable_fields)
        self.assertNotIn('started', editable_fields)
        self.assertNotIn('last_updated', editable_fields)
        self.assertNotIn('run_status', editable_fields)
        self.assertNotIn('staged_status', editable_fields)
        self.assertNotIn('time_preparing', editable_fields)
        self.assertNotIn('time_running', editable_fields)
        self.assertNotIn('time_cleaning_up', editable_fields)
        self.assertNotIn('time_uploading_results', editable_fields)
        self.assertNotIn('docker_image', editable_fields)
        self.assertNotIn('exitcode', editable_fields)
        self.assertNotIn('job_handle', editable_fields)
        self.assertNotIn('remote', editable_fields)
        self.assertNotIn('remote_history', editable_fields)
        self.assertNotIn('on_preemptible_worker', editable_fields)
        self.assertNotIn('created', editable_fields)
        self.assertNotIn('data_size', editable_fields)
        self.assertNotIn('failure_message', editable_fields)
        self.assertNotIn('error_traceback', editable_fields)

    def test_get_editable_metadata_fields_after_start(self):
        editable_fields = get_editable_metadata_fields(RunBundle, 'starting')

        # editable fields
        self.assertIn('name', editable_fields)
        self.assertIn('description', editable_fields)
        self.assertIn('tags', editable_fields)

        # editable fields that lock after start
        self.assertNotIn('request_docker_image', editable_fields)
        self.assertNotIn('request_time', editable_fields)
        self.assertNotIn('request_memory', editable_fields)
        self.assertNotIn('request_disk', editable_fields)
        self.assertNotIn('request_cpus', editable_fields)
        self.assertNotIn('request_gpus', editable_fields)
        self.assertNotIn('request_queue', editable_fields)
        self.assertNotIn('request_priority', editable_fields)
        self.assertNotIn('request_network', editable_fields)
        self.assertNotIn('exclude_patterns', editable_fields)
        self.assertNotIn('store', editable_fields)
        self.assertNotIn('allow_failed_dependencies', editable_fields)

        # generated fields
        self.assertNotIn('cpu_usage', editable_fields)
        self.assertNotIn('memory_usage', editable_fields)
        self.assertNotIn('actions', editable_fields)
        self.assertNotIn('time', editable_fields)
        self.assertNotIn('time_user', editable_fields)
        self.assertNotIn('time_system', editable_fields)
        self.assertNotIn('memory', editable_fields)
        self.assertNotIn('memory_max', editable_fields)
        self.assertNotIn('started', editable_fields)
        self.assertNotIn('last_updated', editable_fields)
        self.assertNotIn('run_status', editable_fields)
        self.assertNotIn('staged_status', editable_fields)
        self.assertNotIn('time_preparing', editable_fields)
        self.assertNotIn('time_running', editable_fields)
        self.assertNotIn('time_cleaning_up', editable_fields)
        self.assertNotIn('time_uploading_results', editable_fields)
        self.assertNotIn('docker_image', editable_fields)
        self.assertNotIn('exitcode', editable_fields)
        self.assertNotIn('job_handle', editable_fields)
        self.assertNotIn('remote', editable_fields)
        self.assertNotIn('remote_history', editable_fields)
        self.assertNotIn('on_preemptible_worker', editable_fields)
        self.assertNotIn('created', editable_fields)
        self.assertNotIn('data_size', editable_fields)
        self.assertNotIn('failure_message', editable_fields)
        self.assertNotIn('error_traceback', editable_fields)
