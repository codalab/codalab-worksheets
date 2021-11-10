import unittest
from .base import BaseTestCase


class WorkersTest(BaseTestCase):
    def test_checkin(self):
        body = {
            'tag': None,
            'cpus': 6,
            'gpus': 0,
            'memory_bytes': 7560388608,
            'free_disk_bytes': 1664086700032,
            'dependencies': [],
            'hostname': 'b215bfdd92c8',
            'runs': [],
            'shared_file_system': False,
            'tag_exclusive': False,
            'exit_after_num_runs': 999999999,
            'is_terminating': False,
            'preemptible': True,
        }
        response = self.app.post_json('/rest/workers/test_worker/checkin', body)
        self.assertEqual(response.status_int, 200)
        # self.assertEqual(response.body, b'')

    @unittest.skip("not implemented yet")
    def test_checkin_with_run(self):
        """Create a run, and then when the worker checks in, that run should be returned."""
        pass
