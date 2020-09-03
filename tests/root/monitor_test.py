from monitor import get_public_workers

import unittest
import os


class MonitorTest(unittest.TestCase):
    def tearDown(self):
        os.environ["CODALAB_PUBLIC_WORKERS"] = ""

    def test_get_public_workers(self):
        os.environ["CODALAB_PUBLIC_WORKERS"] = "vm-clws-prod-worker-0,vm-clws-prod-worker-1"
        self.assertSetEqual(
            set("vm-clws-prod-worker-0", "vm-clws-prod-worker-1"), get_public_workers()
        )

    def test_get_public_workers_with_emtpy_value(self):
        os.environ["CODALAB_PUBLIC_WORKERS"] = ""
        self.assertSetEqual(set(), get_public_workers())
