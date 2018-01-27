import unittest
from codalabworker.aws_batch import *
from run_test import RunManagerBaseTestMixin


class AwsBatchRunManagerTest(RunManagerBaseTestMixin, unittest.TestCase):
    def create_run_manager(self):
        # TODO Mock these out better
        batch_client = object()
        queue_name = 'test'
        bundle_service = object()
        worker = object()
        return AwsBatchRunManager(batch_client, queue_name, bundle_service, worker)
