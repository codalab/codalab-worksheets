import unittest
from codalabworker.aws_batch import *
from codalabworker.worker import Worker
from codalabworker.bundle_service_client import BundleServiceClient
from run_test import RunManagerBaseTestMixin
import mock


class AwsBatchRunManagerTest(RunManagerBaseTestMixin, unittest.TestCase):
    def create_run_manager(self):
        # TODO Mock these out better
        batch_client = mock.MagicMock()
        queue_name = 'test'
        bundle_service = mock.create_autospec(BundleServiceClient)
        worker = mock.create_autospec(Worker)
        return AwsBatchRunManager(batch_client, queue_name, bundle_service, worker)


def create_state(NewState):
    worker = mock.create_autospec(Worker)
    worker.id = 'fake worker'
    kwargs = {
        'bundle': {'uuid': 'fake_uuid', 'command': 'echo fake command', 'metadata': {}},
        'batch_client': mock.MagicMock(),
        'queue_name': 'fake queue',
        'worker': worker,
        'bundle_service': mock.create_autospec(BundleServiceClient),
        'bundle_path': '/tmp/foo',  # TODO Probably have a random path joined with test name
        'resources': {'docker_image': 'fake image'},
        'dependencies': {}
    }
    return NewState(**kwargs)


class InitialStateTest(unittest.TestCase):
    def test_transition(self):
        state = create_state(Initial)
        self.assertIsInstance(state.update(), Setup, "when no previous state: initial -> setup")
        state = create_state(Initial)
        state.metadata['batch_job_id'] = 'fake id'
        self.assertIsInstance(state.update(), Running, "when job id exists: initial -> running ")
        state = create_state(Initial)
        state.metadata['batch_job_definition'] = 'fake arn'
        self.assertIsInstance(state.update(), Submit, "when job id exists: initial -> submit ")


class SetupStateTest(unittest.TestCase):
    def test_setup(self):
        arn = 'fake arn'
        setup = create_state(Setup)
        batch_client = setup._batch_client
        batch_client.register_job_definition.return_value = {'jobDefinitionArn': arn}
        bundle_service = setup._bundle_service

        def update_bundle_metadata(worker_id, bundle_uuid, updates):
            self.assertEqual(updates.get('batch_job_definition'), arn, 'batch_job_definition should be submitted to bundle service')

        bundle_service.update_bundle_metadata.side_effect = update_bundle_metadata

        next_state = setup.update()
        self.assertIsInstance(next_state, Submit)

        batch_client.register_job_definition.assert_called_once()
        bundle_service.update_bundle_metadata.assert_called_once()
        self.assertEqual(next_state.metadata.get('batch_job_definition'), arn, 'batch_job_definition should be set in next state')

    def test_specific_resources(self):
        setup = create_state(Setup)
        job_definition = setup.get_job_definition()
        self.assertGreater(job_definition['containerProperties']['vcpus'], 0, 'must request some vpus')
        self.assertGreater(job_definition['containerProperties']['memory'], 4, 'must request at least 4 mb')
        self.assertEqual('fake image', job_definition['containerProperties']['image'],
                         'must request correct docker image')

        setup.metadata['request_cpus'] = 10
        setup.resources['request_memory'] = 50 * 1024 * 1024  # Codalab specifies in bytes
        job_definition = setup.get_job_definition()
        self.assertEqual(10, job_definition['containerProperties']['vcpus'], 'should request correct number of vcpus')
        self.assertEqual(50, job_definition['containerProperties']['memory'], 'should request correct amount of memory')

    def test_mounting_dependencies(self):
        def check_volume_sanity(job_definition):
            volumes = job_definition['containerProperties']['volumes']
            volume_names = set([v['name'] for v in volumes])
            self.assertEqual(len(volumes), len(volume_names), 'volumes must have unique names')

            mount_points = job_definition['containerProperties']['mountPoints']
            for mount_point in mount_points:
                self.assertTrue(mount_point['sourceVolume'] in volume_names, 'all mount points must be from a volume')

            mount_paths = set([m['containerPath'] for m in mount_points])
            self.assertEqual(len(mount_points), len(mount_paths), 'mount points must have unique paths')

        setup = create_state(Setup)
        job_definition = setup.get_job_definition()
        check_volume_sanity(job_definition)
        self.assertEqual(job_definition['containerProperties']['mountPoints'][0]['containerPath'],
                         '/' + setup._bundle['uuid'], 'bundle should have a mount for itself')

        setup._dependencies = [
            ['/tmp/fsdfd', '/fsdfd', 'fsdfd']
        ]
        job_definition = setup.get_job_definition()
        check_volume_sanity(job_definition)


class SubmitStateTest(unittest.TestCase):
    def test_submit(self):
        state = create_state(Submit)
        state.metadata['batch_job_definition'] = 'fake arn'
        batch_client = state._batch_client
        batch_client.submit_job.return_value = {'jobId': 'fake job'}

        next_state = state.update()

        self.assertIsInstance(next_state, Running, 'should transition to running state')
        batch_client.submit_job.assert_called_once_with(jobName='fake_uuid', jobQueue='fake queue',
                                                          jobDefinition='fake arn')
        self.assertEqual(next_state.metadata.get('batch_job_id'), 'fake job', 'job id should be set in next state')


class RunningStateTest(unittest.TestCase):
    def test_running(self):
        state = create_state(Running)
        state.metadata['batch_job_id'] = 'fake job'
        batch_client = state._batch_client
        batch_client.describe_jobs.return_value = {'jobs': [{
            'status': BatchStatus.Running,
        }]}
        next_state = state.update()

        self.assertIsInstance(next_state, Running, 'should continue running when batch it')
        batch_client.describe_jobs.assert_called_once_with(jobs=['fake job'])

        self.assertTrue(BatchStatus.Running in next_state.metadata['run_status'],
                        'run status should contain the batch status')
        state._bundle_service.update_bundle_metadata.assert_called_once()

    def test_failed(self):
        state = create_state(Running)
        state.metadata['batch_job_id'] = 'fake job'
        batch_client = state._batch_client
        batch_client.describe_jobs.return_value = {'jobs': [{
            'status': BatchStatus.Failed,
        }]}
        next_state = state.update()

        self.assertIsInstance(next_state, Cleanup, 'should continue running when batch it')

    def test_succeeded(self):
        state = create_state(Running)
        state.metadata['batch_job_id'] = 'fake job'
        batch_client = state._batch_client
        batch_client.describe_jobs.return_value = {'jobs': [{
            'status': BatchStatus.Succeeded,
        }]}
        next_state = state.update()

        self.assertIsInstance(next_state, Cleanup, 'should continue running when batch it')


class CleanupStateTest(unittest.TestCase):
    def test_basic(self):
        state = create_state(Cleanup)
        state.metadata['batch_job_definition'] = 'fake def'

        next_state = state.update()

        self.assertIsInstance(next_state, Complete, 'should move to complete state')
        state._batch_client.deregister_job_definition.assert_called_once_with(
            jobDefinition='fake def'
        )


class CompleteStateTest(unittest.TestCase):
    def test_succeeded(self):
        state = create_state(Complete)
        state.metadata['batch_job_id'] = 'fake job'
        worker = state._worker
        worker.shared_file_system = False
        batch_client = state._batch_client
        batch_client.describe_jobs.return_value = {'jobs': [{
            'status': BatchStatus.Succeeded,
            'startedAt': 0,
            'stoppedAt': 100,
            'container': {
                'exitCode': 0
            },
        }]}
        next_state = state.update()
        self.assertIs(next_state, fsm.State.DONE, 'should transition to done')

        batch_client.describe_jobs.assert_called_once_with(jobs=['fake job'])

        bundle_service = state._bundle_service
        bundle_service.update_bundle_metadata.assert_called_once()
        bundle_service.update_bundle_contents.assert_called_once()
        bundle_service.finalize_bundle.assert_called_once_with('fake worker', 'fake_uuid',
                                                               {'exitcode': 0, 'failure_message': None})
        worker.finish_run.assert_called_once_with('fake_uuid')

    def test_failed(self):
        state = create_state(Complete)
        worker = state._worker
        worker.shared_file_system = False
        state.metadata['batch_job_id'] = 'fake job'
        batch_client = state._batch_client
        batch_client.describe_jobs.return_value = {'jobs': [{
            'status': BatchStatus.Failed,
            'container': {
                'exitCode': 1,
                'reason': 'just cause'
            },
        }]}
        state.update()

        bundle_service = state._bundle_service
        bundle_service.finalize_bundle.assert_called_once_with('fake worker', 'fake_uuid',
                                                               {'exitcode': 1, 'failure_message': 'just cause'})
        bundle_service.update_bundle_contents.assert_called_once()
        worker.finish_run.assert_called_once_with('fake_uuid')

    def test_shared_filesystem(self):
        state = create_state(Complete)
        state.metadata['batch_job_id'] = 'fake job'
        worker = state._worker
        worker.shared_file_system = True
        batch_client = state._batch_client
        batch_client.describe_jobs.return_value = {'jobs': [{
            'status': BatchStatus.Succeeded,
            'startedAt': 0,
            'stoppedAt': 100,
            'container': {
                'exitCode': 0
            },
        }]}
        bundle_service = state._bundle_service
        bundle_service.update_bundle_contents.assert_not_called()
