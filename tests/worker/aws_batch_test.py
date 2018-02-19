import unittest
from codalabworker.aws_batch import *
from codalabworker.worker import Worker
from codalabworker.bundle_service_client import BundleServiceClient
from run_test import RunManagerBaseTestMixin
import mock
import re


class AwsBatchRunManagerTest(RunManagerBaseTestMixin, unittest.TestCase):
    def create_run_manager(self):
        # TODO Mock these out better
        batch_client = mock.MagicMock()
        queue_name = 'test'
        bundle_service = mock.create_autospec(BundleServiceClient)
        worker = mock.create_autospec(Worker)
        return AwsBatchRunManager(batch_client, queue_name, bundle_service, worker)

    def test_start(self):
        with mock.patch('threading.Thread') as Thread, \
                mock.patch('socket.gethostname') as gethostname, \
                mock.patch('time.time') as timetime:
            gethostname.return_value = 'fakehost'
            timetime.return_value = 1000

            run_manager = self.create_run_manager()
            run_manager._bundle_service.start_bundle.return_value = True
            run_manager._worker.id = 'fake worker'
            run_manager._worker.shared_file_system = True

            run = run_manager.create_run({'uuid': 'fake_uuid', 'dependencies': []}, '/tmp', {})

            self.assertRaises(AssertionError, run.start)
            run.pre_start()
            run.start()
            run._bundle_service.start_bundle.assert_called_once_with('fake worker', 'fake_uuid',
                                                                     {'hostname': 'fakehost', 'start_time': 1000})
            Thread.assert_called_once()

    def test_post_stop(self):
        with mock.patch('os.rmdir'):
            run_manager = self.create_run_manager()
            run_manager._worker.shared_file_system = False
            run_manager._worker.id = 'fake worker'
            dependencies = [{
                'parent_uuid': 'foo',
                'uuid': 'fake_uuid',
                'parent_path': '/tmp/foo',
                'child_path': 'foo'
            }]
            bundle_path = '/tmp/fake_uuid'
            run = run_manager.create_run({'uuid': 'fake_uuid', 'dependencies': dependencies}, bundle_path, {})
            run.post_stop()
            os.rmdir.assert_called_with('%s/%s' % (bundle_path, 'foo'))
            run_manager._worker.remove_dependency.assert_called_once_with('foo', '/tmp/foo', 'fake_uuid')

            run_manager._bundle_service.update_bundle_contents.assert_called_once()

def create_state(NewState):
    worker = mock.create_autospec(Worker)
    worker.id = 'fake worker'
    kwargs = {
        'bundle': {'uuid': 'fake_uuid', 'command': 'echo fake command', 'metadata': {}},
        'batch_client': mock.MagicMock(),
        'queue_name': 'fake queue',
        'worker': worker,
        'bundle_service': mock.create_autospec(BundleServiceClient),
        'bundle_path': '/tmp/fake_uuid',  # TODO Probably have a random path joined with test name
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
            self.assertEqual(updates.get('batch_job_definition'), arn,
                             'batch_job_definition should be submitted to bundle service')

        bundle_service.update_bundle_metadata.side_effect = update_bundle_metadata

        next_state = setup.update()
        self.assertIsInstance(next_state, Submit)

        batch_client.register_job_definition.assert_called_once()
        bundle_service.update_bundle_metadata.assert_called_once()
        self.assertEqual(next_state.metadata.get('batch_job_definition'), arn,
                         'batch_job_definition should be set in next state')

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
                name = mount_point['sourceVolume']
                self.assertTrue(name in volume_names, 'all mount points must be from a volume')
                self.assertTrue(len(name) <= 255, 'volume name %s is longer than 255 characters' % name)
                self.assertTrue(re.match(r'^[a-zA-Z0-9_-]+$', name), 'volume name %s contains illegal characters' % name)

            mount_paths = set([m['containerPath'] for m in mount_points])
            self.assertEqual(len(mount_points), len(mount_paths), 'mount points must have unique paths')

        setup = create_state(Setup)
        job_definition = setup.get_job_definition()
        check_volume_sanity(job_definition)
        self.assertEqual(job_definition['containerProperties']['mountPoints'][0]['containerPath'],
                         '/' + setup._bundle['uuid'], 'bundle should have a mount for itself')

        long_name = ''.join(['ha']*500)
        setup._dependencies = [
            # Normal dependency
            ['/tmp/fsdfd', '/fsdfd', 'fsdfd'],
            # Contains illegal characters
            ['/tmp/foo.bar.*', '/foo.bar.*', 'foo.bar.*'],
            ['/tmp/%s' % long_name, '/%s' % long_name, long_name]
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
        batch_client.submit_job.assert_called_once_with(jobName=batch_name_for_uuid('fake_uuid'), jobQueue='fake queue',
                                                        jobDefinition='fake arn')
        self.assertEqual(next_state.metadata.get('batch_job_id'), 'fake job', 'job id should be set in next state')


class RunningStateTest(unittest.TestCase):
    def create_state(self, **describe_jobs_return):
        state = create_state(Running)
        state._fs_monitor = mock.MagicMock()
        state.metadata['batch_job_id'] = 'fake job'
        batch_client = state._batch_client
        batch_client.describe_jobs.return_value = {'jobs': [describe_jobs_return]}
        return state, batch_client

    def test_fs_monitor_setup(self):
        state = create_state(Running)
        self.assertEqual('/tmp/fake_uuid', state._fs_monitor._fsm._state._bundle_path)

    def test_running(self):
        state, batch_client = self.create_state(status=BatchStatus.Running)
        next_state = state.update()

        self.assertIsInstance(next_state, Running, 'should continue running when batch it')
        batch_client.describe_jobs.assert_called_once_with(jobs=['fake job'])

        self.assertTrue(BatchStatus.Running in next_state.metadata['run_status'],
                        'run status should contain the batch status')
        state._bundle_service.update_bundle_metadata.assert_called_once()

    def test_failed(self):
        state, batch_client = self.create_state(status=BatchStatus.Failed)
        next_state = state.update()

        self.assertIsInstance(next_state, Cleanup, 'should continue running when batch it')

    def test_succeeded(self):
        state, batch_client = self.create_state(status=BatchStatus.Succeeded)
        next_state = state.update()

        self.assertIsInstance(next_state, Cleanup, 'should continue running when batch it')

    def test_too_much_time(self):
        state, batch_client = self.create_state(status=BatchStatus.Running, startedAt=100)
        state._resources['request_time'] = 1
        with mock.patch('time.time') as timetime:
            timetime.return_value = 200
            next_state = state.update()
        self.assertIsInstance(next_state, Running, 'should continue so that batch can handle killing gracefully')
        batch_client.terminate_job.assert_called_once()

    def test_too_much_disk(self):
        state, batch_client = self.create_state(status=BatchStatus.Running)
        state._resources['request_disk'] = 100
        state._fs_monitor.disk_utilization = 200
        next_state = state.update()
        self.assertIsInstance(next_state, Running, 'should continue so that batch can handle killing gracefully')
        batch_client.terminate_job.assert_called_once()


class CleanupStateTest(unittest.TestCase):
    def test_basic(self):
        state = create_state(Cleanup)
        state.metadata['batch_job_definition'] = 'fake def'

        with mock.patch('os.rmdir'):
            next_state = state.update()

        self.assertIsInstance(next_state, Complete, 'should move to complete state')
        state._batch_client.deregister_job_definition.assert_called_once_with(
            jobDefinition='fake def'
        )

    def test_dependencies_cleanup(self):
        state = create_state(Cleanup)
        state.metadata['batch_job_definition'] = 'fake def'
        state._dependencies = [
            ['/tmp/fsdfd', '/fsdfd', 'fsdfd']
        ]


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
