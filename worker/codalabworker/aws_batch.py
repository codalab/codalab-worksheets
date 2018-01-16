import logging
import os
import socket
import time

import fsm
from file_util import remove_path
from run_manager import RunManagerBase, RunBase, FilesystemRunMixin

try:
    import boto3
except ImportError:
    print("Missing dependencies, please install boto3 to enable AWS support.")
    import sys

    sys.exit(1)


def parse_int(to_parse, default_value):
    try:
        return int(to_parse)
    except (ValueError, TypeError):
        return default_value


def current_time():
    return int(time.time())


BYTES_PER_MEGABYTE = 1024 * 1024


class AwsBatchRunManager(RunManagerBase):
    def __init__(self, batch_client, queue_name, bundle_service, worker):
        self._bundle_service = bundle_service
        self._queue_name = queue_name
        self._worker = worker
        self._batch_client = batch_client

    @property
    def cpus(self):
        # TODO Compute this from the batch queue
        return 100000

    @property
    def memory_bytes(self):
        # TODO Compute this from the batch queue
        return 1000 * 1000 * BYTES_PER_MEGABYTE

    @property
    def gpus(self):
        # TODO Compute this from the batch queue
        return 0

    def create_run(self, bundle, bundle_path, resources):
        run = AwsBatchRun(
            bundle_service=self._bundle_service,
            batch_client=self._batch_client,
            queue_name=self._queue_name,
            worker=self._worker,
            bundle=bundle,
            bundle_path=bundle_path,
            resources=resources
        )
        return run

    def serialize(self, run):
        assert isinstance(run, AwsBatchRun), "Could not serialize run which was from different run manager."
        data = {
            'bundle': run._bundle,
            'bundle_path': run._bundle_path,
            'queue_name': run._queue_name,
            'resources': run._resources,
        }
        return data

    def deserialize(self, run_data):
        bundle = run_data['bundle']
        bundle_path = run_data['bundle_path']
        resources = run_data['resources']
        queue_name = run_data['queue_name']
        run = AwsBatchRun(
            bundle_service=self._bundle_service,
            batch_client=self._batch_client,
            queue_name=queue_name,
            worker=self._worker,
            bundle=bundle,
            bundle_path=bundle_path,
            resources=resources
        )
        return run


class AwsBatchRun(FilesystemRunMixin, RunBase):
    """
    This class manages a single run on AWS Batch.
    This is similar to the standard Run, but all execution happens in a Batch job rather than locally on the worker.

    It also has the added constraint for the shared file system option, that the file system must be shared properly
    on the compute cluster which is serving the Batch queue.
    For an article on how to achieve this, see:
    """

    def __init__(self, bundle_service, batch_client, queue_name, worker, bundle, bundle_path, resources):
        super(AwsBatchRun, self).__init__()
        self._bundle_service = bundle_service
        self._batch_client = batch_client
        self._queue_name = queue_name
        self._worker = worker
        self._bundle = bundle
        self._uuid = bundle['uuid']
        self._bundle_path = bundle_path
        self._resources = resources
        self._fsm = None

    @property
    def is_shared_file_system(self):
        return self._worker.shared_file_system

    @property
    def bundle(self):
        return self._bundle

    @property
    def resources(self):
        return self._resources

    @property
    def bundle_path(self):
        return self._bundle_path

    def start(self):
        # Report that the bundle is running. We note the start time here for
        # accurate accounting of time used, since the clock on the bundle
        # service and on the worker could be different.
        start_message = {
            'hostname': socket.gethostname(),
            'start_time': current_time(),
        }
        if not self._bundle_service.start_bundle(self._worker.id, self._uuid, start_message):
            return False

        if self.is_shared_file_system:
            # On a shared file system we create the path in the bundle manager
            # to avoid NFS directory cache issues. Here, we wait for the cache
            # on this machine to expire and for the path to appear.
            while not os.path.exists(self._bundle_path):
                time.sleep(0.5)
        else:
            # Set up a directory to store the bundle.
            remove_path(self._bundle_path)
            os.mkdir(self._bundle_path)

        self.create_fsm().start()
        return True

    def resume(self):
        """
        Report that the bundle is running. We note the start time here for
        accurate accounting of time used, since the clock on the bundle
        service and on the worker could be different.
        """
        start_message = {
            'hostname': socket.gethostname(),
            'start_time': current_time(),
        }

        if not self._bundle_service.resume_bundle(self._worker.id, self._uuid, start_message):
            return False
        # TODO Do we need to do anything special here? We already deserialized to the correct state presumably
        self.create_fsm().start()
        return True

    def kill(self):
        if self._fsm:
            self._fsm.stop()

        job_id = self.bundle['metadata'].get('batch_job_id')
        if job_id:
            self._batch_client.terminate_job(jobId=job_id, reason='Codalab kill requested.')

        job_definition = self.bundle['metadata'].get('batch_job_definition')
        if job_definition:
            self._batch_client.deregister_job_definition(jobDefinition=job_definition)


    def create_fsm(self):
        assert self._fsm is None, "FSM was already created."
        # TODO Can this be replaced by just mounting dependencies directly?
        dependencies = self.setup_dependencies()

        # TODO Add a cleanup state which is used when exceptions are thrown from anything
        state = Initial(bundle=self._bundle,
                        batch_client=self._batch_client,
                        queue_name=self._queue_name,
                        worker=self._worker,
                        bundle_service=self._bundle_service,
                        bundle_path=self._bundle_path,
                        resources=self._resources,
                        dependencies=dependencies)
        self._fsm = fsm.ThreadedFiniteStateMachine(state, sleep_time=5.0)
        return self._fsm


def event(name, **payload):
    return {
        'name': name,
        'payload': payload
    }


class Event(object):
    UPDATE_METADATA = 'update_metadata'
    FAILED = 'failed'
    SUCCESS = 'success'
    Kill = 'kill'


class AwsBatchRunState(fsm.State):
    def __init__(self, bundle, batch_client, queue_name, worker, bundle_service, bundle_path, resources, dependencies):
        self._bundle = bundle
        self._batch_client = batch_client if batch_client else boto3.client('batch')
        self._queue_name = queue_name
        self._worker = worker
        self._bundle_service = bundle_service
        self._bundle_path = bundle_path
        self._logger = logging.getLogger(self.uuid)
        self._resources = resources
        self._dependencies = dependencies

    @property
    def logger(self):
        return self._logger

    @property
    def uuid(self):
        return self._bundle['uuid']

    @property
    def batch_queue(self):
        return self._queue_name

    @property
    def is_shared_file_system(self):
        return self._worker.shared_file_system

    @property
    def metadata(self):
        return self._bundle['metadata']

    @property
    def resources(self):
        return self._resources

    @property
    def name(self):
        return self.__class__.__name__

    def transition(self, NewState, outputs=None):
        status_event = event(Event.UPDATE_METADATA, run_status=self.name, last_updated=current_time())
        outputs = outputs + [status_event] if outputs is not None else [status_event]
        new_state = NewState(bundle=self._bundle,
                             batch_client=self._batch_client,
                             queue_name=self._queue_name,
                             worker=self._worker,
                             bundle_service=self._bundle_service,
                             bundle_path=self._bundle_path,
                             resources=self.resources,
                             dependencies=self._dependencies)
        self.logger.info("Job %s transitioning %s -> %s", self.uuid, self.name, new_state.name)
        return new_state, outputs

    def noop(self):
        return self, []

    def update_metadata(self, **kwargs):
        self.logger.debug("Updating metadata: %s", kwargs)
        # Update the bundle locally
        self._bundle['metadata'].update(kwargs)
        # Update the bundle on the bundle service
        self._bundle_service.update_bundle_metadata(self._worker.id, self._bundle['uuid'], kwargs)


# TODO If there is really nothing to do here, then just go straight to setup
class Initial(AwsBatchRunState):
    def update(self, events):
        transition = None
        # If this job has previously made some progress, then pickup where we left off
        if self.metadata.get('batch_job_id'):
            transition = self.transition(Running)
        elif self.metadata.get('batch_job_definition'):
            transition = self.transition(Submit)
        else:
            transition = self.transition(Setup)

        return transition


class Setup(AwsBatchRunState):
    def update(self, events):
        # Create job definition
        job_definition = self.create_job_definition(self._dependencies)

        self.update_metadata(batch_job_definition=job_definition)
        return self.transition(Submit)

    @property
    def docker_image(self):
        return self._bundle['metadata']['docker_image']

    @property
    def docker_working_directory(self):
        return '/' + self.uuid

    @property
    def docker_dependencies_directory(self):
        return '/' + self.uuid + '_dependencies'

    @property
    def shared_batch_efs_directory(self):
        # TODO This is the filesystem shared between the worker and the compute cluster. For now hard-code
        return '/data/codalab-home/bundles'

    @property
    def docker_command(self):
        bash_commands = [
            'cd %s' % self.docker_working_directory,
            '(%s) >stdout 2>stderr' % self._bundle['command'],
            ]

        return ['bash', '-c', '; '.join(bash_commands)]

    def create_job_definition(self, dependencies):
        """
        Create the Batch job definition.
        Each run has its own job definition which it cleans up when the run is complete.
        """
        bundle = self._bundle
        # The docker image is always specified
        image = self.resources['docker_image']
        # Default to 100 MB so we have some breathing room.
        memory_bytes = self.resources.get('request_memory') or 100*BYTES_PER_MEGABYTE
        cpus = max(self.resources.get('cpus', 0), 1)

        # TODO All of this mounting only works on shared file systems.
        #      Figure out a strategy for when this isn't the case (e.g. s3, transfer beforehand, etc)

        volumes_and_mounts = [self.volume_and_mount(
            host_path=os.path.join(self.shared_batch_efs_directory, self.uuid),
            container_path=self.docker_working_directory,
            name=self.uuid,
            read_only=False
        )]
        for host_path, docker_path, uuid in dependencies:
            volumes_and_mounts.append(self.volume_and_mount(
                host_path=host_path,
                container_path=docker_path,
                name='dependency_'+uuid,
                read_only=True
            ))

        job_definition = {
            'jobDefinitionName': bundle['uuid'],
            'type': 'container',
            'parameters': {},
            'containerProperties': {
                'image': image,
                'vcpus': cpus,
                'memory': int(memory_bytes / BYTES_PER_MEGABYTE),
                'command': self.docker_command,
                'volumes': map(lambda pair: pair[0], volumes_and_mounts),
                'environment': [
                    {
                        'name': 'HOME',
                        'value': self.docker_working_directory
                    }
                ],  # TODO Should the env be set?
                'mountPoints': map(lambda pair: pair[1], volumes_and_mounts),
                'readonlyRootFilesystem': False,
                'user': 'root'  # TODO Figure out what to do here
            },
            'retryStrategy': {
                'attempts': 1
            }
        }

        self.logger.debug(job_definition)

        response = self._batch_client.register_job_definition(**job_definition)
        arn = response['jobDefinitionArn']

        self.logger.debug("Job %s registered job definition arn %s", self.uuid, arn)
        return arn

    def volume_and_mount(self, host_path, container_path, name, read_only):
        volume_definition = {
            'host': {
                # TODO Figure out how to break assumption about shared file system
                'sourcePath': host_path
            },
            'name': name
        }

        mount_point = {
            'containerPath': container_path,
            'readOnly': read_only,
            'sourceVolume': name
        }

        return volume_definition, mount_point


class Submit(AwsBatchRunState):
    def update(self, events):
        job_definition = self._bundle['metadata']['batch_job_definition']

        # Submit job to AWS Batch
        response = self._batch_client.submit_job(
            jobName=self.uuid,
            jobQueue=self.batch_queue,
            jobDefinition=job_definition
        )

        job_id = response['jobId']

        self.update_metadata(batch_job_id=job_id, submit_time=current_time())

        return self.transition(Running)


class BatchStatus(object):
    Submitted = 'SUBMITTED'
    Pending = 'PENDING'
    Runnable = 'RUNNABLE'
    Starting = 'STARTING'
    Running = 'RUNNING'
    Succeeded = 'SUCCEEDED'
    Failed = 'FAILED'


class Running(AwsBatchRunState):
    def __init__(self, *args, **kwargs):
        super(Running, self).__init__(*args, **kwargs)
        self._last_check_time = 0
        self._check_frequency = 30  # seconds
        self._job = None

    def should_refresh(self):
        now = current_time()
        if now - self._last_check_time > self._check_frequency:
            self._last_check_time = now
            return True
        return False

    def get_job(self):
        if self.should_refresh() or self._job is None:
            response = self._batch_client.describe_jobs(jobs=[self.metadata['batch_job_id']])
            self._job = response['jobs'][0]
        return self._job

    """
    Waiting for Batch to schedule the job
    """
    def update(self, events):
        job = self.get_job()
        # The contain contains information about the most recent docker container used to run the job
        container = job['container']
        status = job['status']

        now = current_time()
        started = job.get('startedAt')
        stopped = job.get('stoppedAt', now * 1000)
        runtime = (stopped - started if started else 0) / 1000

        finalize_message = None
        run_status = 'Batch Status: %s' % status

        if status == BatchStatus.Failed:
            run_status = 'Failed'
            finalize_message = {
                'exitcode': container.get('exitCode', 1),
                'failure_message': container.get('reason', job.get('statusReason', "Failed for unknown reason."))
            }
        elif status == BatchStatus.Succeeded:
            run_status = 'Succeeded'
            finalize_message = {
                'exitcode': container.get('exitCode'),
                'failure_message': container.get('reason')
            }

        self.update_metadata(run_status=run_status, last_updated=now, time=runtime)

        if finalize_message:
            self._bundle_service.finalize_bundle(self._worker.id, self.uuid, finalize_message)
            return self.transition(Cleanup)

        return self.noop()


class Cleanup(AwsBatchRunState):
    def update(self, events):
        self._batch_client.deregister_job_definition(
            jobDefinition=self.metadata['batch_job_definition']
        )

        # TODO Cleanup <bundle_uuid>_dependencies folder
        return self.transition(Complete)


class Complete(AwsBatchRunState):
    def update(self, events):
        self._worker.finish_run(self.uuid)
        # TODO Maybe we should set the final success/failed status here instead of above
        return None, []

