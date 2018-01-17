import logging
import os
import socket
import time

import fsm
from file_util import remove_path
from run_manager import RunManagerBase, RunBase, FilesystemRunMixin
from formatting import size_str

try:
    import boto3
except ImportError:
    print("Missing dependencies, please install boto3 to enable AWS support.")
    import sys

    sys.exit(1)


def current_time():
    """Helper to get the current time as Codalab likes it"""
    return int(time.time())


BYTES_PER_MEGABYTE = 1024 * 1024


class AwsBatchRunManager(RunManagerBase):
    """
    A run manager which schedules runs on the AWS Batch service: https://aws.amazon.com/batch/.
    Batch allows machines to be dynamically allocated on your behalf.
    These machines will ultimately be what executes the runs created by this run manager.

    In order to use this run manager, the following things must be true:
    1) The Python package boto3 must be installed
    2) AWS credentials must be accessible by boto3 which have permission to submit Batch jobs
    3) A Batch Job Queue must exist with the name `queue_name`
    4) The Job Queue must use a Compute Environment whose AMI has a shared filesystem mounted (e.g. AWS EFS)
    5) The shared filesystem must also be mounted on the worker machine
    6) The absolute path to the shared filesystem must be the same on the worker and Compute Environment machines
    7) The workers work-dir, and hence bundle_paths, must be on this shared filesystem

    If used with the worker --shared-file-system, then that filesystem must be mounted by both the worker and the
    Compute Environment rather then requirements 5-7 above.
    """
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

    def download_dependency(self, uuid, path):
        # TODO Implement a better shared spot for this
        def update_status_and_check_killed(bytes_downloaded):
            logging.debug('Downloading dependency %s/%s: %s done (archived size)' %
                          (uuid, path, size_str(bytes_downloaded)))
        dependency_path = self._worker.add_dependency(uuid, path, self._uuid, update_status_and_check_killed)
        return dependency_path

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


class BatchStatus(object):
    """
    Constants for the statuses a Batch job can be in.
    see: https://docs.aws.amazon.com/batch/latest/APIReference/API_JobDetail.html
    """
    Submitted = 'SUBMITTED'
    Pending = 'PENDING'
    Runnable = 'RUNNABLE'
    Starting = 'STARTING'
    Running = 'RUNNING'
    Succeeded = 'SUCCEEDED'
    Failed = 'FAILED'


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
        new_state = NewState(bundle=self._bundle,
                             batch_client=self._batch_client,
                             queue_name=self._queue_name,
                             worker=self._worker,
                             bundle_service=self._bundle_service,
                             bundle_path=self._bundle_path,
                             resources=self.resources,
                             dependencies=self._dependencies)
        self.logger.info("Job %s transitioning %s -> %s", self.uuid, self.name, new_state.name)
        return new_state, []

    def noop(self):
        return self, []

    def update_metadata(self, **kwargs):
        self.logger.debug("Updating metadata: %s", kwargs)
        # Update the bundle locally
        self._bundle['metadata'].update(kwargs)
        # Update the bundle on the bundle service
        self._bundle_service.update_bundle_metadata(self._worker.id, self._bundle['uuid'], kwargs)


class Initial(AwsBatchRunState):
    def update(self, events):
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

        # Note: The bundle path MUST be on a shared mount between the worker machine and the compute environment nodes.
        volumes_and_mounts = [self.volume_and_mount(
            host_path=self._bundle_path,
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


class JobFetcher(object):
    def __init__(self, job_id, batch_client):
        self._last_check_time = 0
        self._check_frequency = 30  # seconds
        self._job = None
        self._job_id = job_id
        self._batch_client = batch_client

    def should_refresh(self):
        now = current_time()
        if now - self._last_check_time > self._check_frequency:
            self._last_check_time = now
            return True
        return False

    def fetch_job(self):
        if self.should_refresh() or self._job is None:
            response = self._batch_client.describe_jobs(jobs=[self._job_id])
            self._job = response['jobs'][0]
        return self._job


class Running(AwsBatchRunState):
    def __init__(self, *args, **kwargs):
        super(Running, self).__init__(*args, **kwargs)
        self._job_fetcher = JobFetcher(self._bundle['metadata']['batch_job_id'], self._batch_client)

    """
    Waiting for Batch to schedule the job
    """
    def update(self, events):
        job = self._job_fetcher.fetch_job()
        status = job['status']

        now = current_time()
        started = job.get('startedAt')
        runtime = now - (started / 1000) if started else 0

        run_status = 'Batch Status: %s' % status

        self.update_metadata(run_status=run_status, last_updated=now, time=runtime)

        if status in [BatchStatus.Succeeded, BatchStatus.Failed]:
            return self.transition(Cleanup)

        return self.noop()


class Cleanup(AwsBatchRunState):
    def update(self, events):
        self._batch_client.deregister_job_definition(
            jobDefinition=self.metadata['batch_job_definition']
        )

        # TODO Cleanup the empty directories made by mounting the dependencies
        return self.transition(Complete)


class Complete(AwsBatchRunState):
    def __init__(self, *args, **kwargs):
        super(Complete, self).__init__(*args, **kwargs)
        self._job_fetcher = JobFetcher(self._bundle['metadata']['batch_job_id'], self._batch_client)

    def update(self, events):
        job = self._job_fetcher.fetch_job()
        status = job['status']
        # The contain contains information about the most recent docker container used to run the job
        container = job['container']

        started = job.get('startedAt')
        stopped = job.get('stoppedAt')

        metadata_updates = {
            'last_updated': current_time()
        }

        if started and stopped:
            metadata_updates['time'] = (stopped - started) / 1000

        if status == BatchStatus.Succeeded:
            run_status = 'Succeeded'
            finalize_message = {
                'exitcode': container.get('exitCode'),
                'failure_message': container.get('reason')
            }
        else:
            run_status = 'Failed'
            finalize_message = {
                'exitcode': container.get('exitCode', 1),
                'failure_message': container.get('reason', job.get('statusReason', "Failed for unknown reason."))
            }

        metadata_updates['run_status'] = run_status
        self.update_metadata(**metadata_updates)

        # Upload the data if needed
        if not self._worker.shared_file_system:
            self.logger.debug('Uploading results for run with UUID %s', self.uuid)

            def update_status(bytes_uploaded):
                self.logger.debug('Uploading results: %s done (archived size)' % size_str(bytes_uploaded))

            self._bundle_service.update_bundle_contents(self._worker.id, self.uuid, self._bundle_path, update_status)

        self._bundle_service.finalize_bundle(self._worker.id, self.uuid, finalize_message)

        # Notify the worker that we are finished
        self._worker.finish_run(self.uuid)

        return None, []
