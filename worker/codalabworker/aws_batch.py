import logging
import os
import socket
import time
import re

import fsm
from bundle_service_client import BundleServiceException
from file_util import remove_path
from run import RunManagerBase, RunBase
from filesystem_run import FilesystemRunMixin, FilesystemBundleMonitor
from formatting import *
from worker import VERSION


def current_time():
    """Helper to get the current time as Codalab likes it"""
    return int(time.time())


BYTES_PER_MEGABYTE = parse_size('1m')

# AWS Batch always requires the amount of memory to be specified. We provide a reasonable default here.
BATCH_DEFAULT_MEMORY = parse_size('1g')


class AwsBatchRunManager(RunManagerBase):
    """
    A run manager which schedules runs on the AWS Batch service: https://aws.amazon.com/batch/.
    Batch allows machines to be dynamically allocated on your behalf, we will call these Computes.
    We will refer to the machine you are running this worker code on, as the Worker.

    The Worker essentially serves as an intermediary between Codalab and Batch.
    From the Codalab master's perspective, the Worker is a single large machine.
    The Worker then submits the jobs it receives to Batch which creates new Computes to run the jobs.
    Computes will ultimately be what executes the runs.

    In order to use this run manager, the following things must be true:
    1) The Python package boto3 must be installed.
    2) AWS credentials must be accessible by boto3 which have permission to submit Batch jobs.
     see: http://boto3.readthedocs.io/en/latest/guide/configuration.html
    3) A Batch Job Queue must exist with the name passed in as `queue_name`.
    4) The Job Queue must use a Compute Environment whose AMI has a shared filesystem mounted (e.g. AWS EFS).
       This will cause the Computes created to mount the filesystem.
    5) The shared filesystem must also be mounted on the Worker.
    6) The absolute path to the shared filesystem must be the same on the Worker and the Computes.
    7) The Worker's work-dir, and hence bundle_paths, must be on this shared filesystem.

    If used with the --shared-file-system option, then the shared filesystem from the steps above must be the same
    filesystem that the bundle service has mounted.
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
        return parse_size('1000t')

    @property
    def gpus(self):
        # TODO Compute this from the batch queue
        return 100000

    def create_run(self, bundle, bundle_path, resources):
        resources['request_memory'] = resources.get('request_memory') or BATCH_DEFAULT_MEMORY
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
            'dependencies': run._dependencies
        }
        return data

    def deserialize(self, run_data):
        bundle = run_data['bundle']
        bundle_path = run_data['bundle_path']
        resources = run_data['resources']
        queue_name = run_data['queue_name']
        dependencies = run_data['dependencies']
        run = AwsBatchRun(
            bundle_service=self._bundle_service,
            batch_client=self._batch_client,
            queue_name=queue_name,
            worker=self._worker,
            bundle=bundle,
            bundle_path=bundle_path,
            resources=resources,
            dependencies=dependencies
        )
        return run


class AwsBatchRun(FilesystemRunMixin, RunBase):
    """
    This class manages a single run on AWS Batch.
    This is similar to the standard Run, but all execution happens in a Batch job rather than locally on the worker.

    It also has the added constraint for the shared file system option, that the file system must be shared properly
    on the compute cluster which is serving the Batch queue. The easiest way to achieve this is to create a custom ami
    which mounts your shared file system.
    For an article on how to achieve this, see: https://docs.aws.amazon.com/batch/latest/userguide/create-batch-ami.html
    """
    def __init__(self, bundle_service, batch_client, queue_name, worker, bundle, bundle_path, resources,
                 dependencies=None):
        super(AwsBatchRun, self).__init__()
        assert 'request_memory' in resources, "AWS Batch runs require request_memory to be specified."
        self._bundle_service = bundle_service
        self._batch_client = batch_client
        self._queue_name = queue_name
        self._worker = worker
        self._bundle = bundle
        self._uuid = bundle['uuid']
        self._bundle_path = bundle_path
        self._resources = resources
        self._fsm = None
        # When resuming a bundle, we pass the dependencies in since they have already been setup
        if dependencies:
            self._dependencies = dependencies

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
        assert self._dependencies is not None, \
            "Tried to start FSM before dependencies were setup. Probably pre_start was not called."

        # TODO Much of this setup logic needs deduplicated with Run.
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

        self._start_fsm()
        return True

    def resume(self):
        start_message = {
            'hostname': socket.gethostname(),
            'start_time': current_time(),
        }

        if not self._bundle_service.resume_bundle(self._worker.id, self._uuid, start_message):
            return False
        # The FSM handles transitioning from the initial state to the correct state according to the state of Batch.
        self._start_fsm()
        return True

    def kill(self, reason):
        # Tell Batch to kill the job, but let the run's FSM continue to gracefully stop and cleanup resources
        job_id = self.bundle['metadata'].get('batch_job_id')
        if job_id:
            self._batch_client.terminate_job(jobId=job_id, reason=reason)

    def _start_fsm(self):
        assert self._fsm is None, "FSM was already created."

        # TODO Add a cleanup state which is used when exceptions are thrown from anything
        state = Initial(bundle=self._bundle,
                        batch_client=self._batch_client,
                        queue_name=self._queue_name,
                        worker=self._worker,
                        bundle_service=self._bundle_service,
                        bundle_path=self._bundle_path,
                        resources=self._resources,
                        dependencies=self._dependencies)
        self._fsm = fsm.ThreadedFiniteStateMachine(state)
        self._fsm.start()


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


def batch_name_for_uuid(uuid):
    return 'codalab-worker-%s-%s' % (VERSION, uuid)


class AwsBatchRunState(fsm.State):
    def __init__(self, bundle, batch_client, queue_name, worker, bundle_service, bundle_path, resources, dependencies):
        self._bundle = bundle
        self._batch_client = batch_client
        self._queue_name = queue_name
        self._worker = worker
        self._bundle_service = bundle_service
        self._bundle_path = bundle_path
        self._logger = logging.getLogger(self.uuid)
        self._resources = resources
        self._dependencies = dependencies

    @property
    def update_period(self):
        return 1.0

    @property
    def logger(self):
        return self._logger

    @property
    def uuid(self):
        return self._bundle['uuid']

    @property
    def docker_working_directory(self):
        return '/' + self.uuid

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

    def transition(self, NewState):
        new_state = NewState(bundle=self._bundle,
                             batch_client=self._batch_client,
                             queue_name=self._queue_name,
                             worker=self._worker,
                             bundle_service=self._bundle_service,
                             bundle_path=self._bundle_path,
                             resources=self.resources,
                             dependencies=self._dependencies)
        self.logger.info("Job %s transitioning %s -> %s", self.uuid, self.name, new_state.name)
        return new_state

    def update_metadata(self, **kwargs):
        self.logger.debug("Updating metadata: %s", kwargs)
        # Update the bundle locally
        self.metadata.update(kwargs)
        # Update the bundle on the bundle service
        try:
            self._bundle_service.update_bundle_metadata(self._worker.id, self.uuid, kwargs)
        except BundleServiceException:
            self.logger.exception('Failure updating bundle metadata for bundle.', self.uuid)


class Initial(AwsBatchRunState):
    def update(self):
        # If this job has previously made some progress, then pickup where we left off
        if self.metadata.get('batch_job_id'):
            transition = self.transition(Running)
        elif self.metadata.get('batch_job_definition'):
            transition = self.transition(Submit)
        else:
            transition = self.transition(Setup)

        return transition


class Setup(AwsBatchRunState):
    def update(self):
        # Create job definition
        job_definition = self.get_job_definition()

        self.logger.debug(job_definition)

        response = self._batch_client.register_job_definition(**job_definition)
        job_definition_arn = response['jobDefinitionArn']

        self.logger.debug("Job %s registered job definition arn %s", self.uuid, job_definition_arn)

        self.update_metadata(batch_job_definition=job_definition_arn)

        return self.transition(Submit)

    @property
    def docker_command(self):
        # TODO Cleanup the duplication between here and docker_client
        bash_commands = [
            'cd %s' % self.docker_working_directory,
            '(%s) >stdout 2>stderr' % self._bundle['command'],
            ]

        return ['bash', '-c', '; '.join(bash_commands)]

    def get_job_definition(self):
        """
        Create the Batch job definition.
        Each run has its own job definition which it cleans up when the run is complete.
        """
        dependencies = self._dependencies
        bundle = self._bundle
        # The docker image is always specified
        image = self.resources['docker_image']
        assert image, "Docker image must be specified for submitting to AWS Batch."
        # Default to 100 MB so we have some breathing room.
        memory_bytes = self.resources['request_memory']
        # TODO CPUs should really be in resources, but for some reason it isn't so use it or default in metadata
        cpus = max(self.metadata.get('request_cpus', 0), 1)

        # gpus is optional, if it isn't present then use None
        gpus = self.resources.get('request_gpus')

        # Note: The bundle path MUST be on a shared mount between the worker machine and the compute environment nodes.
        volumes_and_mounts = [self.volume_and_mount(
            host_path=self._bundle_path,
            container_path=self.docker_working_directory,
            name=self.uuid,
            read_only=False
        )]
        volume_names = set()
        max_clean_name_length = 244
        for host_path, docker_path, name in dependencies:
            # Batch has some restrictions with the name, so force it to conform
            # See: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html#volumes
            clean_name = re.sub(r'[^a-zA-Z0-9_-]', '', name)[:max_clean_name_length]

            # Find a unique clean name in case our cleaning created duplicates
            if clean_name in volume_names:
                unique_index = 1

                def unique_name():
                    return ('%d-%s' % (unique_index, clean_name))[:max_clean_name_length]
                while unique_name() in volume_names:
                    unique_index += 1

                clean_name = unique_name()

            volume_names.add(clean_name)

            volumes_and_mounts.append(self.volume_and_mount(
                host_path=host_path,
                container_path=docker_path,
                name='dependency_'+clean_name,
                read_only=True
            ))

        job_definition = {
            'jobDefinitionName': batch_name_for_uuid(self.uuid),
            'type': 'container',
            'parameters': {},
            'containerProperties': {
                'image': image,
                'vcpus': cpus,
                'memory': int(memory_bytes / BYTES_PER_MEGABYTE),
                'command': self.docker_command,
                'volumes': [vol for vol, _ in volumes_and_mounts],
                'environment': [
                    {
                        'name': 'HOME',
                        'value': self.docker_working_directory
                    }
                ],  # TODO Should the env be set?
                'mountPoints': [mount for _, mount in volumes_and_mounts],
                'readonlyRootFilesystem': False,
                'user': 'root'  # TODO Figure out what to do here, running as root is bad for file permissions
            },
            'retryStrategy': {
                'attempts': 1
            }
        }

        if gpus is not None:
            job_definition['containerProperties']['resourceRequirements'] = [{
                'type': 'GPU',
                'value': str(gpus)
            }]

        return job_definition

    def volume_and_mount(self, host_path, container_path, name, read_only):
        volume_definition = {
            'host': {
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
    def update(self):
        job_definition = self.metadata['batch_job_definition']

        # Submit job to AWS Batch
        response = self._batch_client.submit_job(
            jobName=batch_name_for_uuid(self.uuid),
            jobQueue=self.batch_queue,
            jobDefinition=job_definition
        )

        job_id = response['jobId']

        self.update_metadata(batch_job_id=job_id, submit_time=current_time())

        return self.transition(Running)


def fetch_batch_job(batch_client, job_id):
    """Get the batch job or None if it could not be found"""
    response = batch_client.describe_jobs(jobs=[job_id])
    if len(response['jobs']) == 0:
        raise BatchJobNotFound(job_id)
    return response['jobs'][0]


class Running(AwsBatchRunState):
    def __init__(self, *args, **kwargs):
        super(Running, self).__init__(*args, **kwargs)
        self._fs_monitor = FilesystemBundleMonitor(
            bundle_path=self._bundle_path,
            dependencies=self._dependencies
        )

    @property
    def job_id(self):
        return self.metadata['batch_job_id']

    def recover(self, error):
        # If the job can no longer be found, then transition to cleanup.
        # Otherwise, continue to run despite errors.
        # This generally only happens on stale Batch jobs which have been removed ~3 days after they finish
        if isinstance(error, BatchJobNotFound):
            self.logger.error('Running batch job %s could not be found.' % error.job_id)
            self._fs_monitor.stop()
            return self.transition(Cleanup)

        return self

    def update(self):
        if not self._fs_monitor.is_alive:
            self._fs_monitor.start()

        job = fetch_batch_job(self._batch_client, self.job_id)
        status = job['status']
        status_reason = job.get('statusReason')

        now = current_time()
        started = job.get('startedAt')
        runtime = now - (started / 1000) if started else 0

        run_status = 'Batch: %s' % status
        if status_reason:
            run_status = '%s - %s' % (run_status, status_reason)

        disk_utilization = self._fs_monitor.disk_utilization

        # While running, we continually update the metadata to let the master know we are still online
        self.update_metadata(run_status=run_status, last_updated=now, time=runtime, data_size=disk_utilization)

        if status in [BatchStatus.Succeeded, BatchStatus.Failed]:
            self._fs_monitor.stop()
            return self.transition(Cleanup)

        # Check if the job needs killed for resource constraint reasons
        request_time = self._resources.get('request_time')
        request_disk = self._resources.get('request_disk')
        if request_time is not None and runtime > request_time:
            self.kill('Time limit %s exceeded.' % duration_str(request_time))
        elif request_disk is not None and disk_utilization > request_disk:
            self.kill('Disk limit %sb exceeded.' % size_str(request_disk))

        self.keep_alive()

        return self

    @property
    def update_period(self):
        # Update less frequently when in the run state so we don't spam the Batch API quite so much.
        return 5.0

    def kill(self, reason):
        self._batch_client.terminate_job(jobId=self.job_id, reason=reason)

    def keep_alive(self):
        # We call resume periodically as part of the loop while a job is running.
        # This ensures that if the job transitions to the WORKER_OFFLINE state in the master,
        # it is transitioned back to running.
        message = {
            'hostname': socket.gethostname(),
            'start_time': self.metadata.get('started', current_time()),
        }

        if not self._bundle_service.resume_bundle(self._worker.id, self._bundle['uuid'], message):
            # If the call returns False, then the master wants us to stop running so kill ourselves
            self.kill('Master requested we do not resume.')


class Cleanup(AwsBatchRunState):
    def update(self):
        self._batch_client.deregister_job_definition(
            jobDefinition=self.metadata['batch_job_definition']
        )

        return self.transition(Complete)


class Complete(AwsBatchRunState):
    def recover(self, error):
        # If the there was an error, then report that and finish gracefully
        self.update_metadata(run_status='Failed')
        finalize_message = {
            'exitcode': 7,
            'failure_message': 'Could not complete Batch Job for unknown reason.\n\n%s' % str(error)
        }
        self._bundle_service.finalize_bundle(self._worker.id, self.uuid, finalize_message)
        self._worker.finish_run(self.uuid)
        return fsm.State.DONE

    def update(self):
        job = fetch_batch_job(self._batch_client, self.metadata['batch_job_id'])
        status = job['status']
        # The container contains information about the most recent docker container used to run the job
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
                'exitcode': container.get('exitCode', 0),
                'failure_message': container.get('reason')
            }
        elif status == BatchStatus.Failed:
            run_status = 'Failed'
            finalize_message = {
                'exitcode': container.get('exitCode', 1),
                'failure_message': container.get('reason', job.get('statusReason', "Failed for unknown reason."))
            }
        elif status == BatchStatus.Running:
            # If somehow were got to completed but the job is still running, then go back to running
            # This is not at all expected to happen, but if it does then we should at least log and handle it
            self.logger.warning('Completed state reached when Batch job was still running.')
            return self.transition(Running)
        else:
            run_status = 'Failed'
            finalize_message = {
                'exitcode': container.get('exitCode', 2),
                'failure_message': container.get('reason', 'Unexpected Batch status %s during Complete.' % status)
            }

        metadata_updates['run_status'] = run_status
        self.update_metadata(**metadata_updates)

        self._bundle_service.finalize_bundle(self._worker.id, self.uuid, finalize_message)

        # Notify the worker that we are finished
        self._worker.finish_run(self.uuid)

        return fsm.State.DONE


class BatchJobNotFound(Exception):
    """Exception to provide a useful error message when a Batch job cannot be found."""
    def __init__(self, job_id):
        self.job_id = job_id

    def __str__(self):
        return "Could not find Batch Job %s." % self.job_id
