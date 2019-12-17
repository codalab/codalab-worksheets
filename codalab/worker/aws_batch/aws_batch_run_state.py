from collections import namedtuple
import logging
import os
import re
import threading
import time
import traceback

from codalab.lib.formatting import size_str, duration_str, parse_size
from codalab.worker.docker_utils import get_bundle_docker_command
from codalab.worker.file_util import get_path_size
from codalab.worker.bundle_state import State
from codalab.worker.fsm import StateTransitioner
from codalab.worker.worker_thread import ThreadDict

logger = logging.getLogger(__name__)


class AWSBatchStatus(object):
    """
    Constants for the statuses a Batch job can be in.
    see: https://docs.aws.amazon.com/batch/latest/APIReference/API_JobDetail.html
    """

    SUBMITTED = 'SUBMITTED'
    PENDING = 'PENDING'
    RUNNABLE = 'RUNNABLE'
    STARTING = 'STARTING'
    RUNNING = 'RUNNING'
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'


class AWSBatchRunStage(object):
    """
    Defines the finite set of possible stages and transition functions
    Note that it is important that each state be able to be re-executed
    without unintended adverse effects (which happens upon worker resume)
    """

    WORKER_STATE_TO_SERVER_STATE = {}

    """
    This stage is while we're checking on the job for the first time on Batch
    """
    INITIALIZING = 'AWS_BATCH_RUN.INITIALIZING'
    WORKER_STATE_TO_SERVER_STATE[INITIALIZING] = State.PREPARING

    """
    This stage is for creating and submitting a job definition to Batch
    """
    SETTING_UP = 'AWS_BATCH_RUN.SETTING_UP'
    WORKER_STATE_TO_SERVER_STATE[SETTING_UP] = State.PREPARING

    """
    This stage is for submitting the job to Batch
    """
    SUBMITTING = 'AWS_BATCH_RUN.SUBMITTING'
    WORKER_STATE_TO_SERVER_STATE[SUBMITTING] = State.PREPARING

    """
    Running encompasses the state where the user's job is running
    """
    RUNNING = 'AWS_BATCH_RUN.RUNNING'
    WORKER_STATE_TO_SERVER_STATE[RUNNING] = State.RUNNING

    """
    This stage encompasses cleaning up intermediary components like
    the dependency symlinks and also the releasing of dependencies
    """
    CLEANING_UP = 'AWS_BATCH_RUN.CLEANING_UP'
    WORKER_STATE_TO_SERVER_STATE[CLEANING_UP] = State.RUNNING

    """
    Finalizing means the worker is finalizing the bundle metadata with the server
    """
    FINALIZING = 'LOCAL_RUN.FINALIZING'
    WORKER_STATE_TO_SERVER_STATE[FINALIZING] = State.FINALIZING

    """
    Finished means the worker is done with this run
    """
    FINISHED = 'LOCAL_RUN.FINISHED'
    WORKER_STATE_TO_SERVER_STATE[FINISHED] = State.READY


AWSBatchRunState = namedtuple(
    'AWSBatchRunState',
    [
        'stage',  # AWSBatchRunStage
        'is_killed',  # bool
        'is_finalized',  # bool
        'is_finished',  # bool
        'bundle',  # BundleInfo
        'resources',  # RunResources
        'run_status',  # Optional[str]
        'bundle_dir_wait_num_tries',  # int
        'batch_job_definition',  # Optional[str]
        'batch_job_id',  # Optional[str]
        'disk_utilization',  # int
        'failure_message',  # Optional[str]
        'kill_message',  # Optional[str]
    ],
)


class AWSBatchRunStateMachine(StateTransitioner):
    """
    Manages the state machine of the runs running on an AWS Batch queue

    Note that in general there are two types of errors:
    - User errors (fault of bundle) - we fail the bundle (move to CLEANING_UP state).
    - System errors (fault of worker) - we freeze this worker (Exception is thrown up).
    It's not always clear where the line is.
    """

    BYTES_PER_MEGABYTE = parse_size('1m')

    def __init__(self, batch_client, batch_queue):
        super(AWSBatchRunStateMachine, self).__init__()

        self._batch_client = batch_client
        self._batch_queue = batch_queue

        # bundle.uuid -> {'thread': Thread, 'disk_utilization': int, 'running': bool}
        self.disk_utilization = ThreadDict(
            fields={'disk_utilization': 0, 'running': True, 'lock': None}
        )

        self.add_transition(AWSBatchRunStage.INITIALIZING, self._transition_from_INITIALIZING)
        self.add_transition(AWSBatchRunStage.SETTING_UP, self._transition_from_SETTING_UP)
        self.add_transition(AWSBatchRunStage.SUBMITTING, self._transition_from_SUBMITTING)
        self.add_transition(AWSBatchRunStage.RUNNING, self._transition_from_RUNNING)
        self.add_transition(AWSBatchRunStage.CLEANING_UP, self._transition_from_CLEANING_UP)
        self.add_transition(AWSBatchRunStage.FINALIZING, self._transition_from_FINALIZING)
        self.add_terminal(AWSBatchRunStage.FINISHED)

    def stop(self):
        for uuid in self.disk_utilization.keys():
            self.disk_utilization[uuid]['running'] = False
        self.disk_utilization.stop()

    def _transition_from_INITIALIZING(self, run_state):
        if not os.path.exists(run_state.bundle.path):
            if run_state.bundle_dir_wait_num_tries == 0:
                message = (
                    "Bundle directory cannot be found on the shared filesystem. "
                    "Please ensure the shared fileystem between the server and "
                    "your worker is mounted properly or contact your administrators."
                )
                logger.error(message)
                return run_state._replace(
                    stage=AWSBatchRunStage.FINALIZING, failure_message=message
                )
            return run_state._replace(
                run_status="Waiting for bundle directory to be created by the server",
                bundle_dir_wait_num_tries=run_state.bundle_dir_wait_num_tries - 1,
            )
        return run_state._replace(stage=AWSBatchRunStage.SETTING_UP)

    def _transition_from_SETTING_UP(self, run_state):
        job_definition = self._get_job_definition(run_state)
        response = self._batch_client.register_job_definition(**job_definition)
        job_definition_arn = response['jobDefinitionArn']
        return run_state._replace(
            stage=AWSBatchRunStage.SUBMITTING, batch_job_definition=job_definition_arn
        )

    def _get_job_definition(self, run_state):
        """
        Create the Batch job definition.
        Each run has its own job definition which it cleans up when the run is complete.
        """

        # Note: The bundle path MUST be on a shared mount between the worker machine and the compute environment nodes.
        volumes_and_mounts = [
            self._volume_and_mount(
                host_path=run_state.bundle.path,
                container_path='/%s' % run_state.bundle.uuid,
                name=run_state.bundle.uuid,
                read_only=False,
            )
        ]
        volume_names = set()
        max_clean_name_length = 244
        for host_path, docker_path, name in run_state.bundle.dependencies:
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

            volumes_and_mounts.append(
                self._volume_and_mount(
                    host_path=host_path,
                    container_path=docker_path,
                    name='dependency_' + clean_name,
                    read_only=True,
                )
            )

        job_definition = {
            'jobDefinitionName': 'codalab-worker-%s' % run_state.bundle.uuid,
            'type': 'container',
            'parameters': {},
            'containerProperties': {
                'image': run_state.resources.docker_image,
                'vcpus': run_state.resources.cpus,
                'memory': int(run_state.resources.memory_bytes / self.BYTES_PER_MEGABYTE),
                'command': get_bundle_docker_command(run_state.bundle.command),
                'volumes': [vol for vol, _ in volumes_and_mounts],
                'environment': [
                    {'name': 'HOME', 'value': '/%s' % run_state.bundle.uuid}
                ],  # TODO Should the env be set?
                'mountPoints': [mount for _, mount in volumes_and_mounts],
                'readonlyRootFilesystem': False,
                'user': 'root',  # TODO Figure out what to do here, running as root is bad for file permissions
            },
            'retryStrategy': {'attempts': 1},
        }

        if run_state.resources.gpus > 0:
            job_definition['containerProperties']['resourceRequirements'] = [
                {'type': 'GPU', 'value': str(run_state.resources.gpus)}
            ]

        return job_definition

    @staticmethod
    def _volume_and_mount(host_path, container_path, name, read_only):
        volume_definition = {'host': {'sourcePath': host_path}, 'name': name}

        mount_point = {'containerPath': container_path, 'readOnly': read_only, 'sourceVolume': name}

        return volume_definition, mount_point

    def _transition_from_SUBMITTING(self, run_state):
        response = self._batch_client.submit_job(
            jobName='codalab-worker-%s' % run_state.bundle.uuid,
            jobQueue=self._batch_queue,
            jobDefinition=run_state.batch_job_definition,
        )
        return run_state._replace(stage=AWSBatchRunStage.RUNNING, batch_job_id=response['jobId'])

    def _transition_from_RUNNING(self, run_state):
        def check_disk_utilization():
            running = True
            while running:
                start_time = time.time()
                try:
                    disk_utilization = get_path_size(run_state.bundle.path)
                    self.disk_utilization[run_state.bundle.uuid][
                        'disk_utilization'
                    ] = disk_utilization
                    running = self.disk_utilization[run_state.bundle.uuid]['running']
                except Exception:
                    logger.error(traceback.format_exc())
                end_time = time.time()
                # To ensure that we don't hammer the disk for this computation when
                # there are lots of files, we run it at most 10% of the time.
                time.sleep(max((end_time - start_time) * 10, 1.0))

        def check_run_status(run_state, batch_job):
            job_status = batch_job['status']
            job_status_reason = batch_job.get('statusReason', "")
            run_status = 'Batch: %s. %s' % (job_status, job_status_reason)
            if job_status == AWSBatchStatus.FAILED:
                run_state = run_state._replace(failure_message=job_status_reason, is_finished=True)
            elif job_status == AWSBatchStatus.SUCCEEDED:
                run_state = run_state._replace(is_finished=True)
            run_state = run_state._replace(run_status=run_status)
            return run_state

        def check_resource_utilization(run_state, batch_job):
            kill_messages = []

            run_state = run_state._replace(
                disk_utilization=self.disk_utilization[run_state.bundle.uuid]['disk_utilization']
            )

            if 'startedAt' in batch_job:
                run_state = run_state._replace(
                    container_time_total=int(time.time()) - (batch_job['startedAt'] / 1000)
                )

            if (
                run_state.resources.time
                and run_state.container_time_total > run_state.resources.time
            ):
                kill_messages.append(
                    'Time limit exceeded. (Container uptime %s > time limit %s)'
                    % (
                        duration_str(run_state.container_time_total),
                        duration_str(run_state.resources.time),
                    )
                )

            if run_state.resources.disk and run_state.disk_utilization > run_state.resources.disk:
                kill_messages.append(
                    'Disk limit %sb exceeded.' % size_str(run_state.resources.disk)
                )

            if kill_messages:
                run_state = run_state._replace(kill_message=' '.join(kill_messages), is_killed=True)

            return run_state

        self.disk_utilization.add_if_new(
            run_state.bundle.uuid, threading.Thread(target=check_disk_utilization, args=[])
        )
        try:
            batch_job = self._fetch_batch_job(run_state.batch_job_id)
            run_state = check_run_status(run_state, batch_job)
            run_state = check_resource_utilization(run_state, batch_job)
            if run_state.is_killed:
                self._batch_client.terminate_job(
                    jobId=run_state.batch_job_id, reason=run_state.kill_message
                )
        except BatchJobNotFound as e:
            logger.error(
                "Batch job %s for run %s not found: %s", e.job_id, run_state.bundle.uuid, e
            )
            run_state = run_state._replace(
                is_finished=True, failure_message="Batch job %s not found" % e.job_id
            )
        if run_state.is_finished:
            logger.debug(
                'Finished run with UUID %s, final run status: %s',
                run_state.bundle.uuid,
                run_state.run_status,
            )
            self.disk_utilization[run_state.bundle.uuid]['running'] = False
            self.disk_utilization.remove(run_state.bundle.uuid)
            return run_state._replace(stage=AWSBatchRunStage.CLEANING_UP)
        else:
            return run_state

    def _fetch_batch_job(self, run_state):
        response = self._batch_client.describe_jobs(jobs=[run_state.batch_job_id])
        if len(response['jobs']) == 0:
            raise BatchJobNotFound(run_state.batch_job_id)
        return response['jobs'][0]

    def _transition_from_CLEANING_UP(self, run_state):
        self._batch_client.deregister_job_definition(jobDefinition=run_state.batch_job_definition)
        if not run_state.failure_message and run_state.is_killed:
            run_state = run_state._replace(failure_message=run_state.kill_message)
        try:
            job = self._fetch_batch_job(run_state.batch_job_id)
            status = job['status']
            # The container contains information about the most recent docker container used to run the job
            container = job['container']

            if 'startedAt' in job and 'stoppedAt' in job:
                run_state = run_state._replace(
                    container_time_total=(job['stoppedAt'] - job['startedAt']) / 1000
                )

            if status == AWSBatchStatus.SUCCEEDED:
                run_state = run_state._replace(
                    run_status='Succeeded',
                    exitcode=container.get('exitCode', 0),
                    failure_message=container.get('reason'),
                )
            elif status == AWSBatchStatus.FAILED:
                run_state = run_state._replace(
                    run_status='Failed',
                    exitcode=container.get('exitCode', 1),
                    failure_message=container.get(
                        'reason', job.get('statusReason', run_state.failure_message)
                    ),
                )
            elif status == AWSBatchStatus.RUNNING:
                # If somehow were got to completed but the job is still running, then go back to running
                # This is not at all expected to happen, but if it does then we should at least log and handle it
                logger.warning('Completed state reached when Batch job was still running.')
                return run_state._replace(stage=AWSBatchRunStage.RUNNING)
            else:
                run_state = run_state._replace(
                    run_status='Failed',
                    exitcode=container.get('exitCode', 2),
                    failure_message=container.get(
                        'reason', 'Unexpected Batch status %s during Complete.' % status
                    ),
                )
        except BatchJobNotFound as ex:
            logger.error(
                "Expected finished batch job %s for run %s not found: %s",
                ex.job_id,
                run_state.bundle.uuid,
                ex,
            )
        return run_state._replace(stage=AWSBatchRunStage.FINALIZING)

    @staticmethod
    def _transition_from_FINALIZING(run_state):
        """
        If a full worker cycle has passed since we got into FINALIZING we already reported to
        server so can move on to FINISHED. Can also remove bundle_path now
        """
        if run_state.is_finalized:
            return run_state._replace(stage=AWSBatchRunStage.FINISHED)
        return run_state


class BatchJobNotFound(Exception):
    """Exception to provide a useful error message when a Batch job cannot be found."""

    def __init__(self, job_id):
        super(BatchJobNotFound, self).__init__()
        self.job_id = job_id

    def __str__(self):
        return "Could not find Batch Job %s." % self.job_id
