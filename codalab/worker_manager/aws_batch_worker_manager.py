import logging
import os
import re
import uuid
from argparse import ArgumentParser
from shlex import quote

from .worker_manager import WorkerManager, WorkerJob
from codalab.lib.telemetry_util import CODALAB_SENTRY_INGEST, CODALAB_SENTRY_ENVIRONMENT, using_sentry

logger = logging.getLogger(__name__)


class AWSBatchWorkerManager(WorkerManager):
    NAME: str = 'aws-batch'
    DESCRIPTION: str = 'Worker manager for submitting jobs to AWS Batch'

    @staticmethod
    def add_arguments_to_subparser(subparser: ArgumentParser) -> None:
        subparser.add_argument(
            '--region', type=str, default='us-east-1', help='AWS region to run jobs in'
        )
        subparser.add_argument(
            '--job-definition-name',
            type=str,
            default='codalab-worker',
            help='Name for the job definitions that will be generated by this worker manager',
        )
        subparser.add_argument(
            '--cpus', type=int, default=1, help='Default number of CPUs for each worker'
        )
        subparser.add_argument(
            '--gpus', type=int, default=0, help='Default number of GPUs to request for each worker'
        )
        subparser.add_argument(
            '--memory-mb', type=int, default=2048, help='Default memory (in MB) for each worker'
        )
        subparser.add_argument(
            '--user', type=str, default='root', help='User to run the Batch jobs as'
        )
        subparser.add_argument(
            '--job-queue',
            type=str,
            default='codalab-batch-cpu',
            help='Name of the AWS Batch job queue to use',
        )
        subparser.add_argument(
            '--job-filter',
            type=str,
            help=(
                'Only consider jobs on the job queue with job names that '
                'completely match this regex filter.'
            ),
        )

    def __init__(self, args):
        super().__init__(args)
        # We import this lazily, so a user doesn't have to install boto3 unless
        # they absolutely want to run the AWS worker manager, versus if it's incidentally
        # imported by other code (e.g., to access AWSBatchWorkerManager.DESCRIPTION , as done
        # in codalab/worker_manager/main.py ).
        try:
            import boto3
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "Running the AWS worker manager requires the boto3 module.\n"
                "Please run: pip install boto3"
            )
        self.batch_client = boto3.client('batch', region_name=self.args.region)

    def get_worker_jobs(self):
        """Return list of workers."""
        # Get all jobs that are not SUCCEEDED or FAILED.
        jobs = []
        for status in ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING']:
            response = self.batch_client.list_jobs(jobQueue=self.args.job_queue, jobStatus=status)
            for jobSummary in response['jobSummaryList']:
                # Only record jobs if a job regex filter isn't provided or if the job's name completely matches
                # a provided job regex filter.
                if not self.args.job_filter or re.fullmatch(
                    self.args.job_filter, jobSummary.get("jobName", "")
                ):
                    jobs.append(jobSummary)
        logger.info(
            'Workers: {}'.format(
                ' '.join(job['jobId'] + ':' + job['status'] for job in jobs) or '(none)'
            )
        )
        # Only RUNNING jobs are `active` (see WorkerJob definition for meaning of active)
        return [WorkerJob(job['status'] == 'RUNNING') for job in jobs]

    def start_worker_job(self):
        image = 'codalab/worker:' + os.environ.get('CODALAB_VERSION', 'latest')
        worker_id = uuid.uuid4().hex
        logger.debug('Starting worker %s with image %s', worker_id, image)
        work_dir_prefix = (
            self.args.worker_work_dir_prefix if self.args.worker_work_dir_prefix else "/tmp/"
        )
        # This needs to be a unique directory since Batch jobs may share a host
        work_dir = os.path.join(work_dir_prefix, 'cl_worker_{}_work_dir'.format(worker_id))
        command = self.build_command(worker_id, work_dir)

        # https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-batch-jobdefinition.html
        # Need to mount:
        # - docker.sock to enable us to start docker in docker
        # - work_dir so that the run bundle's output is visible to the worker
        job_definition = {
            'jobDefinitionName': self.args.job_definition_name,
            'type': 'container',
            'parameters': {},
            'containerProperties': {
                'image': image,
                'vcpus': self.args.cpus,
                'memory': self.args.memory_mb,
                'command': [
                    "/bin/bash",
                    "-c",
                    "/opt/scripts/detect-ec2-spot-preemption.sh & "
                    + " ".join(quote(arg) for arg in command),
                ],
                'environment': [
                    {'name': 'CODALAB_USERNAME', 'value': os.environ.get('CODALAB_USERNAME')},
                    {'name': 'CODALAB_PASSWORD', 'value': os.environ.get('CODALAB_PASSWORD')},
                ],
                'volumes': [
                    {'host': {'sourcePath': '/var/run/docker.sock'}, 'name': 'var_run_docker_sock'},
                    {'host': {'sourcePath': work_dir}, 'name': 'work_dir'},
                ],
                'mountPoints': [
                    {
                        'sourceVolume': 'var_run_docker_sock',
                        'containerPath': '/var/run/docker.sock',
                        'readOnly': False,
                    },
                    {'sourceVolume': 'work_dir', 'containerPath': work_dir, 'readOnly': False},
                ],
                'readonlyRootFilesystem': False,
                'user': self.args.user,
            },
            'retryStrategy': {'attempts': 1},
        }
        if self.args.gpus:
            job_definition["containerProperties"]["resourceRequirements"] = [
                {"value": str(self.args.gpus), "type": "GPU"}
            ]

        # Allow worker to directly mount a directory.  Note that the worker
        # needs to be set up a priori with this shared filesystem.
        if os.environ.get('CODALAB_SHARED_FILE_SYSTEM') == 'true':
            command.append('--shared-file-system')
            bundle_mount = os.environ.get('CODALAB_BUNDLE_MOUNT')
            job_definition['containerProperties']['volumes'].append(
                {'host': {'sourcePath': bundle_mount}, 'name': 'shared_dir'}
            )
            job_definition['containerProperties']['mountPoints'].append(
                {'sourceVolume': 'shared_dir', 'containerPath': bundle_mount, 'readOnly': False}
            )

        if using_sentry():
            job_definition["containerProperties"]["environment"].append(
                {'name': 'CODALAB_SENTRY_INGEST_URL', 'value': CODALAB_SENTRY_INGEST}
            )
            job_definition["containerProperties"]["environment"].append(
                {'name': 'CODALAB_SENTRY_ENVIRONMENT', 'value': CODALAB_SENTRY_ENVIRONMENT}
            )
        # Create a job definition
        response = self.batch_client.register_job_definition(**job_definition)
        logger.info('register_job_definition: %s', response)

        # Submit the job
        response = self.batch_client.submit_job(
            jobName=self.args.job_definition_name,
            jobQueue=self.args.job_queue,
            jobDefinition=self.args.job_definition_name,
        )
        logger.info('submit_job: %s', response)

        # TODO: Do we need to delete the jobs and job definitions?
