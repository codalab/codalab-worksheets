try:
    import boto3
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "Running the worker manager requires the boto3 module.\n"
        "Please run: pip install boto3==1.9.228"
    )
import logging
import os
import uuid
from .worker_manager import WorkerManager, WorkerJob

logger = logging.getLogger(__name__)


class AWSBatchWorkerManager(WorkerManager):
    NAME = 'aws-batch'
    DESCRIPTION = 'Worker manager for submitting jobs to AWS Batch'

    @staticmethod
    def add_arguments_to_subparser(subparser):
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
                'Ignore jobs on the job queue if their job name does not '
                'completely match provided regex filter.'
            ),
        )

    def __init__(self, args):
        super().__init__(args)
        self.batch_client = boto3.client('batch', region_name=self.args.region)

    def get_worker_jobs(self):
        """Return list of workers."""
        # Get all jobs that are not SUCCEEDED or FAILED.
        jobs = []
        for status in ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING']:
            response = self.batch_client.list_jobs(jobQueue=self.args.job_queue, jobStatus=status)
            # Only record jobs if a job regex filter isn't provided or if the job's name completely matches
            # a provided job regex filter.
            if not args.job_filter or re.fullmatch(args.job_filter, response.get("jobName", "")):
                jobs.extend(response['jobSummaryList'])
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
        worker_network_prefix = 'cl_worker_{}_network'.format(worker_id)
        command = [
            self.args.worker_executable,
            '--server',
            self.args.server,
            '--verbose',
            '--exit-when-idle',
            '--idle-seconds',
            str(self.args.worker_idle_seconds),
            '--work-dir',
            work_dir,
            '--id',
            worker_id,
            '--network-prefix',
            worker_network_prefix,
        ]
        if self.args.worker_tag:
            command.extend(['--tag', self.args.worker_tag])
        if self.args.worker_max_work_dir_size:
            command.extend(['--max-work-dir-size', self.args.worker_max_work_dir_size])
        if self.args.worker_delete_work_dir_on_exit:
            command.extend(['--worker-delete-work-dir-on-exit'])
        if self.args.worker_exit_after_num_runs and self.args.worker_exit_after_num_runs > 0:
            command.extend(['--exit-after-num-runs', str(self.args.worker_exit_after_num_runs)])
        if self.args.worker_exit_on_exception:
            command.extend(['--exit-on-exception'])
        if self.args.worker_pass_down_termination:
            command.extend(['--pass-down-termination'])

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
                'command': command,
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
