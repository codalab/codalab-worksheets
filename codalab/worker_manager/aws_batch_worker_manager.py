import boto3
import json
import logging
import os
import uuid
from .worker_manager import WorkerManager, WorkerJob

logger = logging.getLogger(__name__)


class AWSBatchWorkerManagerConfig:
    DEFAULT_CONFIG = {
        'region': 'us-east-1',
        'job_definition_name': 'codalab-worker-4',
        'cpus': 4,
        'memory_mb': 1024 * 10,
        'user': 'root',
        'queue': 'codalab-batch-cpu',
    }

    def __init__(self, config_filename):
        try:
            with open(config_filename, 'r') as config_file:
                config = json.load(config_file)
        except (IOError, ValueError, json.decoder.JSONDecodeError) as ex:
            logger.error(
                "Problem loading config file [%s]: %s. Using the default config.",
                config_filename,
                str(ex),
            )
            config = AWSBatchWorkerManagerConfig.DEFAULT_CONFIG
        self.region = config.get('region', self.DEFAULT_CONFIG['region'])
        self.job_definition_name = config.get(
            'job_definition_name', self.DEFAULT_CONFIG['job_definition_name']
        )
        self.cpus = config.get('cpus', self.DEFAULT_CONFIG['cpus'])
        self.memory_mb = config.get('memory_mb', self.DEFAULT_CONFIG['memory_mb'])
        self.user = config.get('user', self.DEFAULT_CONFIG['user'])
        self.queue = config.get('queue', self.DEFAULT_CONFIG['queue'])


class AWSBatchWorkerManager(WorkerManager):
    def __init__(self, args):
        super().__init__(args)
        self.config = AWSBatchWorkerManagerConfig(args.queue_config_file)
        self.batch_client = boto3.client('batch', region_name=self.config.region)

    def get_worker_jobs(self):
        """Return list of workers."""
        # Get all jobs that are not SUCCEEDED or FAILED.  Assume these
        # represent workers we launched (no one is sharing this queue).
        jobs = []
        for status in ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING']:
            response = self.batch_client.list_jobs(jobQueue=self.config.queue, jobStatus=status)
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
        work_dir = '/tmp/cl_worker_{}_work_dir'.format(
            worker_id
        )  # This needs to be a unique directory since Batch jobs may share a host
        worker_network_prefix = 'cl_worker_{}_network'.format(worker_id)
        command = [
            'cl-worker',
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

        # https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-batch-jobdefinition.html
        # Need to mount:
        # - docker.sock to enable us to start docker in docker
        # - work_dir so that the run bundle's output is visible to the worker
        job_definition = {
            'jobDefinitionName': self.config.job_definition_name,
            'type': 'container',
            'parameters': {},
            'containerProperties': {
                'image': image,
                'vcpus': self.config.cpus,
                'memory': self.config.memory_mb,
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
                'user': self.config.user,
            },
            'retryStrategy': {'attempts': 1},
        }

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
            jobName=self.config.job_definition_name,
            jobQueue=self.config.queue,
            jobDefinition=self.config.job_definition_name,
        )
        logger.info('submit_job: %s', response)

        # TODO: Do we need to delete the jobs and job definitions?
