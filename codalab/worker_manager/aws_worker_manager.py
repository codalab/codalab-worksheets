import boto3
import logging
import os
from .worker_manager import WorkerManager

logger = logging.getLogger(__name__)


class AWSWorkerManager(WorkerManager):
    def __init__(self, args):
        super().__init__(args)
        if not args.queue:
            raise Exception('Missing queue for AWS Batch')
        self.batch_client = boto3.client('batch', region_name='us-east-1')

    def get_workers(self):
        """Return list of workers."""
        # Get all jobs that are not SUCCEEDED or FAILED.  These represent the active workers.
        # This isn't really used, but just used to help us monitor things.
        jobs = []
        for status in ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING']:
            response = self.batch_client.list_jobs(jobQueue=self.args.queue, jobStatus=status)
            jobs.extend(response['jobSummaryList'])
        logger.info(
            'Workers: {}'.format(' '.join(job['jobId'] + ':' + job['status'] for job in jobs))
        )
        return jobs

    def start_worker(self):
        job_definition_name = 'codalab-worker-2'
        image = 'codalab/worker:' + os.environ.get('CODALAB_VERSION', 'latest')
        logger.debug('Starting worker with image {}'.format(image))
        # TODO: don't hard code these, get these from some config file.
        cpus = 4
        memory_mb = 1024 * 10
        work_dir = '/tmp/codalab-worker-scratch'
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
        ]
        if self.args.worker_tag:
            command.extend(['--tag', self.args.worker_tag])

        # https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-batch-jobdefinition.html
        # Need to mount:
        # - docker.sock to enable us to start docker in docker
        # - work_dir so that the run bundle's output is visible to the worker
        job_definition = {
            'jobDefinitionName': job_definition_name,
            'type': 'container',
            'parameters': {},
            'containerProperties': {
                'image': image,
                'vcpus': cpus,
                'memory': memory_mb,
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
                        "sourceVolume": 'var_run_docker_sock',
                        'containerPath': '/var/run/docker.sock',
                        "readOnly": False,
                    },
                    {"sourceVolume": 'work_dir', 'containerPath': work_dir, "readOnly": False},
                ],
                'readonlyRootFilesystem': False,
                'user': 'root',  # TODO: if shared file system, use user
            },
            'retryStrategy': {'attempts': 1},
        }

        # Create a job definition
        response = self.batch_client.register_job_definition(**job_definition)
        logger.info('register_job_definition', response)

        # Submit the job
        response = self.batch_client.submit_job(
            jobName=job_definition_name, jobQueue=self.args.queue, jobDefinition=job_definition_name
        )
        logger.info('submit_job', response)

        # TODO: Clean up after ourselves too; delete workers?
