import boto3
import logging
from .worker_manager import WorkerManager
from codalab.common import CODALAB_VERSION

logger = logging.getLogger(__name__)


class AWSWorkerManager(WorkerManager):
    def __init__(self, args):
        super().__init__(args)
        self.batch_client = boto3.client('batch', region_name='us-east-1')

    def get_workers(self):
        """Return list of workers."""
        # Get all jobs that are not SUCCEEDED.  These represent the workers.
        # Note we include FAILED so that if something goes wrong, we don't keep
        # on trying to start workers to no avail.
        jobs = []
        for status in ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING', 'FAILED']:
            response = self.batch_client.list_jobs(jobQueue=self.args.queue, jobStatus=status)
            jobs.extend(response['jobSummaryList'])
        logger.info(
            'Workers: {}'.format(' '.join(job['jobId'] + ':' + job['status'] for job in jobs))
        )
        return jobs

    def start_worker(self):
        job_definition_name = 'codalab-worker-' + CODALAB_VERSION
        image = 'codalab/worker:' + CODALAB_VERSION
        # TODO: don't hard code these, get these from some config file.
        cpus = 4
        memory_mb = 1024 * 10
        command = [
            'cl-worker',
            '--server',
            self.args.server,
            '--verbose',
            '--exit-when-idle',
        ]
        if self.args.worker_tag:
            command.extend(['--tag', self.args.worker_tag])

        # https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-batch-jobdefinition.html
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
                    {
                        'name': 'CODALAB_USERNAME',
                        'value': os.environ.get('CODALAB_USERNAME'),
                    },
                    {
                        'name': 'CODALAB_PASSWORD',
                        'value': os.environ.get('CODALAB_PASSWORD'),
                    },
                ],
                'volumes': [
                    {
                        'host': {
                            'sourcePath': '/var/run/docker.sock',
                        },
                        'name': 'var_run_docker_sock',
                    }
                ],
                'mountPoints': [
                    {
                        "sourceVolume" : 'var_run_docker_sock',
                        'containerPath': '/var/run/docker.sock',
                        "readOnly": False,
                    },
                ],
                'readonlyRootFilesystem': False,
                'user': 'root',  # TODO: if shared file system, use user
            },
            'retryStrategy': {
                'attempts': 1
            }
        }

        # Create a job definition
        response = batch_client.register_job_definition(**job_definition)
        logger.info('register_job_definition', response)

        # Submit the job
        response = batch_client.submit_job(
            jobName=job_definition_name,
            jobQueue=self.args.queue,
            jobDefinition=job_definition_name,
        )
        logger.info('submit_job', response)

        # TODO: Clean up after ourselves too; delete workers
