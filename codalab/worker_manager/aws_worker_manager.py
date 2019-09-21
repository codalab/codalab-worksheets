import boto3
import logging
import os
from .worker_manager import WorkerManager, WorkerJob

logger = logging.getLogger(__name__)


class AWSWorkerManager(WorkerManager):
    def __init__(self, args):
        super().__init__(args)
        if not args.queue:
            raise Exception('Missing queue for AWS Batch')
        self.batch_client = boto3.client('batch', region_name='us-east-1')

    def get_worker_jobs(self):
        """Return list of workers."""
        # Get all jobs that are not SUCCEEDED or FAILED.  Assume these
        # represent the active workers (no one is sharing this queue).
        jobs = []
        for status in ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING']:
            response = self.batch_client.list_jobs(jobQueue=self.args.queue, jobStatus=status)
            jobs.extend(response['jobSummaryList'])
        logger.info(
            'Workers: {}'.format(
                ' '.join(job['jobId'] + ':' + job['status'] for job in jobs) or '(none)'
            )
        )
        return [WorkerJob() for job in jobs]

    def start_worker_job(self):
        image = 'codalab/worker:' + os.environ.get('CODALAB_VERSION', 'latest')
        logger.debug('Starting worker with image {}'.format(image))
        job_definition_name = 'codalab-worker-4'  # This is just an arbitrary identifier.
        # TODO: don't hard code these, get these from some config file.
        cpus = 4
        memory_mb = 1024 * 10
        work_dir = '/tmp'  # Need a directory outside the dockerized worker that already exists
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
                        'sourceVolume': 'var_run_docker_sock',
                        'containerPath': '/var/run/docker.sock',
                        'readOnly': False,
                    },
                    {'sourceVolume': 'work_dir', 'containerPath': work_dir, 'readOnly': False},
                ],
                'readonlyRootFilesystem': False,
                # TODO: if shared file system, use user
                # Ideally, we should use user everywhere, but that's a
                # different issue.
                'user': 'root',
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
        logger.info('register_job_definition', response)

        # Submit the job
        response = self.batch_client.submit_job(
            jobName=job_definition_name, jobQueue=self.args.queue, jobDefinition=job_definition_name
        )
        logger.info('submit_job', response)

        # TODO: Do we need to delete the jobs and job definitions?
