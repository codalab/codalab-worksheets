try:
    import azure.batch._batch_service_client as batch
    import azure.batch.batch_auth as batchauth
    import azure.batch.models as batchmodels
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "Running the worker manager requires the azure-batch module.\n"
        "Please run: pip install azure-batch==9.0.0"
    )

import configparser
import logging
import os
import uuid

from codalab.lib.telemetry_util import CODALAB_SENTRY_INGEST, using_sentry
from .worker_manager import WorkerManager, WorkerJob


logger = logging.getLogger(__name__)


class AzureBatchWorkerManager(WorkerManager):
    NAME = 'azure-batch'
    DESCRIPTION = 'Worker manager for submitting jobs to Azure Batch'

    @staticmethod
    def add_arguments_to_subparser(subparser):
        subparser.add_argument(
            '--azure-config-path',
            type=str,
            help='Path to the Azure batch configuration file (.cfg)',
        )  # required
        subparser.add_argument(
            '--pool-id', type=str, help='ID of the Azure Batch pool to use',
        )  # required
        subparser.add_argument(
            '--job-name', type=str, default='codalab-worker', help='Name of the job',
        )
        subparser.add_argument(
            '--cpus', type=int, default=1, help='Default number of CPUs for each worker'
        )
        subparser.add_argument(
            '--gpus', type=int, default=0, help='Default number of GPUs to request for each worker'
        )
        subparser.add_argument(
            '--memory-mb', type=int, default=1024, help='Default memory (in MB) for each worker'
        )
        subparser.add_argument(
            '--user', type=str, default='root', help='User to run the Batch jobs as'
        )

    def __init__(self, args):
        super().__init__(args)

        azure_config = configparser.ConfigParser()
        azure_config.read(self.args.azure_config_path)
        batch_account_key = azure_config.get('Batch', 'batchaccountkey')
        batch_account_name = azure_config.get('Batch', 'batchaccountname')
        batch_service_url = azure_config.get('Batch', 'batchserviceurl')

        credentials = batchauth.SharedKeyCredentials(batch_account_name, batch_account_key)
        self.batch_client = batch.BatchServiceClient(credentials, batch_url=batch_service_url)
        self.batch_client.config.retry_policy.retries = 1

    def get_worker_jobs(self):
        """Return list of worker jobs."""
        worker_jobs = []
        azure_batch_jobs = self.batch_client.job.list(
            options=batchmodels.JobListOptions(filter="state eq 'active'")
        )

        for job in azure_batch_jobs:
            task_counts = self.batch_client.job.get_task_counts(job.id)

            if task_counts.active == 1 or task_counts.running == 1:
                worker_jobs.append(WorkerJob(True))
        return worker_jobs

    def start_worker_job(self):
        worker_image = 'codalab/worker:' + os.environ.get('CODALAB_VERSION', 'latest')
        worker_id = uuid.uuid4().hex
        logger.debug('Starting worker {} with image {}'.format(worker_id, worker_image))
        work_dir_prefix = (
            self.args.worker_work_dir_prefix if self.args.worker_work_dir_prefix else "/tmp/"
        )
        # This needs to be a unique directory since Batch jobs may share a host
        work_dir = os.path.join(work_dir_prefix, 'cl_worker_{}_work_dir'.format(worker_id))
        command = self.build_command(worker_id, work_dir)

        # Create a job using the pool
        job_id = 'azure-{}-{}'.format(self.args.job_name, worker_id)
        job = batch.models.JobAddParameter(
            id=job_id,
            display_name=self.args.job_name,
            pool_info=batch.models.PoolInformation(pool_id=self.args.pool_id),
            common_environment_settings=[
                batch.models.EnvironmentSetting(
                    name='CODALAB_USERNAME', value=os.environ.get('CODALAB_USERNAME')
                ),
                batch.models.EnvironmentSetting(
                    name='CODALAB_PASSWORD', value=os.environ.get('CODALAB_PASSWORD')
                ),
            ],
        )
        self.batch_client.job.add(job, raw=True)

        # Create a task in job
        task_container_run_options = [
            '--cpus %d' % self.args.cpus,
            '--memory %dM' % self.args.memory_mb,
            '--volume /var/run/docker.sock:/var/run/docker.sock',
            '--volume %s:%s' % (work_dir, work_dir),
            '--user %s' % self.args.user,
        ]

        if self.args.gpus > 0:
            task_container_run_options.append('--gpus all')

        # Allow worker to directly mount a directory.
        if os.environ.get('CODALAB_SHARED_FILE_SYSTEM') == 'true':
            command.append('--shared-file-system')
            bundle_mount = os.environ.get('CODALAB_BUNDLE_MOUNT')
            task_container_run_options.append('--volume shared_dir:%s' % bundle_mount)

        # Configure Sentry
        if using_sentry():
            task_container_run_options.append(
                '--env CODALAB_SENTRY_INGEST_URL=%s' % CODALAB_SENTRY_INGEST
            )

        command_line = "/bin/sh -c '{}'".format(' '.join(command))
        logger.debug("Running as a task: {}".format(command_line))

        task_container_settings = batch.models.TaskContainerSettings(
            image_name=worker_image, container_run_options=' '.join(task_container_run_options)
        )
        task = batch.models.TaskAddParameter(
            id=job_id, command_line=command_line, container_settings=task_container_settings,
        )
        self.batch_client.task.add(job_id, task, raw=True)
