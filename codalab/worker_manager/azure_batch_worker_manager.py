try:
    import azure.batch._batch_service_client as batch   # type: ignore
    import azure.batch.batch_auth as batchauth  # type: ignore
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
            help='Path to the Azure Batch configuration file (.cfg)',
        )
        subparser.add_argument(
            '--job-id', type=str, help='ID of the Azure Batch job to add tasks to',
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
        # Count the number active and running tasks only within the Batch job
        task_counts = self.batch_client.job.get_task_counts(self.args.job_id)
        return [WorkerJob(True) for _ in range(task_counts.active + task_counts.running)]

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

        # Create a task within the job
        task_container_run_options = [
            '--cpus %d' % self.args.cpus,
            '--memory %dM' % self.args.memory_mb,
            '--volume /var/run/docker.sock:/var/run/docker.sock',
            '--volume %s:%s' % (work_dir, work_dir),
            '--user %s' % self.args.user,
        ]

        if os.environ.get('CODALAB_USERNAME') and os.environ.get('CODALAB_PASSWORD'):
            task_container_run_options.extend(
                [
                    '--env CODALAB_USERNAME=%s' % os.environ.get('CODALAB_USERNAME'),
                    '--env CODALAB_PASSWORD=%s' % os.environ.get('CODALAB_PASSWORD'),
                ]
            )

        # Allow worker to directly mount a directory
        if os.environ.get('CODALAB_SHARED_FILE_SYSTEM') == 'true':
            command.append('--shared-file-system')
            task_container_run_options.append(
                '--volume shared_dir:%s' % os.environ.get('CODALAB_BUNDLE_MOUNT')
            )

        # Configure Sentry
        if using_sentry():
            task_container_run_options.append(
                '--env CODALAB_SENTRY_INGEST_URL=%s' % CODALAB_SENTRY_INGEST
            )

        command_line = "/bin/sh -c '{}'".format(' '.join(command))
        logger.debug("Running the following as an Azure Batch task: {}".format(command_line))

        task_container_settings = batch.models.TaskContainerSettings(
            image_name=worker_image, container_run_options=' '.join(task_container_run_options)
        )
        task = batch.models.TaskAddParameter(
            id='cl_worker_{}'.format(worker_id),
            command_line=command_line,
            container_settings=task_container_settings,
        )
        self.batch_client.task.add(self.args.job_id, task)
