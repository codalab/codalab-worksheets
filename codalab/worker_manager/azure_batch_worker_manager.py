try:
    from azure.batch.batch_auth import SharedKeyCredentials  # type: ignore
    from azure.batch._batch_service_client import BatchServiceClient  # type: ignore
    from azure.batch.models import (  # type: ignore
        OutputFile,
        OutputFileBlobContainerDestination,
        OutputFileDestination,
        OutputFileUploadCondition,
        OutputFileUploadOptions,
        TaskAddParameter,
        TaskCounts,
        TaskContainerSettings,
    )
    from msrest.exceptions import ClientRequestError  # type: ignore
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "Running the worker manager requires the azure-batch module.\n"
        "Please run: pip install azure-batch"
    )

import logging
import os
import uuid
from argparse import ArgumentParser
from typing import List

from codalab.lib.telemetry_util import CODALAB_SENTRY_INGEST, using_sentry
from .worker_manager import WorkerManager, WorkerJob


logger: logging.Logger = logging.getLogger(__name__)


class AzureBatchWorkerManager(WorkerManager):
    NAME: str = 'azure-batch'
    DESCRIPTION: str = 'Worker manager for submitting jobs to Azure Batch'

    @staticmethod
    def add_arguments_to_subparser(subparser: ArgumentParser) -> None:
        subparser.add_argument(
            '--account-name', type=str, help='Azure Batch account name', required=True
        )
        subparser.add_argument(
            '--account-key', type=str, help='Azure Batch account key', required=True
        )
        subparser.add_argument(
            '--service-url', type=str, help='Azure Batch service URL', required=True
        )
        subparser.add_argument(
            '--log-container-url',
            type=str,
            help='URL of the Azure Storage container to store the worker logs',
            required=True,
        )
        subparser.add_argument(
            '--job-id', type=str, help='ID of the Azure Batch job to add tasks to', required=True
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

    def __init__(self, args):
        super().__init__(args)

        credentials: SharedKeyCredentials = SharedKeyCredentials(
            self.args.account_name, self.args.account_key
        )
        self._batch_client: BatchServiceClient = BatchServiceClient(
            credentials, batch_url=self.args.service_url
        )
        self._batch_client.config.retry_policy.retries = 1

    def get_worker_jobs(self) -> List[WorkerJob]:
        try:
            # Count the number active and running tasks for the Azure Batch job.
            # Catch request errors to keep the worker manager running.
            task_counts: TaskCounts = self._batch_client.job.get_task_counts(self.args.job_id)
            return [WorkerJob(True) for _ in range(task_counts.active + task_counts.running)]
        except ClientRequestError as e:
            logger.error('Batch request to retrieve the number of tasks failed: {}'.format(str(e)))
            return []

    def start_worker_job(self) -> None:
        worker_image: str = 'codalab/worker:' + os.environ.get('CODALAB_VERSION', 'latest')
        worker_id: str = uuid.uuid4().hex
        logger.debug('Starting worker {} with image {}'.format(worker_id, worker_image))
        work_dir_prefix: str = (
            self.args.worker_work_dir_prefix if self.args.worker_work_dir_prefix else "/tmp/"
        )

        # This needs to be a unique directory since Batch jobs may share a host
        work_dir: str = os.path.join(work_dir_prefix, 'cl_worker_{}_work_dir'.format(worker_id))
        command: List[str] = self.build_command(worker_id, work_dir)

        task_container_run_options: List[str] = [
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
        else:
            raise EnvironmentError(
                'Valid credentials need to be set as environment variables: CODALAB_USERNAME and CODALAB_PASSWORD'
            )

        if os.environ.get('CODALAB_SHARED_FILE_SYSTEM') == 'true':
            # Allow workers to directly mount a directory
            command.append('--shared-file-system')
            task_container_run_options.append(
                '--volume shared_dir:%s' % os.environ.get('CODALAB_BUNDLE_MOUNT')
            )

        # Configure Sentry
        if using_sentry():
            task_container_run_options.append(
                '--env CODALAB_SENTRY_INGEST_URL=%s' % CODALAB_SENTRY_INGEST
            )

        command_line: str = "/bin/bash -c '{}'".format(' '.join(command))
        logger.debug("Running the following as an Azure Batch task: {}".format(command_line))

        task_id: str = 'cl_worker_{}'.format(worker_id)
        task: TaskAddParameter = TaskAddParameter(
            id=task_id,
            command_line=command_line,
            container_settings=TaskContainerSettings(
                image_name=worker_image, container_run_options=' '.join(task_container_run_options)
            ),
            output_files=[
                OutputFile(
                    file_pattern='../stderr.txt',
                    destination=OutputFileDestination(
                        container=OutputFileBlobContainerDestination(
                            path=task_id, container_url=self.args.log_container_url
                        )
                    ),
                    upload_options=OutputFileUploadOptions(
                        # Upload worker logs once the task completes
                        upload_condition=OutputFileUploadCondition.task_completion
                    ),
                )
            ],
        )

        try:
            # Create a task under the Azure Batch job.
            # Catch request errors to keep the worker manager running.
            self._batch_client.task.add(self.args.job_id, task)
        except ClientRequestError as e:
            logger.error(
                'Batch request to add task {} to job {} failed: {}'.format(
                    task_id, self.args.job_id, str(e)
                )
            )
