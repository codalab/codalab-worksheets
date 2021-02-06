try:
    import dsub
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "Running the GCP worker manager requires the dsub module.\n"
        "Please run: pip install dsub. See https://github.com/databiosphere/dsub for more information."
    )

import logging
import os
import uuid
from argparse import ArgumentParser
from typing import List

from codalab.lib.telemetry_util import CODALAB_SENTRY_INGEST, using_sentry
from .worker_manager import WorkerManager, WorkerJob


logger: logging.Logger = logging.getLogger(__name__)


class GCPWorkerManager(WorkerManager):
    NAME: str = 'gcp-batch'
    DESCRIPTION: str = 'Worker manager for submitting jobs to Google Cloud Platform via dsub'

    @staticmethod
    def add_arguments_to_subparser(subparser: ArgumentParser) -> None:
        subparser.add_argument('--account-key', type=str, help='GCP account key', required=True)
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

        # TODO: authorize and instantiate client
        pass

    def get_worker_jobs(self) -> List[WorkerJob]:
        try:
            # Use the client to retrieve the number active and running tasks in GCP
            # Catch request errors to keep the worker manager running.
            return [WorkerJob(True) for _ in range(1)]
        # TODO: replace with GCP error
        except Exception as e:
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

        # TODO: start job in GCP -Tony
