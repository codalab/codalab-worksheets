try:
    # TODO: change this -Tony
    import dsub  # type: ignore
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "Running the GCP worker manager requires the dsub module.\n"
        "Please run: pip install dsub. See https://github.com/databiosphere/dsub for more information."
    )

import json
import logging
import os
import uuid
import yaml
from argparse import ArgumentParser
from typing import Any, Dict, List

from .worker_manager import WorkerManager, WorkerJob


logger: logging.Logger = logging.getLogger(__name__)


class GCPBatchWorkerManager(WorkerManager):
    NAME: str = 'gcp-batch'
    DESCRIPTION: str = 'Worker manager for submitting jobs to Google Cloud Platform via dsub'

    @staticmethod
    def add_arguments_to_subparser(subparser: ArgumentParser) -> None:
        # GCP arguments
        subparser.add_argument('--project', type=str, help='GCP project', required=True)
        subparser.add_argument('--bucket', type=str, help='GCP bucket for logs', required=True)
        subparser.add_argument('--region', type=str, help='GCP region', default='us-west1')

        # Job-related arguments
        subparser.add_argument(
            '--cpus', type=int, default=1, help='Default number of CPUs for each worker'
        )
        subparser.add_argument(
            '--gpus', type=int, default=0, help='Default number of GPUs to request for each worker'
        )
        subparser.add_argument(
            '--memory-mb', type=int, default=2048, help='Default memory (in MB) for each worker'
        )

    def __init__(self, args):
        super().__init__(args)

        self.codalab_username = os.environ.get('CODALAB_USERNAME')
        self.codalab_password = os.environ.get('CODALAB_PASSWORD')
        if not self.codalab_username or not self.codalab_password:
            raise EnvironmentError(
                'Valid credentials need to be set as environment variables: CODALAB_USERNAME and CODALAB_PASSWORD'
            )

    def get_worker_jobs(self) -> List[WorkerJob]:
        # Use dstat to retrieve running jobs from GCP
        output_json: str = self.run_command(
            ['kubectl', 'get', 'pod', '--field-selector=status.phase==Running', '--output=json',]
        )
        return (
            [WorkerJob(True) for _ in range(len(json.loads(output_json)['items']))]
            if output_json
            else []
        )

    def start_worker_job(self) -> None:
        # This needs to be a unique directory since jobs may share a host
        work_dir_prefix: str = (
            self.args.worker_work_dir_prefix if self.args.worker_work_dir_prefix else "/tmp/"
        )
        worker_id: str = uuid.uuid4().hex
        worker_name: str = f'cl-worker-{worker_id}'
        work_dir: str = os.path.join(work_dir_prefix, f'{worker_name}_work_dir')
        command_args: List[str] = self.build_command(worker_id, work_dir)
        worker_image: str = 'codalab/worker:' + os.environ.get('CODALAB_VERSION', 'latest')

        config: Dict[str, Any] = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {'name': worker_name},
            'spec': {
                # 'ttlSecondsAfterFinished': 0,  # Clean up job after it finishes
                'containers': [
                    {
                        'command': command_args,
                        'image': worker_image,
                        'name': f'{worker_name}-container',
                        'securityContext': {'runAsUser': 0,},
                        'env': [
                            {'name': 'CODALAB_USERNAME', 'value': self.codalab_username},
                            {'name': 'CODALAB_PASSWORD', 'value': self.codalab_password},
                        ],
                        'volumeMounts': [
                            {'name': 'dockersock', 'mountPath': '/var/run/docker.sock'},
                            # {'name': 'workdir', 'mountPath': work_dir},
                        ],
                    }
                ],
                'volumes': [{'name': 'dockersock', 'hostPath': {'path': '/var/run/docker.sock'}}],
            },
        }
        with open('job.yaml', 'w') as yaml_file:
            yaml.dump(config, yaml_file, default_flow_style=False)

        # Use kubectl to start a worker on GCP
        logger.debug('Starting worker {} with image {}'.format(worker_id, worker_image))
        self.run_command(['kubectl', 'apply', '--filename=job.yaml'])

        # TODO: use f string or just get rid of this -Tony
        task_container_run_options: List[str] = [
            '--cpus %d' % self.args.cpus,
            '--memory %dM' % self.args.memory_mb,
            '--volume /var/run/docker.sock:/var/run/docker.sock',
            '--volume %s:%s' % (work_dir, work_dir),
            '--user root',
        ]
