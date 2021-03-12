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
    DESCRIPTION: str = 'Worker manager for submitting jobs to Google Cloud Platform via Kubernetes'

    @staticmethod
    def add_arguments_to_subparser(subparser: ArgumentParser) -> None:
        # GCP arguments
        subparser.add_argument('--project', type=str, help='GCP project', required=True)
        subparser.add_argument('--bucket', type=str, help='GCP bucket for logs', required=True)

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

        # Output path
        subparser.add_argument(
            '--output-path', type=str, default='.', help='Path to the output directory'
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
        # Use kubectl to get the current running pods in JSON format
        output_json: str = self.run_command(
            ['kubectl', 'get', 'pod', '--field-selector=status.phase==Running', '--output=json']
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
        command: List[str] = self.build_command(worker_id, work_dir)
        worker_image: str = 'codalab/worker:' + os.environ.get('CODALAB_VERSION', 'latest')

        config: Dict[str, Any] = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {'name': worker_name},
            'spec': {
                'containers': [
                    {
                        'name': f'{worker_name}-container',
                        'image': worker_image,
                        'command': command,
                        'securityContext': {'runAsUser': 0},  # Run as root
                        'env': [
                            {'name': 'CODALAB_USERNAME', 'value': self.codalab_username},
                            {'name': 'CODALAB_PASSWORD', 'value': self.codalab_password},
                        ],
                        'resources': {
                            'limits': {
                                'cpu': self.args.cpus,
                                'memory': f'{self.args.memory_mb}Mi',
                                'nvidia.com/gpu': self.args.gpus,  # Configure NVIDIA GPUs
                            }
                        },
                        'volumeMounts': [
                            {'name': 'dockersock', 'mountPath': '/var/run/docker.sock'},
                            {'name': 'workdir', 'mountPath': work_dir},
                        ],
                    }
                ],
                'nodeSelector': {'cloud.google.com/gke-accelerator': 'nvidia-tesla-k80'},
                'volumes': [
                    {'name': 'dockersock', 'hostPath': {'path': '/var/run/docker.sock'}},
                    {'name': 'workdir', 'hostPath': {'path': work_dir}},
                ],
                'restartPolicy': 'Never',  # Only run a job once
            },
        }
        with open(os.path.join(self.args.output_path, 'job.yaml'), 'w') as yaml_file:
            yaml.dump(config, yaml_file, default_flow_style=False)

        # Use kubectl to start a worker on GCP
        logger.debug('Starting worker {} with image {}'.format(worker_id, worker_image))
        self.run_command(['kubectl', 'apply', '--filename=job.yaml'])
