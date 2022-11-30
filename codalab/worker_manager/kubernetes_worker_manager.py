try:
    from kubernetes import client, utils  # type: ignore
    from kubernetes.utils.create_from_yaml import FailToCreateError  # type: ignore
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        'Running the worker manager requires the kubernetes module.\n'
        'Please run: pip install kubernetes'
    )

import logging
import os
import uuid
from argparse import ArgumentParser
from typing import Any, Dict, List, Optional

from urllib3.exceptions import MaxRetryError, NewConnectionError  # type: ignore

from .worker_manager import WorkerManager, WorkerJob


logger: logging.Logger = logging.getLogger(__name__)


class KubernetesWorkerManager(WorkerManager):
    NAME: str = 'kubernetes'
    DESCRIPTION: str = 'Worker manager for submitting jobs to a Kubernetes cluster'

    @staticmethod
    def add_arguments_to_subparser(subparser: ArgumentParser) -> None:
        # Kubernetes arguments
        subparser.add_argument(
            '--cluster-host', type=str, help='Host address of the Kubernetes cluster', required=True
        )
        subparser.add_argument(
            '--auth-token', type=str, help='Kubernetes cluster authorization token', required=True,
        )
        subparser.add_argument(
            '--cert-path',
            type=str,
            help='Path to the SSL cert for the Kubernetes cluster',
            required=True,
        )
        subparser.add_argument(
            '--nfs-volume-name', type=str, help='Name of the persistent volume for the NFS server.',
        )

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

        # Configure and initialize Kubernetes client
        configuration: client.Configuration = client.Configuration()
        configuration.api_key_prefix['authorization'] = 'Bearer'
        configuration.api_key['authorization'] = args.auth_token
        configuration.host = args.cluster_host
        configuration.ssl_ca_cert = args.cert_path
        if configuration.host == "https://codalab-control-plane:8443":
            # Don't verify SSL if we are connecting to a local cluster for testing / development.
            configuration.verify_ssl = False
            configuration.ssl_ca_cert = None
            del configuration.api_key_prefix['authorization']
            del configuration.api_key['authorization']
            configuration.debug = False

        self.k8_client: client.ApiClient = client.ApiClient(configuration)
        self.k8_api: client.CoreV1Api = client.CoreV1Api(self.k8_client)
        self.nfs_volume_name: Optional[str] = args.nfs_volume_name

    def get_worker_jobs(self) -> List[WorkerJob]:
        try:
            # Fetch the running pods
            pods: client.V1PodList = self.k8_api.list_namespaced_pod(
                'default', field_selector='status.phase==Running'
            )
            logger.debug(pods.items)
            return [WorkerJob(True) for pod in pods.items if 'cl-worker' in pod.metadata.name]
        except (client.ApiException, MaxRetryError, NewConnectionError) as e:
            logger.error(f'Exception when calling Kubernetes CoreV1Api->list_namespaced_pod: {e}')
            return []

    def start_worker_job(self) -> None:
        # This needs to be a unique directory since jobs may share a host
        work_dir_prefix: str = (
            self.args.worker_work_dir_prefix if self.args.worker_work_dir_prefix else '/tmp/'
        )
        worker_id: str = uuid.uuid4().hex
        worker_name: str = f'cl-worker-{worker_id}'
        work_dir: str = os.path.join(work_dir_prefix, 'codalab-worker-scratch')
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
                            {
                                "name": self.nfs_volume_name if self.nfs_volume_name else 'workdir',
                                "mountPath": work_dir,
                            },
                        ],
                    }
                ],
                'volumes': [
                    {'name': 'dockersock', 'hostPath': {'path': '/var/run/docker.sock'}},
                    {
                        "name": self.nfs_volume_name,
                        # When attaching a volume over NFS, use a persistent volume claim
                        "persistentVolumeClaim": {"claimName": f"{self.nfs_volume_name}-claim"},
                    }
                    if self.nfs_volume_name
                    else {"name": 'workdir', "hostPath": {"path": work_dir}},
                ],
                'restartPolicy': 'Never',  # Only run a job once
            },
        }

        # Start a worker pod on the k8s cluster
        logger.debug('Starting worker {} with image {}'.format(worker_id, worker_image))
        try:
            utils.create_from_dict(self.k8_client, config)
        except (client.ApiException, FailToCreateError, MaxRetryError, NewConnectionError) as e:
            logger.error(f'Exception when calling Kubernetes utils->create_from_dict: {e}')
