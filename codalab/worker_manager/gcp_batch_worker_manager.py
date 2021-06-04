try:
    from google.cloud.container_v1 import ClusterManagerClient  # type: ignore
    from google.oauth2 import service_account  # type: ignore
    from kubernetes import client, utils  # type: ignore
    from kubernetes.utils.create_from_yaml import FailToCreateError  # type: ignore
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        'Running the worker manager requires the kubernetes module.\n'
        'Please run: pip install kubernetes'
    )

import base64
import logging
import os
import uuid
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
        subparser.add_argument('--project', type=str, help='Name of the GCP project', required=True)
        subparser.add_argument('--cluster', type=str, help='Name of the GKE cluster', required=True)
        subparser.add_argument(
            '--zone', type=str, help='The availability zone of the GKE cluster', required=True
        )
        subparser.add_argument(
            '--credentials-path',
            type=str,
            help='Path to the GCP service account json file',
            required=True,
        )
        subparser.add_argument(
            '--cert-path', type=str, default='.', help='Path to the generated SSL cert.'
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

        # Authenticate via GCP
        credentials: service_account.Credentials = service_account.Credentials.from_service_account_file(
            self.args.credentials_path, scopes=['https://www.googleapis.com/auth/cloud-platform']
        )

        cluster_manager_client: ClusterManagerClient = ClusterManagerClient(credentials=credentials)
        cluster = cluster_manager_client.get_cluster(
            name=f'projects/{self.args.project}/locations/{self.args.zone}/clusters/{self.args.cluster}'
        )

        # Save SSL certificate to connect to the GKE cluster securely
        cert_path = os.path.join(self.args.cert_path, 'gke.crt')
        with open(cert_path, 'wb') as f:
            f.write(base64.b64decode(cluster.master_auth.cluster_ca_certificate))

        # Configure and initialize Kubernetes client
        configuration: client.Configuration = client.Configuration()
        configuration.host = f'https://{cluster.endpoint}:443'
        configuration.api_key = {'authorization': f'Bearer {credentials.token}'}
        configuration.verify_ssl = True
        configuration.ssl_ca_cert = cert_path
        client.Configuration.set_default(configuration)

        self.k8_client: client.ApiClient = client.ApiClient(configuration)
        self.k8_api: client.CoreV1Api = client.CoreV1Api(self.k8_client)

    def get_worker_jobs(self) -> List[WorkerJob]:
        try:
            # Fetch the running pods
            pods: client.V1PodList = self.k8_api.list_namespaced_pod(
                'default', field_selector='status.phase==Running'
            )
            logger.debug(pods.items)
            return [WorkerJob(True) for _ in pods.items]
        except client.ApiException as e:
            logger.error(f'Exception when calling Kubernetes CoreV1Api->list_namespaced_pod: {e}')
            return []

    def start_worker_job(self) -> None:
        # This needs to be a unique directory since jobs may share a host
        work_dir_prefix: str = (
            self.args.worker_work_dir_prefix if self.args.worker_work_dir_prefix else '/tmp/'
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
                'volumes': [
                    {'name': 'dockersock', 'hostPath': {'path': '/var/run/docker.sock'}},
                    {'name': 'workdir', 'hostPath': {'path': work_dir}},
                ],
                'restartPolicy': 'Never',  # Only run a job once
            },
        }

        # Use Kubernetes to start a worker on GCP
        logger.debug('Starting worker {} with image {}'.format(worker_id, worker_image))
        try:
            utils.create_from_dict(self.k8_client, config)
        except (client.ApiException, FailToCreateError) as e:
            logger.error(f'Exception when calling Kubernetes utils->create_from_dict: {e}')
