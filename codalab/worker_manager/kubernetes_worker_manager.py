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
from codalab.common import BundleRuntime
import tempfile

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
            '--bundle-runtime',
            choices=[BundleRuntime.DOCKER.value, BundleRuntime.KUBERNETES.value,],
            default=BundleRuntime.DOCKER.value,
            help='The runtime through which the worker will run bundles. The options are docker (default) or kubernetes.',
        )
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
            '--cert',
            type=str,
            help='Contents of the SSL cert for the Kubernetes cluster',
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

        self.bundle_runtime = args.bundle_runtime
        self.auth_token = args.auth_token
        self.cluster_host = args.cluster_host
        self.cert_path = args.cert_path
        self.cert = args.cert

        # Configure and initialize Kubernetes client
        configuration: client.Configuration = client.Configuration()
        configuration.api_key_prefix['authorization'] = 'Bearer'
        configuration.api_key['authorization'] = args.auth_token
        configuration.host = args.cluster_host
        if args.cert_path == "/dev/null" and args.cert != "/dev/null":
            # Create temp file to store kubernetes cert, as we need to pass in a file path.
            # TODO: Delete the file afterwards (upon CodaLab service stop?)
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
                f.write(
                    args.cert.replace(r'\n', '\n')
                )  # Properly add newlines, which appear as "\n" if specified in the environment variable.
                cert_path = f.name
                logger.info('Temporarily writing kubernetes cert to: %s', cert_path)
        else:
            cert_path = args.cert_path
        configuration.ssl_ca_cert = cert_path
        if configuration.host == "https://codalab-control-plane:6443":
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

        command.extend(['--bundle-runtime', self.bundle_runtime])
        command.extend(['--kubernetes-cluster-host', self.cluster_host])
        command.extend(['--kubernetes-auth-token', self.auth_token])
        command.extend(['--kubernetes-cert-path', self.cert_path])
        command.extend(['--kubernetes-cert', self.cert])

        worker_image: str = 'codalab/worker:' + os.environ.get('CODALAB_VERSION', 'latest')

        # If we only need one CPU, only request 0.5 CPUs. This way, workers with only one CPU,
        # for example during integration tests, can still run the job
        # (as some overhead may be taken by other things in the cluster).
        limits = {'cpu': self.args.cpus, 'memory': f'{self.args.memory_mb}Mi'}
        requests = {
            'cpu': 0.5 if self.args.cpus == 1 else self.args.cpus,
            'memory': f'{self.args.memory_mb}Mi',
        }
        if self.args.gpus:
            limits['nvidia.com/gpu'] = self.args.gpus
            requests['nvidia.com/gpu'] = self.args.gpus
        config: Dict[str, Any] = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {'name': worker_name, 'labels': {'app': 'cl-worker'}},
            'spec': {
                'containers': [
                    {
                        'name': f'{worker_name}-container',
                        'image': worker_image,
                        'command': command,
                        'env': [
                            {'name': 'CODALAB_USERNAME', 'value': self.codalab_username},
                            {'name': 'CODALAB_PASSWORD', 'value': self.codalab_password},
                            {
                                'name': 'CODALAB_KUBERNETES_NODE_NAME',
                                'valueFrom': {'fieldRef': {'fieldPath': 'spec.nodeName'}},
                            },
                        ],
                        'resources': {'limits': limits, 'requests': requests},
                        'volumeMounts': [
                            {
                                "name": self.nfs_volume_name if self.nfs_volume_name else 'workdir',
                                "mountPath": work_dir,
                            },
                        ],
                    }
                ],
                # Only one worker pod should be scheduled per node.
                'affinity': {
                    'podAntiAffinity': {
                        'requiredDuringSchedulingIgnoredDuringExecution': [
                            {
                                'podAffinityTerm': {
                                    'labelSelector': {
                                        "matchExpressions": [
                                            {
                                                "key": "app",
                                                "operator": "In",
                                                "values": ["cl-worker"],
                                            }
                                        ]
                                    },
                                },
                                'topologyKey': 'topology.kubernetes.io/hostname',
                            }
                        ]
                    }
                },
                'volumes': [
                    {'name': 'certpath', 'hostPath': {'path': self.cert_path}},
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
        logger.error('Starting worker {} with image {}'.format(worker_id, worker_image))
        try:
            utils.create_from_dict(self.k8_client, config)
        except (client.ApiException, FailToCreateError, MaxRetryError, NewConnectionError) as e:
            logger.error(f'Exception when calling Kubernetes utils->create_from_dict: {e}')
