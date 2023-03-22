import datetime
import logging
from dateutil import tz
from typing import Any, Dict, Optional, Tuple
from urllib3.exceptions import MaxRetryError, NewConnectionError  # type: ignore

from kubernetes import client, utils
from kubernetes.utils.create_from_yaml import FailToCreateError
from kubernetes.client.rest import ApiException

from codalab.worker.docker_utils import DEFAULT_RUNTIME
from codalab.common import BundleRuntime
from codalab.worker.runtime import Runtime

import os
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger: logging.Logger = logging.getLogger(__name__)

removeprefix = lambda l, p: l[len(p) :]


class KubernetesRuntime(Runtime):
    """Runtime that launches Kubernetes pods."""

    @property
    def name(self) -> str:
        return BundleRuntime.KUBERNETES.value

    def __init__(self, work_dir: str, auth_token: str, cluster_host: str, cert_path: str):
        # Configure and initialize Kubernetes client
        self.work_dir = work_dir

        # TODO: Unify this code with the client setup steps in kubernetes_worker_manager.py
        configuration: client.Configuration = client.Configuration()
        configuration.api_key_prefix['authorization'] = 'Bearer'
        configuration.api_key['authorization'] = auth_token
        configuration.host = cluster_host
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

    def get_nvidia_devices(self, use_docker=True):
        """
        Returns a Dict[index, UUID] of all NVIDIA devices available to docker

        Arguments:
            use_docker: whether or not to use a docker container to run nvidia-smi.

        Raises docker.errors.ContainerError if GPUs are unreachable,
            docker.errors.ImageNotFound if the CUDA image cannot be pulled
            docker.errors.APIError if another server error occurs
        """
        return {}

    def get_container_ip(self, network_name: str, pod_name: str):
        """Returns (finished boolean, exitcode or None of bundle, failure message or None)"""
        pod = self.k8_api.read_namespaced_pod_status(pod_name, "default")
        return pod.status.pod_ip

    def start_bundle_container(
        self,
        bundle_path,
        uuid,
        dependencies,
        command,
        docker_image,
        network=None,
        cpuset=None,
        gpuset=None,
        request_cpus=0,
        request_gpus=0,
        memory_bytes=0,
        detach=True,
        tty=False,
        runtime=DEFAULT_RUNTIME,
        shared_memory_size_gb=1,
    ) -> str:
        if not command.endswith(';'):
            command = '{};'.format(command)
        # Explicitly specifying "/bin/bash" instead of "bash" for bash shell to avoid the situation when
        # the program can't find the symbolic link (default is "/bin/bash") of bash in the environment
        command = ['/bin/bash', '-c', '( %s ) >stdout 2>stderr' % command]
        working_directory = '/' + uuid
        container_name = 'codalab-run-%s' % uuid
        # If we only need one CPU, only request 0.5 CPUs. This way, workers with only one CPU,
        # for example during integration tests, can still run the job
        # (as some overhead may be taken by other things in the cluster).
        limits = {'cpu': request_cpus, 'memory': memory_bytes}
        requests = {'cpu': 0.5 if request_cpus == 1 else request_cpus, 'memory': memory_bytes}
        if request_gpus > 0:
            limits['nvidia.com/gpu'] = request_gpus
            requests['nvidia.com/gpu'] = request_gpus
        config: Dict[str, Any] = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {'name': container_name},
            'spec': {
                'containers': [
                    {
                        'name': container_name,
                        'image': docker_image,
                        'command': command,
                        'workingDir': working_directory,
                        'env': [
                            {'name': 'HOME', 'value': working_directory},
                            {'name': 'CODALAB', 'value': 'true'},
                        ],
                        'resources': {'limits': limits, 'requests': requests},
                        # Mount only the needed dependencies as read-only and the working directory of the bundle,
                        # rather than mounting all of self.work_dir.
                        'volumeMounts': [
                            {
                                'name': 'workdir',
                                'mountPath': working_directory,
                                'subPath': removeprefix(bundle_path, self.work_dir).lstrip("/"),
                            }
                        ]
                        + [
                            {
                                'name': 'workdir',
                                'mountPath': mounted_dep_path,
                                'subPath': removeprefix(dep_path, self.work_dir).lstrip("/"),
                            }
                            for dep_path, mounted_dep_path in dependencies
                        ],
                    }
                ],
                'volumes': [{'name': 'workdir', 'hostPath': {'path': self.work_dir}},],
                'restartPolicy': 'Never',  # Only run a job once
            },
        }

        logger.warn('Starting job {} with image {}'.format(container_name, docker_image))
        try:
            pod = utils.create_from_dict(self.k8_client, config)
        except (client.ApiException, FailToCreateError, MaxRetryError, NewConnectionError) as e:
            logger.error(f'Exception when calling Kubernetes utils->create_from_dict...: {e}')
            raise e

        return pod[0].metadata.name

    def get_container_stats(self, pod_name: str):
        # TODO (Ashwin): implement
        return {}

    def get_container_stats_with_docker_stats(self, pod_name: str):
        """Returns the cpu usage and memory limit of a container using the Docker Stats API."""
        # TODO (Ashwin): implement
        return 0.0, 0

    def container_exists(self, pod_name: str) -> bool:
        try:
            self.k8_api.read_namespaced_pod_status(pod_name, "default")
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            logger.error(
                f'Exception when calling Kubernetes api->read_namespaced_pod_status...: {e}'
            )
            raise e

    def check_finished(self, pod_name: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """Returns (finished boolean, exitcode or None of bundle, failure message or None)"""
        try:
            pod = self.k8_api.read_namespaced_pod_status(pod_name, "default")
        except ApiException as e:
            if e.status == 404:
                # Pod no longer exists
                return (True, None, None)
            logger.error(
                f'Exception when calling Kubernetes api->read_namespaced_pod_status...: {e}'
            )
            raise e
        if pod.status.phase in ("Succeeded", "Failed"):
            statuses = pod.status.container_statuses
            if statuses is None or len(statuses) == 0 or statuses[0].state.terminated is None:
                return (False, None, None)
            exitcode = statuses[0].state.terminated.exit_code
            return (
                True,
                exitcode,
                pod.status.container_statuses[0].state.terminated.reason if exitcode != 0 else None,
            )
        return (False, None, None)

    def get_container_running_time(self, pod_name: str) -> int:
        try:
            pod = self.k8_api.read_namespaced_pod_status(pod_name, "default")
        except ApiException as e:
            if e.status == 404:
                # Pod no longer exists
                return 0
            logger.error(
                f'Exception when calling Kubernetes api->read_namespaced_pod_status...: {e}'
            )
            raise e
        statuses = pod.status.container_statuses
        if statuses is None or len(statuses) == 0:
            # Pod does not exist
            return 0
        state = statuses[0].state
        if state.running:
            return (datetime.datetime.now(tz.tzutc()) - state.running.started_at).total_seconds()
        elif state.terminated:
            return (state.terminated.finished_at - state.terminated.started_at).total_seconds()
        elif state.waiting:
            logger.debug("get_container_running_time: pod state is waiting: %s", state)
        else:
            logger.info("get_container_running_time: pod info couldn't be parsed, is: %s", pod)
        return 0

    def kill(self, pod_name: str):
        return self.remove(pod_name)

    def remove(self, pod_name: str):
        try:
            self.k8_api.delete_namespaced_pod(pod_name, "default")
        except ApiException as e:
            if e.status != 404:
                logger.error(
                    f'Exception when calling Kubernetes api->delete_namespaced_pod...: {e}'
                )
                raise e

    def get_node_availability_stats(self) -> dict:
        node_name = os.getenv("CODALAB_KUBERNETES_NODE_NAME")
        node = self.k8_api.read_node(name=node_name)
        allocatable = node.status.allocatable

        return {
            'cpus': int(allocatable.get('cpu')),
            'gpus': int(allocatable.get('nvidia.com/gpu') or '0'),
            'memory_bytes': int(utils.parse_quantity(allocatable.get('memory'))),
            'free_disk_bytes': int(utils.parse_quantity(allocatable.get('ephemeral-storage'))),
        }
