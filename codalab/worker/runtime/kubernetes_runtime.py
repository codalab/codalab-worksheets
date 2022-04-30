from typing import Tuple
from kubernetes import client, utils
from kubernetes.utils.create_from_yaml import FailToCreateError
from codalab.worker.docker_utils import DEFAULT_RUNTIME

from codalab.common import BundleRuntime
from codalab.worker.runtime import Runtime


class KubernetesRuntime(Runtime):
    """Runtime that launches Kubernetes pods."""

    @property
    def name(self):
        return BundleRuntime.KUBERNETES.value

    def __init__(self, auth_token, cluster_host, cert_path):
        # Configure and initialize Kubernetes client
        # TODO: unify this code with the client setup steps in kubernetes_worker_manager.py
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

    def get_container_ip(self, network_name, container):
        raise NotImplementedError

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
        memory_bytes=0,
        detach=True,
        tty=False,
        runtime=DEFAULT_RUNTIME,
        shared_memory_size_gb=1,
    ):
        raise NotImplementedError

    def get_container_stats(self, container):
        raise NotImplementedError

    def get_container_stats_with_docker_stats(self, container):
        """Returns the cpu usage and memory limit of a container using the Docker Stats API."""
        raise NotImplementedError

    def container_exists(self, container) -> bool:
        raise NotImplementedError

    def check_finished(self, container) -> Tuple[bool, str, str]:
        """Returns (finished boolean, exitcode or None of bundle, failure message or None)"""
        raise NotImplementedError

    @wrap_exception('Unable to check Docker container running time')
    def get_container_running_time(self, container) -> int:
        raise NotImplementedError
