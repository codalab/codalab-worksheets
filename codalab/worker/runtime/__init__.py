from typing import Optional, Tuple
import docker

DEFAULT_RUNTIME = 'runc'  # copied from docker_utils to avoid a circular import


# Any errors that relate to runtime API calls failing.
RuntimeAPIError = (docker.errors.APIError,)


class Runtime:
    """Base class for a runtime."""

    @property
    def name(self) -> str:
        raise NotImplementedError

    def get_nvidia_devices(self, use_docker=True):
        """
        Returns a Dict[index, UUID] of all NVIDIA devices available to docker

        Arguments:
            use_docker: whether or not to use a docker container to run nvidia-smi.

        Raises docker.errors.ContainerError if GPUs are unreachable,
            docker.errors.ImageNotFound if the CUDA image cannot be pulled
            docker.errors.APIError if another server error occurs
        """
        raise NotImplementedError

    def get_container_ip(self, network_name: str, container_id: str):
        raise NotImplementedError

    def start_bundle_container(
        self,
        bundle_path,
        uuid,
        dependencies,  # array of (original path, mounted path)
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
        """Starts bundle job. Should return a unique identifier that can be used to fetch the job later."""
        raise NotImplementedError

    def get_container_stats(self, container_id: str):
        raise NotImplementedError

    def get_container_stats_with_docker_stats(self, container_id: str):
        """Returns the cpu usage and memory limit of a container using the Docker Stats API."""
        raise NotImplementedError

    def container_exists(self, container_id: str) -> bool:
        raise NotImplementedError

    def check_finished(self, container_id: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """Returns (finished boolean, exitcode or None of bundle, failure message or None)"""
        raise NotImplementedError

    def get_container_running_time(self, container_id: str) -> int:
        raise NotImplementedError

    def kill(self, container_id: str):
        raise NotImplementedError

    def remove(self, container_id: str):
        raise NotImplementedError
