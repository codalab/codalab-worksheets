"""
docker_utils
General collection of Codalab-specific stateless utility functions to work with Docker.
Most are wrappers around the official Docker python client.
A preexisting client may be passed as a keyword parameter to all functions but one is automatically
created if not.
"""

import logging
import os
from typing import Optional, Tuple
import docker
from dateutil import parser, tz
import datetime
import re
import traceback
from codalab.common import BundleRuntime
from codalab.worker.runtime import Runtime

MIN_API_VERSION = '1.17'
NVIDIA_RUNTIME = 'nvidia'
DEFAULT_RUNTIME = 'runc'
DEFAULT_DOCKER_TIMEOUT = 720
DEFAULT_CONTAINER_RUNNING_TIME = 0
# Docker Registry HTTP API v2 URI prefix
URI_PREFIX = 'https://hub.docker.com/v2/repositories/'

# This specific error happens when a user specifies an image with an incompatible version of CUDA
NVIDIA_MOUNT_ERROR_REGEX = (
    '[\s\S]*OCI runtime create failed[\s\S]*stderr:[\s\S]*nvidia-container-cli: '
    'mount error: file creation failed:[\s\S]*nvidia-smi'
)
# This error happens when the memory requested by user is not enough to start a docker container
MEMORY_LIMIT_ERROR_REGEX = (
    '[\s\S]*OCI runtime create failed[\s\S]*failed to write[\s\S]*'
    'memory.limit_in_bytes: device or resource busy[\s\S]*'
)

logger = logging.getLogger(__name__)


def parse_image_progress(image_info={}):
    """
    As a docker image is being pulled, we have access to a stream of image info.
    This helper takes in image info and returns a consise progress string.

    Example Input:
    image_info = {
        progressDetail: { current: 20320000, total: 28540000 }
        progress: '[===============>     ]  20.32MB/28.54MB'
    }

    Example Return:
    '20.32MB/28.54MB (71% done)'
    """
    progress_detail = image_info.get('progressDetail', {})
    current = progress_detail.get('current')
    total = progress_detail.get('total')

    if total and current:
        percentage = current * 100 / total
        progress = image_info.get('progress')

        if progress:
            progress_parts = progress.split(']')
            concise_progress = progress_parts[-1].strip()
            return '%s (%d%% done)' % (concise_progress, percentage)
        return '(%d%% done)' % percentage
    return ''


def wrap_exception(message):
    def decorator(f):
        def wrapper(*args, **kwargs):
            def format_error_message(exception):
                return '{}: {}'.format(message, exception)

            def check_for_user_error(exception):
                error_message = format_error_message(exception)
                if re.match(NVIDIA_MOUNT_ERROR_REGEX, str(exception)):
                    raise DockerUserErrorException(error_message)
                elif re.match(MEMORY_LIMIT_ERROR_REGEX, str(exception)):
                    raise DockerUserErrorException(error_message)
                else:
                    raise DockerException(error_message)

            try:
                return f(*args, **kwargs)
            except DockerException as e:
                raise DockerException(format_error_message(e))
            except docker.errors.APIError as e:
                check_for_user_error(e)
            except (docker.errors.ImageNotFound, docker.errors.NotFound) as e:
                raise DockerException(format_error_message(e))

        return wrapper

    return decorator


class DockerException(Exception):
    def __init__(self, message):
        super(DockerException, self).__init__(message)


class DockerUserErrorException(Exception):
    def __init__(self, message):
        super(DockerUserErrorException, self).__init__(message)


class DockerRuntime(Runtime):
    """Runtime that launches Docker containers."""

    @property
    def name(self) -> str:
        return BundleRuntime.DOCKER.value

    def __init__(self):
        self.client = docker.from_env(timeout=DEFAULT_DOCKER_TIMEOUT)

    @wrap_exception('Unable to use Docker')
    def test_version(self):
        version_info = self.client.version()
        if list(map(int, version_info['ApiVersion'].split('.'))) < list(
            map(int, MIN_API_VERSION.split('.'))
        ):
            raise DockerException('Please upgrade your version of Docker')

    @wrap_exception('Problem establishing NVIDIA support')
    def get_available_runtime(self):
        self.test_version()
        try:
            nvidia_devices = self.get_nvidia_devices()
            if len(nvidia_devices) == 0:
                raise DockerException(
                    "nvidia-docker runtime available but no NVIDIA devices detected"
                )
            return NVIDIA_RUNTIME
        except DockerException as e:
            logger.warning("Cannot initialize NVIDIA runtime, no GPU support: %s", e)
            return DEFAULT_RUNTIME

    @wrap_exception('Problem getting NVIDIA devices')
    def get_nvidia_devices(self, use_docker=True):
        """
        Returns a Dict[index, UUID] of all NVIDIA devices available to docker

        Arguments:
            use_docker: whether or not to use a docker container to run nvidia-smi.

        Raises docker.errors.ContainerError if GPUs are unreachable,
            docker.errors.ImageNotFound if the CUDA image cannot be pulled
            docker.errors.APIError if another server error occurs
        """
        cuda_image = 'nvidia/cuda:9.0-cudnn7-devel-ubuntu16.04'
        nvidia_command = 'nvidia-smi --query-gpu=index,uuid --format=csv,noheader'
        if use_docker:
            self.client.images.pull(cuda_image)
            output = self.client.containers.run(
                cuda_image,
                nvidia_command,
                runtime=NVIDIA_RUNTIME,
                detach=False,
                stdout=True,
                remove=True,
            )
            gpus = output.decode()
        else:
            # use the singularity runtime to run nvidia-smi
            # img = Client.pull('docker://' + cuda_image, pull_folder='/tmp')
            # output = Client.execute(img, nvidia_command, options=['--nv'])
            # if output['return_code'] != 0:
            #     raise SingularityError
            # gpus = output['message']
            gpus = ""
        # Get newline delimited gpu-index, gpu-uuid list
        logger.info("GPUs: " + str(gpus.split('\n')[:-1]))
        return {
            gpu.split(',')[0].strip(): gpu.split(',')[1].strip() for gpu in gpus.split('\n')[:-1]
        }

    @wrap_exception('Unable to fetch Docker container ip')
    def get_container_ip(self, network_name: str, container_id: str):
        # Unfortunately docker SDK doesn't update the status of Container objects
        # so we re-fetch them from the API again to get the most recent state
        container = self.client.containers.get(container_id)
        try:
            return container.attrs["NetworkSettings"]["Networks"][network_name]["IPAddress"]
        except KeyError:  # if container ip cannot be found in provided network, return None
            return None

    @wrap_exception('Unable to start Docker container')
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
        docker_command = ['/bin/bash', '-c', '( %s ) >stdout 2>stderr' % command]
        docker_bundle_path = '/' + uuid
        volumes = self.get_bundle_container_volume_binds(
            bundle_path, docker_bundle_path, dependencies
        )
        environment = {'HOME': docker_bundle_path, 'CODALAB': 'true'}
        working_dir = docker_bundle_path
        # Unset entrypoint regardless of image
        entrypoint = ''
        cpuset_str = ','.join(cpuset) if cpuset else ''
        # Get user/group that owns the bundle directory
        # Then we can ensure that any created files are owned by the user/group
        # that owns the bundle directory, not root.
        bundle_stat = os.stat(bundle_path)
        uid = bundle_stat.st_uid
        gid = bundle_stat.st_gid
        # TODO: Fix potential permissions issues arising from this setting
        # This can cause problems if users expect to run as a specific user
        user = '%s:%s' % (uid, gid)

        if runtime == NVIDIA_RUNTIME:
            # nvidia-docker runtime uses this env variable to allocate GPUs
            environment['NVIDIA_VISIBLE_DEVICES'] = ','.join(gpuset) if gpuset else ''

        # Name the container with the UUID for readability
        container_name = 'codalab_run_%s' % uuid
        try:
            container = self.client.containers.run(
                image=docker_image,
                command=docker_command,
                name=container_name,
                network=network,
                mem_limit=memory_bytes,
                shm_size=f"{shared_memory_size_gb}G",
                cpuset_cpus=cpuset_str,
                environment=environment,
                working_dir=working_dir,
                entrypoint=entrypoint,
                volumes=volumes,
                user=user,
                detach=detach,
                runtime=runtime,
                tty=tty,
                stdin_open=tty,
            )
            logger.debug(
                'Started Docker container for UUID %s, container ID %s,', uuid, container.id
            )
        except docker.errors.APIError:
            # The container failed to start, so it's in the CREATED state
            # If we try to re-run the container again, we'll get a 409 CONFLICT
            # because a container with the same name already exists. So, we try to remove
            # the container here.
            try:
                self.client.api.remove_container(container_name, force=True)
            except Exception:
                logger.warning("Failed to clean up Docker container after failed launch.")
                traceback.print_exc()
            raise
        return container.id

    def get_bundle_container_volume_binds(self, bundle_path, docker_bundle_path, dependencies):
        """
        Returns a volume bindings dict for the bundle path and dependencies given
        """
        binds = {
            os.path.abspath(dep_path): {'bind': docker_dep_path, 'mode': 'ro'}
            for dep_path, docker_dep_path in dependencies
        }
        binds[bundle_path] = {'bind': docker_bundle_path, 'mode': 'rw'}
        return binds

    @wrap_exception("Can't get container stats")
    def get_container_stats(self, container_id: str):
        # We don't use the stats API since it doesn't seem to be reliable, and
        # is definitely slow. This doesn't work on Mac.
        cgroup = None
        for path in ['/sys/fs/cgroup', '/cgroup']:
            if os.path.exists(path):
                cgroup = path
                break
        if cgroup is None:
            return {}

        stats = {}

        # Get CPU usage
        try:
            cpu_path = os.path.join(cgroup, 'cpuacct/docker', container_id, 'cpuacct.stat')
            with open(cpu_path) as f:
                for line in f:
                    key, value = line.split(' ')
                    # Convert jiffies to seconds
                    if key == 'user':
                        stats['time_user'] = int(value) / 100.0
                    elif key == 'system':
                        stats['time_system'] = int(value) / 100.0
        except Exception:
            pass

        # Get memory usage
        try:
            memory_path = os.path.join(
                cgroup, 'memory/docker', container_id, 'memory.usage_in_bytes'
            )
            with open(memory_path) as f:
                stats['memory'] = int(f.read())
        except Exception:
            pass

        return stats

    @wrap_exception('Unable to check Docker API for container')
    def get_container_stats_with_docker_stats(self, container_id: str):
        """Returns the cpu usage and memory limit of a container using the Docker Stats API."""
        if self.container_exists(container_id):
            try:
                container_stats: dict = self.client.containers.get(container_id).stats(stream=False)

                cpu_usage: float = self.get_cpu_usage(container_stats)
                memory_usage: float = self.get_memory_usage(container_stats)

                return cpu_usage, memory_usage
            except docker.errors.NotFound:
                raise
        else:
            return 0.0, 0

    def get_cpu_usage(self, container_stats: dict) -> float:
        """Calculates CPU usage from container stats returned from the Docker Stats API.
        The way of calculation comes from here:
        https://www.jcham.com/2016/02/09/calculating-cpu-percent-and-memory-percentage-for-containers/
        That method is also based on how the docker client calculates it:
        https://github.com/moby/moby/blob/131e2bf12b2e1b3ee31b628a501f96bbb901f479/api/client/stats.go#L309"""
        try:
            cpu_delta: int = (
                container_stats['cpu_stats']['cpu_usage']['total_usage']
                - container_stats['precpu_stats']['cpu_usage']['total_usage']
            )
            system_delta: int = (
                container_stats['cpu_stats']['system_cpu_usage']
                - container_stats['precpu_stats']['system_cpu_usage']
            )
            if system_delta > 0 and cpu_delta > 0:
                cpu_usage: float = float(cpu_delta / system_delta) * float(
                    len(container_stats['cpu_stats']['cpu_usage']['percpu_usage'])
                )
                return cpu_usage
            return 0.0
        except KeyError:
            # The stats returned may be missing some keys if the bundle is not fully ready or has exited.
            # We can just skip for now and wait until this function is called the next time.
            return 0.0

    def get_memory_usage(self, container_stats: dict) -> float:
        """Takes a dictionary of container stats returned by docker stats, returns memory usage"""
        try:
            memory_limit: float = container_stats['memory_stats']['limit']
            current_memory_usage: float = container_stats['memory_stats']['usage']
            return current_memory_usage / memory_limit
        except KeyError:
            return 0

    @wrap_exception('Unable to check Docker API for container')
    def container_exists(self, container_id):
        try:
            self.client.containers.get(container_id)
            return True
        except docker.errors.NotFound:
            return False

    @wrap_exception('Unable to check Docker container status')
    def check_finished(self, container_id: str) -> Tuple[bool, Optional[str], Optional[str]]:
        try:
            container = self.client.containers.get(container_id)
        except docker.errors.NotFound:
            return (True, None, 'Docker container not found')
        if container.status != 'running':
            # If the logs are nonempty, then something might have gone
            # wrong with the commands run before the user command,
            # such as bash or cd.
            stderr = container.logs(stderr=True, stdout=False)
            # Strip non-ASCII chars since failure_message is not Unicode
            # TODO: don't need to strip since we can support unicode?
            if len(stderr) > 0:
                failure_msg = stderr.decode('ascii', errors='ignore')
            else:
                failure_msg = None
            exitcode = container.attrs['State']['ExitCode']
            if exitcode == '137':
                failure_msg = 'Memory limit exceeded.'
            return (True, exitcode, failure_msg)
        return (False, None, None)

    @wrap_exception('Unable to check Docker container running time')
    def get_container_running_time(self, container_id: str):
        # Get the current container
        try:
            container = self.client.containers.get(container_id)
        except docker.errors.NotFound:
            # This usually happens when container gets accidentally removed or deleted
            return DEFAULT_CONTAINER_RUNNING_TIME
        # Load this container from the server again and update attributes with the new data.
        container.reload()
        # Calculate the start_time of the current container
        start_time = container.attrs['State']['StartedAt']
        # Calculate the end_time of the current container. If 'Status' of the current container is not 'exited',
        # then using the current time as end_time
        end_time = (
            container.attrs['State']['FinishedAt']
            if container.attrs['State']['Status'] == 'exited'
            else str(datetime.datetime.now(tz.tzutc()))
        )
        # Docker reports both the start_time and the end_time in ISO format. We currently use dateutil.parser.isoparse to
        # parse them. In Python3.7 or above, the built-in function datetime.fromisoformat() can be used to parse ISO
        # formatted datetime string directly.
        container_running_time = parser.isoparse(end_time) - parser.isoparse(start_time)
        return container_running_time.total_seconds()

    def kill(self, container_id: str):
        try:
            container = self.client.containers.get(container_id)
        except docker.errors.NotFound:
            return
        container.kill()

    def remove(self, container_id: str):
        try:
            container = self.client.containers.get(container_id)
        except docker.errors.NotFound:
            return
        container.remove(force=True)
