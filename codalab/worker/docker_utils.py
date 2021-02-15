"""
docker_utils
General collection of Codalab-specific stateless utility functions to work with Docker.
Most are wrappers around the official Docker python client.
A preexisting client may be passed as a keyword parameter to all functions but one is automatically
created if not.
"""

import logging
import os
import docker
from dateutil import parser, tz
import datetime
import re
import requests

from requests.adapters import HTTPAdapter
import traceback
from urllib3.util.retry import Retry


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
client = docker.from_env(timeout=DEFAULT_DOCKER_TIMEOUT)


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


@wrap_exception('Unable to use Docker')
def test_version():
    version_info = client.version()
    if list(map(int, version_info['ApiVersion'].split('.'))) < list(
        map(int, MIN_API_VERSION.split('.'))
    ):
        raise DockerException('Please upgrade your version of Docker')


@wrap_exception('Problem establishing NVIDIA support')
def get_available_runtime():
    test_version()
    try:
        nvidia_devices = get_nvidia_devices()
        if len(nvidia_devices) == 0:
            raise DockerException("nvidia-docker runtime available but no NVIDIA devices detected")
        return NVIDIA_RUNTIME
    except DockerException as e:
        logger.warning("Cannot initialize NVIDIA runtime, no GPU support: %s", e)
        return DEFAULT_RUNTIME


@wrap_exception('Problem getting NVIDIA devices')
def get_nvidia_devices():
    """
    Returns a Dict[index, UUID] of all NVIDIA devices available to docker
    Raises docker.errors.ContainerError if GPUs are unreachable,
           docker.errors.ImageNotFound if the CUDA image cannot be pulled
           docker.errors.APIError if another server error occurs
    """
    cuda_image = 'nvidia/cuda:9.0-cudnn7-devel-ubuntu16.04'
    client.images.pull(cuda_image)
    nvidia_command = 'nvidia-smi --query-gpu=index,uuid --format=csv,noheader'
    output = client.containers.run(
        cuda_image, nvidia_command, runtime=NVIDIA_RUNTIME, detach=False, stdout=True, remove=True
    )
    # Get newline delimited gpu-index, gpu-uuid list
    output = output.decode()
    print(output.split('\n')[:-1])
    return {gpu.split(',')[0].strip(): gpu.split(',')[1].strip() for gpu in output.split('\n')[:-1]}


@wrap_exception('Unable to fetch Docker container ip')
def get_container_ip(network_name, container):
    # Unfortunately docker SDK doesn't update the status of Container objects
    # so we re-fetch them from the API again to get the most recent state
    container = client.containers.get(container.id)
    try:
        return container.attrs["NetworkSettings"]["Networks"][network_name]["IPAddress"]
    except KeyError:  # if container ip cannot be found in provided network, return None
        return None


@wrap_exception('Unable to start Docker container')
def start_bundle_container(
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
):
    if not command.endswith(';'):
        command = '{};'.format(command)
    # Explicitly specifying "/bin/bash" instead of "bash" for bash shell to avoid the situation when
    # the program can't find the symbolic link (default is "/bin/bash") of bash in the environment
    docker_command = ['/bin/bash', '-c', '( %s ) >stdout 2>stderr' % command]
    docker_bundle_path = '/' + uuid
    volumes = get_bundle_container_volume_binds(bundle_path, docker_bundle_path, dependencies)
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
        environment['NVIDIA_VISIBLE_DEVICES'] = ','.join(gpuset) if gpuset else 'all'

    # Name the container with the UUID for readability
    container_name = 'codalab_run_%s' % uuid
    try:
        container = client.containers.run(
            image=docker_image,
            command=docker_command,
            name=container_name,
            network=network,
            mem_limit=memory_bytes,
            shm_size='1G',
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
        logger.debug('Started Docker container for UUID %s, container ID %s,', uuid, container.id)
    except docker.errors.APIError:
        # The container failed to start, so it's in the CREATED state
        # If we try to re-run the container again, we'll get a 409 CONFLICT
        # because a container with the same name already exists. So, we try to remove
        # the container here.
        try:
            container.remove(force=True)
        except Exception:
            logger.warning("Failed to clean up Docker container after failed launch.")
            traceback.print_exc()
        raise
    return container


def get_bundle_container_volume_binds(bundle_path, docker_bundle_path, dependencies):
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
def get_container_stats(container):
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
        cpu_path = os.path.join(cgroup, 'cpuacct/docker', container.id, 'cpuacct.stat')
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
        memory_path = os.path.join(cgroup, 'memory/docker', container.id, 'memory.usage_in_bytes')
        with open(memory_path) as f:
            stats['memory'] = int(f.read())
    except Exception:
        pass

    return stats


@wrap_exception('Unable to check Docker API for container')
def container_exists(container):
    try:
        client.containers.get(container.id)
        return True
    except docker.errors.NotFound:
        return False


@wrap_exception('Unable to check Docker container status')
def check_finished(container):
    # Unfortunately docker SDK doesn't update the status of Container objects
    # so we re-fetch them from the API again to get the most recent state
    if container is None:
        return (True, None, 'Docker container not found')
    container = client.containers.get(container.id)
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
def get_container_running_time(container):
    # This usually happens when container gets accidentally removed or deleted
    if container is None:
        return DEFAULT_CONTAINER_RUNNING_TIME
    # Get the current container
    container = client.containers.get(container.id)
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


@wrap_exception('Unable to get image size without pulling from Docker Hub')
def get_image_size_without_pulling(image_spec):
    """
    Get the compressed size of a docker image without pulling it from Docker Hub. Note that since docker-py doesn't
    report the accurate compressed image size, e.g. the size reported from the RegistryData object, we then switch
    to use Docker Registry HTTP API V2
    :param image_spec: image_spec can have two formats as follows:
            1. "repo:tag": 'codalab/default-cpu:latest'
            2. "repo@digest": studyfang/hotpotqa@sha256:f0ee6bc3b8deefa6bdcbb56e42ec97b498befbbca405a630b9ad80125dc65857
    :return: 1. when fetching from Docker rest API V2 succeeded, return the compressed image size in bytes
             2. when fetching from Docker rest API V2 failed, return None
    """
    logger.info("Downloading tag information for {}".format(image_spec))

    # Both types of image_spec have the ':' character. The '@' character is unique in the type 1.
    image_tag = None
    image_digest = None
    if '@' in image_spec:
        image_name, image_digest = image_spec.split('@')
    else:
        image_name, image_tag = image_spec.split(":")
    # Example URL:
    # 1. image with namespace: https://hub.docker.com/v2/repositories/<namespace>/<image_name>/tags/?page=<page_number>
    #       e.g. https://hub.docker.com/v2/repositories/codalab/default-cpu/tags/?page=1
    # 2. image without namespace: https://hub.docker.com/v2/repositories/library/<image_name>/tags/?page=<page_number>
    #       e.g. https://hub.docker.com/v2/repositories/library/ubuntu/tags/?page=1
    # Each page will return at most 10 tags
    # URI prefix of an image without namespace will be adjusted to https://hub.docker.com/v2/repositories/library
    uri_prefix_adjusted = URI_PREFIX + '/library/' if '/' not in image_name else URI_PREFIX
    request = uri_prefix_adjusted + image_name + '/tags/?page='
    image_size_bytes = None
    page_number = 1

    requests_session = requests.Session()
    # Retry 5 times, sleeping for [0.1s, 0.2s, 0.4s, ...] between retries.
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[413, 429, 500, 502, 503, 504])
    requests_session.mount('https://', HTTPAdapter(max_retries=retries))

    while True:
        response = requests_session.get(url=request + str(page_number))
        data = response.json()
        if len(data['results']) == 0:
            break
        # Get the size information from the matched image
        if image_tag:
            for result in data['results']:
                if result['name'] == image_tag:
                    image_size_bytes = result['full_size']
                    return image_size_bytes
        if image_digest:
            for result in data['results']:
                for image in result['images']:
                    if image_digest in image['digest']:
                        image_size_bytes = result['full_size']
                        return image_size_bytes

        page_number += 1

    return image_size_bytes
