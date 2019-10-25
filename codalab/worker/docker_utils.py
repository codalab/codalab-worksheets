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

from codalab.lib.formatting import parse_size


MIN_API_VERSION = '1.17'
NVIDIA_RUNTIME = 'nvidia'
DEFAULT_RUNTIME = 'runc'
DEFAULT_TIMEOUT = 720


logger = logging.getLogger(__name__)
client = docker.from_env(timeout=DEFAULT_TIMEOUT)


def wrap_exception(message):
    def decorator(f):
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except DockerException as e:
                raise DockerException(message + ': ' + str(e))
            except (docker.errors.APIError, docker.errors.ImageNotFound) as e:
                raise DockerException(message + ': ' + str(e))

        return wrapper

    return decorator


class DockerException(Exception):
    def __init__(self, message):
        super(DockerException, self).__init__(message)


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
        logger.error("Cannot initialize NVIDIA runtime, no GPU support: %s", e)
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
    # Impose a minimum container request memory 4mb, same as docker's minimum allowed value
    # https://docs.docker.com/config/containers/resource_constraints/#limit-a-containers-access-to-memory
    # When using the REST api, it is allowed to set Memory to 0 but that means the container has unbounded
    # access to the host machine's memory, which we have decided to not allow
    if memory_bytes < parse_size('4m'):
        raise DockerException('Minimum memory must be 4m ({} bytes)'.format(parse_size('4m')))
    if not command.endswith(';'):
        command = '{};'.format(command)
    docker_command = ['bash', '-c', '( %s ) >stdout 2>stderr' % command]
    docker_bundle_path = '/' + uuid
    volumes = get_bundle_container_volume_binds(bundle_path, docker_bundle_path, dependencies)
    environment = {'HOME': docker_bundle_path}
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
    container = client.containers.run(
        image=docker_image,
        command=docker_command,
        name=container_name,
        network=network,
        mem_limit=memory_bytes,
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
