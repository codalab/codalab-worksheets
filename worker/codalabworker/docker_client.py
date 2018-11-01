import logging
import os
import sys
import docker

from formatting import size_str, parse_size

logger = logging.getLogger(__name__)


def wrap_exception(message):
    def decorator(f):
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except DockerException as e:
                raise DockerException(message + ': ' + e.message)
            except (docker.errors.APIError, docker.errors.ImageNotFound) as e:
                raise DockerException(message + ': ' + str(e))

        return wrapper

    return decorator


class DockerException(Exception):
    def __init__(self, message):
        super(DockerException, self).__init__(message)


class DockerClient(object):
    """
    TODO(bkgoksel): Add module doc
    """

    MIN_API_VERSION = '1.17'
    NVIDIA_RUNTIME = 'nvidia'
    DEFAULT_RUNTIME = 'runc'

    def __init__(self):
        docker_host = os.environ.get('DOCKER_HOST') or 'unix://var/run/docker.sock'
        cert_path = os.environ.get('DOCKER_CERT_PATH') or None
        if cert_path:
            tls_config = docker.tls.TLSConfig(
                client_cert=(
                    os.path.join(cert_path, 'cert.pem'),
                    os.path.join(cert_path, 'key.pem'),
                ),
                ca_cert=os.path.join(cert_path, 'ca.pem'),
                assert_hostname=False,
            )
            self._client = docker.APIClient(base_url=docker_host, version='auto', tls=tls_config)
        else:
            self._client = docker.APIClient(base_url=docker_host, version='auto')

        # Test to make sure that a connection can be established.
        try:
            self.test()
        except DockerException:
            print >>sys.stderr, """
On Linux, a valid Docker installation should create a Unix socket at
/var/run/docker.sock.

On Mac, DOCKER_HOST and optionally DOCKER_CERT_PATH should be defined. You need
to run the worker from the Docker shell.
"""
            raise

        # Check if nvidia-docker-plugin is available
        try:
            self._test_nvidia_docker()
        except DockerException:
            print >>sys.stderr, """
nvidia-docker-plugin not available, no GPU support on this worker.
"""
            self._docker_runtime = DockerClient.DEFAULT_RUNTIME
        else:
            self._docker_runtime = DockerClient.NVIDIA_RUNTIME

    def _test_nvidia_docker(self):
        """Throw exception if nvidia-docker runtime is not available."""
        try:
            nvidia_devices = self._get_nvidia_devices()
            if len(nvidia_devices) == 0:
                raise DockerException(
                    "nvidia-docker runtime available but no NVIDIA devices detected"
                )
        except Exception as e:
            raise DockerException(e.message)

    def get_nvidia_devices_info(self):
        """
        Returns the index of each NVIDIA device if NVIDIA runtime is available and there are devices.
        Otherwise returns None
        """
        if self._docker_runtime != DockerClient.NVIDIA_RUNTIME:
            return None
        return self._get_nvidia_devices()

    def _get_nvidia_devices(self):
        """
        Returns the index of each NVIDIA device if NVIDIA runtime is available.
        Otherwise raises an Exception
        """
        cuda_image_repo = 'nvidia/cuda'
        cuda_image_tag = '9.0-cudnn7-devel-ubuntu16.04'
        self._client.pull(cuda_image_repo, tag=cuda_image_tag)
        cuda_image_full = '{0}:{1}'.format(cuda_image_repo, cuda_image_tag)
        nvidia_command = 'nvidia-smi --query-gpu=index --format=csv,noheader'
        container = self._client.create_container(
            cuda_image_full, command=nvidia_command, runtime=DockerClient.NVIDIA_RUNTIME
        )
        self._client.start(container.get('Id'))
        container_status = self._client.wait(container.get('Id'))
        if container_status.get('StatusCode') != 0:
            raise DockerException(
                'Problem running nvidia-smi to get NVIDIA devices: %s'
                % self._client.logs(container.get('Id'))
            )
        else:
            indices = self._client.logs(container.get('Id'), stdout=True, stderr=False).split()
            return indices

    @wrap_exception('Unable to use Docker')
    def test(self):
        version_info = self._client.version()
        if map(int, version_info['ApiVersion'].split('.')) < map(
            int, self.MIN_API_VERSION.split('.')
        ):
            raise DockerException('Please upgrade your version of Docker')

    @wrap_exception('Unable to get disk usage info')
    def get_disk_usage(self):
        """
        Return the total amount of disk space used by Docker images, and the
        amount that can be reclaimed by removing unused Docker images, in bytes.

        Emulates computation of the RECLAIMABLE field in the output for
        `docker system df`.

        Original implementation:
        https://github.com/docker/docker/blob/ea61dac9e6d04879445f9c34729055ac1bb15050/cli/command/formatter/disk_usage.go#L197-L214
        """
        df_info = self._client.df()
        total = df_info['LayersSize']
        used = 0.0
        for image in df_info['Images']:
            if image['Containers'] > 0:
                if image['VirtualSize'] == -1 or image['SharedSize'] == -1:
                    continue
                used += image['VirtualSize'] - image['SharedSize']
        reclaimable = total - used
        return total, reclaimable

    def _inspect_image(self, image_name):
        """
        Get raw image info JSON.
        :param image_name: id, tag, or repo digest
        """
        logger.debug('Fetching Docker image metadata for %s', image_name)
        return self._client.inspect_image(image_name)

    @wrap_exception('Unable to ensure unique network')
    def ensure_unique_network(self, name, internal=True):
        """
        Ensures there's a unique docker network with the given name in the machine.
        If no network by the name exists, creates one and returns its Id.
        If one or more networks exist by the name, deletes all but the first one
        then returns the Id of the first one.
        Return (True, Id) if a new network is created, (False, Id) if existing network used
        """
        networks = self._client.networks(names=[name])
        if len(networks) == 0:
            network_id = self.create_network(name, internal)
            return True, network_id
        else:
            # First remove any duplicates that might exist
            for net in networks[1:]:
                self.remove_network(net.get('Id'))
            return False, networks[0].get('Id')

    @wrap_exception('Unable to create Docker network')
    def create_network(self, network_name, internal=True):
        logger.debug('Creating Docker network: %s', network_name)
        if not network_name:
            raise ValueError("empty docker network name")
        network = self._client.create_network(network_name, internal=internal)
        return network['Id']

    @wrap_exception('Unable to remove Docker network')
    def remove_network(self, network_id):
        logger.debug('Removing Docker network: %s', network_id)
        if not network_id:
            raise ValueError("empty docker network id")
        self._client.remove_network(network_id)

    @wrap_exception('Unable to fetch Docker container ip')
    def get_container_ip(self, network_name, container_id):
        logger.debug('Fetching Docker container ip for %s', container_id)
        container_info = self._client.inspect_container(container_id)
        try:
            return container_info["NetworkSettings"]["Networks"][network_name]["IPAddress"]
        except KeyError:  # if container ip cannot be found in provided network, return None
            return None

    @wrap_exception('Unable to fetch Docker image metadata')
    def get_image_repo_digest(self, request_docker_image):
        info = self._inspect_image(request_docker_image)
        return info['RepoDigests'][0]

    @wrap_exception('Unable to remove Docker image')
    def remove_image(self, repo_digest):
        # First get the image id, because removing by repo digest only untags
        # the digest, without deleting the image.
        # https://github.com/docker/docker/issues/24688
        image_id = self._inspect_image(repo_digest)['Id']
        self._client.remove_image(image_id)

    @wrap_exception('Unable to download Docker image')
    def download_image(self, docker_image, progress_callback):
        try:
            repo, tag = docker_image.split(':')
        except ValueError:
            logger.debug(
                'Missing tag/digest on request docker image "%s", defaulting to latest',
                docker_image,
            )
            repo, tag = docker_image, 'latest'

        logger.debug('Downloading Docker image %s:%s', repo, tag)
        output = self._client.pull(repo, tag=tag, stream=True, decode=True)
        for status_dict in output:
            if 'error' in status_dict:
                raise DockerException(status_dict['error'])
            try:
                status = status_dict['status']
            except KeyError:
                pass
            try:
                status += ' (%s / %s)' % (
                    size_str(status_dict['progressDetail']['current']),
                    size_str(status_dict['progressDetail']['total']),
                )
            except KeyError:
                pass
            should_resume = progress_callback(status)
            if not should_resume:
                raise DockerException('Download aborted by user')

    @wrap_exception('Unable to create Docker container')
    def create_bundle_container(
        self,
        bundle_path,
        uuid,
        dependencies,
        command,
        docker_image,
        network_name=None,
        cpuset=None,
        gpuset=None,
        memory_bytes=0,
        detach=True,
    ):
        if not command.endswith(';'):
            command = '{};'.format(command)
        docker_command = ['bash', '-c', '( %s ) >stdout 2>stderr' % command]
        docker_bundle_path = '/' + uuid
        volume_binds = DockerClient.get_bundle_container_volume_binds(
            bundle_path, docker_bundle_path, dependencies
        )
        volumes = list(volume_binds.keys())
        environment = {'HOME': docker_bundle_path}
        working_dir = docker_bundle_path
        # Unset entrypoint regardless of image
        entrypoint = ''
        if cpuset:
            cpuset = ','.join(cpuset)
        else:
            cpuset = ''
        if network_name:
            host_config = self._client.create_host_config(
                binds=volume_binds,
                network_mode=network_name,
                mem_limit=memory_bytes,
                cpuset_cpus=cpuset,
                runtime=self._docker_runtime,
            )
        else:
            host_config = self._client.create_host_config(
                binds=volume_binds,
                mem_limit=memory_bytes,
                cpuset_cpus=cpuset,
                runtime=self._docker_runtime,
            )

        # Get user/group that owns the bundle directory
        # Then we can ensure that any created files are owned by the user/group
        # that owns the bundle directory, not root.
        bundle_stat = os.stat(bundle_path)
        uid = bundle_stat.st_uid
        gid = bundle_stat.st_gid
        # TODO: Fix potential permissions issues arising from this setting
        # This can cause problems if users expect to run as a specific user
        user = '%s:%s' % (uid, gid)

        if self._docker_runtime == DockerClient.NVIDIA_RUNTIME:
            # nvidia-docker runtime uses this env variable to allocate GPUs
            environment['NVIDIA_VISIBLE_DEVICES'] = ','.join(gpuset) if gpuset else 'all'
        return self._client.create_container(
            docker_image,
            command=docker_command,
            environment=environment,
            working_dir=working_dir,
            entrypoint=entrypoint,
            host_config=host_config,
            volumes=volumes,
            user=user,
            detach=True,
            runtime=self._docker_runtime,
        ).get('Id')

    @wrap_exception('Unable to start Docker container')
    def start_bundle_container(
        self,
        bundle_path,
        uuid,
        dependencies,
        command,
        docker_image,
        network_name=None,
        cpuset=None,
        gpuset=None,
        memory_bytes=0,
    ):

        # Impose a minimum container request memory 4mb, same as docker's minimum allowed value
        # https://docs.docker.com/config/containers/resource_constraints/#limit-a-containers-access-to-memory
        # When using the REST api, it is allowed to set Memory to 0 but that means the container has unbounded
        # access to the host machine's memory, which we have decided to not allow
        if memory_bytes < parse_size('4m'):
            raise DockerException('Minimum memory must be 4m ({} bytes)'.format(parse_size('4m')))
        container_id = self.create_bundle_container(
            bundle_path,
            uuid,
            dependencies,
            command,
            docker_image,
            network_name=network_name,
            cpuset=cpuset,
            gpuset=gpuset,
            memory_bytes=memory_bytes,
        )

        # Start the container.
        logger.debug('Starting Docker container for UUID %s, container ID %s,', uuid, container_id)
        self._client.start(container_id)

        return container_id

    @staticmethod
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

    def get_container_stats(self, container_id):
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

    @wrap_exception('Unable to kill Docker container')
    def kill_container(self, container_id):
        logger.debug('Killing container with ID %s', container_id)
        self._client.kill(container_id)

    @wrap_exception('Unable to check Docker container status')
    def check_finished(self, container_id):
        container_info = self._client.inspect_container(container_id)
        if not container_info['State']['Running']:
            # If the logs are nonempty, then something might have gone
            # wrong with the commands run before the user command,
            # such as bash or cd.
            stderr = self._client.logs(container_id, stderr=True, stdout=False)
            # Strip non-ASCII chars since failure_message is not Unicode
            if len(stderr) > 0:
                failure_msg = stderr.decode('ascii', errors='ignore')
            else:
                failure_msg = None
            return (True, container_info['State']['ExitCode'], failure_msg)
        return (False, None, None)

    @wrap_exception('Unable to delete Docker container')
    def delete_container(self, container_id):
        logger.debug('Deleting container with ID %s', container_id)
        self._client.remove_container(container_id, force=True)
