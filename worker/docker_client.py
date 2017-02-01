from contextlib import closing
import httplib
import json
import logging
import os
import re
import socket
import ssl
import struct
import subprocess
import sys

from formatting import size_str

logger = logging.getLogger(__name__)


def wrap_exception(message):
    def decorator(f):
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except DockerException as e:
                raise DockerException, \
                    DockerException(message + ': ' + e.message), \
                    sys.exc_info()[2]
            except (httplib.HTTPException, socket.error) as e:
                raise DockerException, \
                    DockerException(message + ': ' + str(e)), \
                    sys.exc_info()[2]
        return wrapper
    return decorator


class DockerException(Exception):
    def __init__(self, message):
        super(DockerException, self).__init__(message)


class DockerClient(object):
    """
    Methods for talking to Docker.

    GPU Support
    -----------
    DockerClient relies on nvidia-docker-plugin for runs that require GPUs.
    During initialization, DockerClient checks to see if nvidia-docker-plugin
    is available on the host machine by contacting the REST API at the default
    address `localhost:3476`. If the plugin is available, then DockerClient will
    query the REST API for information about the volumes and devices that it
    should specify in the Docker container creation request.

    If you want your worker to support GPU jobs, make sure to install
    nvidia-docker on the host machine. You can find the project and installation
    instructions on GitHub:
    https://github.com/NVIDIA/nvidia-docker

    DockerClient will read the CUDA_VISIBLE_DEVICES environment variable and
    only mount the GPU device corresponding to the indices listed in the
    variable. The order in which the devices are listed is ignored.
    """
    # Where to look for nvidia-docker-plugin
    # https://github.com/NVIDIA/nvidia-docker/wiki/nvidia-docker-plugin
    NV_HOST = 'localhost:3476'

    def __init__(self):
        self._docker_host = os.environ.get('DOCKER_HOST') or None
        if self._docker_host:
            self._docker_host = self._docker_host.replace('tcp://', '')

        cert_path = os.environ.get('DOCKER_CERT_PATH') or None
        if cert_path:
            self._ssl_context = ssl.create_default_context(
                cafile=os.path.join(cert_path, 'ca.pem'))
            self._ssl_context.load_cert_chain(
                os.path.join(cert_path, 'cert.pem'),
                os.path.join(cert_path, 'key.pem'))
            self._ssl_context.check_hostname = False

        # Test to make sure that a connection can be established.
        try:
            self.test()
        except DockerException:
            print >> sys.stderr, """
On Linux, a valid Docker installation should create a Unix socket at
/var/run/docker.sock.

On Mac, DOCKER_HOST and optionally DOCKER_CERT_PATH should be defined. You need
to run the worker from the Docker shell.
"""
            raise

        # Read CUDA_VISIBLE_DEVICES
        if 'CUDA_VISIBLE_DEVICES' in os.environ:
            self._cuda_visible_devices = os.environ['CUDA_VISIBLE_DEVICES'].split(',')
        else:
            self._cuda_visible_devices = None

        # Check if nvidia-docker-plugin is available
        try:
            self._test_nvidia_docker()
        except DockerException:
            print >> sys.stderr, """
nvidia-docker-plugin not available, no GPU support on this worker.
"""
            self._use_nvidia_docker = False
        else:
            self._use_nvidia_docker = True

    def _create_nvidia_docker_connection(self):
        return httplib.HTTPConnection(self.NV_HOST)

    def _create_connection(self):
        if self._docker_host:
            if self._ssl_context:
                return httplib.HTTPSConnection(self._docker_host,
                                               context=self._ssl_context)
            return httplib.HTTPConnection(self._docker_host)
        return DockerUnixConnection()

    def _test_nvidia_docker(self):
        """Throw exception if nvidia-docker-plugin is not available."""
        try:
            # Test the API call directly
            # Will catch any errors (such as CUDA_VISIBLE_DEVICES format)
            # ahead of time.
            self._add_nvidia_docker_arguments({})
        except Exception as e:
            raise DockerException(e.message)

    def _add_nvidia_docker_arguments(self, request):
        """Add the arguments supplied by nvidia-docker-plugin REST API"""
        # nvidia-docker-plugin REST API documentation:
        # https://github.com/NVIDIA/nvidia-docker/wiki/nvidia-docker-plugin#rest-api
        with closing(self._create_nvidia_docker_connection()) as conn:
            path = '/v1.0/docker/cli/json?dev='
            if self._cuda_visible_devices is not None:
                path += '+'.join(self._cuda_visible_devices)
            conn.request('GET', path)
            cli_response = conn.getresponse()
            if cli_response.status != 200:
                raise DockerException(cli_response.read())
            cli_args = json.loads(cli_response.read())

        # Build device jsons
        devices = [{
            "PathOnHost": device_path,
            "PathInContainer": device_path,
            "CgroupPermissions": "mrw",
        } for device_path in cli_args['Devices']]

        # Set configurations in request json
        host_config = request.setdefault('HostConfig', {})
        host_config.setdefault('Binds', []).extend(cli_args['Volumes'])
        host_config.setdefault('Devices', []).extend(devices)
        host_config['VolumeDriver'] = cli_args['VolumeDriver']

        return request

    @wrap_exception('Unable to use Docker')
    def test(self):
        with closing(self._create_connection()) as conn:
            conn.request('GET', '/version')
            version_response = conn.getresponse()
            if version_response.status != 200:
                raise DockerException(version_response.read())
            try:
                version_info = json.loads(version_response.read())
            except:
                raise DockerException('Invalid version information')
            if version_info['ApiVersion'] < '1.17':
                raise DockerException('Please upgrade your version of Docker')

    @wrap_exception('Unable to fetch Docker image metadata')
    def get_image_repo_digest(self, request_docker_image):
        logger.debug('Fetching Docker image metadata for %s', request_docker_image)
        with closing(self._create_connection()) as conn:
            conn.request('GET', '/images/%s/json' % request_docker_image)
            create_image_response = conn.getresponse()
            if create_image_response.status != 200:
                raise DockerException(create_image_response.read())
            contents = json.loads(create_image_response.read())
        return contents.get('RepoDigests', [''])[0]

    @wrap_exception('Unable to download Docker image')
    def download_image(self, docker_image, loop_callback):
        logger.debug('Downloading Docker image %s', docker_image)
        with closing(self._create_connection()) as conn:
            conn.request('POST',
                         '/images/create?fromImage=%s' % docker_image)
            create_image_response = conn.getresponse()
            if create_image_response.status != 200:
                raise DockerException(create_image_response.read())

            # Wait for the download to finish. Docker sends a stream of JSON
            # objects. Since we don't know how long each one is we read a
            # character at a time until what we have so far parses as a valid
            # JSON object.
            while True:
                response = None
                line = ''
                while True:
                    ch = create_image_response.read(1)
                    if not ch:
                        break
                    line += ch
                    try:
                        response = json.loads(line)
                        logger.debug(line.strip())
                        break
                    except ValueError:
                        pass
                if not response:
                    break
                if 'error' in response:
                    raise DockerException(response['error'])

                status = ''
                try:
                    status = response['status']
                except KeyError:
                    pass
                try:
                    status += ' (%s / %s)' % (
                        size_str(response['progressDetail']['current']),
                        size_str(response['progressDetail']['total']))
                except KeyError:
                    pass
                loop_callback(status)

    @wrap_exception('Unable to start Docker container')
    def start_container(self, bundle_path, uuid, command, docker_image,
                        request_network, dependencies):
        # Set up the command.
        docker_bundle_path = '/' + uuid
        docker_commands = [
            # TODO: Remove when codalab/torch uses ENV directives
            # Temporary hack for codalab/torch image to load env variables
            '[ -e /user/.bashrc ] && . /user/.bashrc',
            '(%s) >stdout 2>stderr' % command,
        ]

        # Set up the volumes.
        volume_bindings = ['%s:%s' % (bundle_path, docker_bundle_path)]
        for dependency_path, docker_dependency_path in dependencies:
            volume_bindings.append('%s:%s:ro' % (
                os.path.abspath(dependency_path),
                docker_dependency_path))

        # Get user/group that owns the bundle directory
        # Then we can ensure that any created files are owned by the user/group
        # that owns the bundle directory, not root.
        bundle_stat = os.stat(bundle_path)
        uid = bundle_stat.st_uid
        gid = bundle_stat.st_gid

        # Create the container.
        create_request = {
            'Cmd': ['bash', '-c', '; '.join(docker_commands)],
            'Image': docker_image,
            'WorkingDir': docker_bundle_path,
            'Env': ['HOME=%s' % docker_bundle_path],
            'HostConfig': {
                'Binds': volume_bindings,
                },
            # TODO: Fix potential permissions issues arising from this setting
            # This can cause problems if users expect to run as a specific user
            'User': '%s:%s' % (uid, gid),
        }
        if self._use_nvidia_docker:
            self._add_nvidia_docker_arguments(create_request)
        if not request_network:
            create_request['HostConfig']['NetworkMode'] = 'none'

        with closing(self._create_connection()) as create_conn:
            create_conn.request('POST', '/containers/create',
                                json.dumps(create_request),
                                {'Content-Type': 'application/json'})
            create_response = create_conn.getresponse()
            if create_response.status != 201:
                raise DockerException(create_response.read())
            container_id = json.loads(create_response.read())['Id']

        # Start the container.
        logger.debug('Starting Docker container for UUID %s with command %s, container ID %s',
            uuid, command, container_id)
        with closing(self._create_connection()) as start_conn:
            start_conn.request('POST', '/containers/%s/start' % container_id)
            start_response = start_conn.getresponse()
            if start_response.status != 204:
                raise DockerException(start_response.read())

        return container_id

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
        except:
            pass

        # Get memory usage
        try:
            memory_path = os.path.join(cgroup, 'memory/docker', container_id, 'memory.usage_in_bytes')
            with open(memory_path) as f:
                stats['memory'] = int(f.read())
        except:
            pass

        return stats

    @wrap_exception('Unable to kill Docker container')
    def kill_container(self, container_id):
        logger.debug('Killing container with ID %s', container_id)
        with closing(self._create_connection()) as conn:
            conn.request('POST', '/containers/%s/kill' % container_id)
            kill_response = conn.getresponse()
            if kill_response.status == 500:
                raise DockerException(kill_response.read())

    @wrap_exception('Unable to check Docker container status')
    def check_finished(self, container_id):
        with closing(self._create_connection()) as conn:
            conn.request('GET', '/containers/%s/json' % container_id)
            inspect_response = conn.getresponse()
            if inspect_response.status == 404:
                return (True, None, 'Lost by Docker')
            if inspect_response.status != 200:
                raise DockerException(inspect_response.read())

            inspect_json = json.loads(inspect_response.read())
            if not inspect_json['State']['Running']:
                # If the logs are nonempty, then something might have gone
                # wrong with the commands run before the user command,
                # such as bash or cd.
                _, stderr = self._get_logs(container_id)
                # Strip non-ASCII chars since failure_message is not Unicode
                if len(stderr) > 0:
                    failure_msg = stderr.encode('ascii', 'ignore')
                else:
                    failure_msg = None
                return (True, inspect_json['State']['ExitCode'], failure_msg)
            return (False, None, None)

    @wrap_exception('Unable to delete Docker container')
    def delete_container(self, container_id):
        logger.debug('Deleting container with ID %s', container_id)
        with closing(self._create_connection()) as conn:
            conn.request('DELETE', '/containers/%s?force=1' % container_id)
            delete_response = conn.getresponse()
            if delete_response.status == 500:
                raise DockerException(delete_response.read())

    def _get_logs(self, container_id):
        """
        Read stdout and stderr of the given container.

        The stream is encoded in a format defined here:
        https://docs.docker.com/engine/api/v1.20/#/attach-to-a-container
        """
        with closing(self._create_connection()) as conn:
            conn.request('GET', '/containers/%s/logs?stdout=1&stderr=1' % container_id)
            logs_response = conn.getresponse()
            if logs_response.status == 500:
                raise DockerException(logs_response.read())
            contents = logs_response.read()
            stderr = []
            stdout = []
            start = 0
            while start < len(contents):
                fp, size = struct.unpack('>BxxxL', contents[start:start+8])
                payload = contents[start+8:start+8+size]
                if fp == 1:
                    stdout.append(payload)
                elif fp == 2:
                    stderr.append(payload)
                start += 8 + size
            return ''.join(stdout), ''.join(stderr)


class DockerUnixConnection(httplib.HTTPConnection, object):
    """
    Connection to the Docker Unix socket.
    """

    def __init__(self):
        httplib.HTTPConnection.__init__(self, 'localhost')

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(300)
        self.sock.connect('//var/run/docker.sock')
