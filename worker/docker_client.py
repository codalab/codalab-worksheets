from contextlib import closing
import httplib
import json
import logging
import os
import socket
import ssl
import subprocess
import sys


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
    """
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
        
        # Find libcuda. We pick up the 64-bit version only and put it in a
        # semi-standard directory. It works for the Tensorflow Docker image.
        self._libcuda_files = []
        try:
            for lib in subprocess.check_output(['/sbin/ldconfig', '-p']).split('\n'):
                if 'libcuda.' not in lib or 'x86-64' not in lib:
                    continue
                self._libcuda_files.append(lib.split(' => ')[-1])
        except OSError:
            # ldconfig isn't available on Mac OS X. Let's just say that we
            # don't support libcuda on Mac.
            print >> sys.stderr, """
No ldconfig found. Not loading libcuda libraries.
"""

        # Find all the NVIDIA device files.
        self._nvidia_device_files = []
        for filename in os.listdir('/dev'):
            if filename.startswith('nvidia'):
                self._nvidia_device_files.append(os.path.join('/dev', filename))

    def _create_connection(self):
        if self._docker_host:
            if self._ssl_context:
                return httplib.HTTPSConnection(self._docker_host,
                                               context=self._ssl_context)
            return httplib.HTTPConnection(self._docker_host)
        return DockerUnixConnection()

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
                loop_callback()
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

    @wrap_exception('Unable to start Docker container')
    def start_container(self, bundle_path, uuid, command, docker_image,
                        request_network, dependencies):
        # Set up the command.
        docker_bundle_path = '/' + uuid
        docker_commands = [
            'BASHRC=$(pwd)/.bashrc',
            # Run as the user that owns the bundle directory. That way
            # any new files are created as that user and not root.
            'U_ID=$(stat -c %%u %s)' % docker_bundle_path,
            'G_ID=$(stat -c %%g %s)' % docker_bundle_path,
            'sudo -u \\#$U_ID -g \\#$G_ID -n bash -c ' +
            # We pass several commands for bash to execute as the user as a
            # single argument (i.e. all commands appear in quotes with no spaces
            # outside the quotes). The first commands appear in double quotes
            # since we want environment variables to be expanded. The last
            # appears in single quotes since we do not. The expansion there, if
            # any, should happen when bash executes it. Note, since the user's
            # command can have single quotes we need to escape them.
            '"[ -e $BASHRC ] && . $BASHRC; "' +
            '"cd %s; "' % docker_bundle_path +
            '"export HOME=%s; "' % docker_bundle_path +
            '\'(%s) >stdout 2>stderr\'' % command.replace('\'', '\'"\'"\''),
        ]

        # Set up the volumes.
        volume_bindings = []
        for libcuda_file in self._libcuda_files:
            volume_bindings.append('%s:/usr/lib/x86_64-linux-gnu/%s:ro' % (
                libcuda_file, os.path.basename(libcuda_file)))
        volume_bindings.append('%s:%s' % (bundle_path, docker_bundle_path))
        for dependency_path, child_path in dependencies:
            volume_bindings.append('%s:%s:ro' % (
                dependency_path,
                os.path.join(docker_bundle_path, child_path)))

        # Set up the devices.
        devices = []
        for device in self._nvidia_device_files:
            devices.append({
                'PathOnHost': device,
                'PathInContainer': device,
                'CgroupPermissions': 'mrw'})

        # Create the container.
        logger.debug('Creating Docker container with command %s', command)
        create_request = {
            'Cmd': ['bash', '-c', '; '.join(docker_commands)],
            'Image': docker_image,
            'NetworkDisabled': not request_network,
            'HostConfig': {
                'Binds': volume_bindings,
                'Devices': devices,
                'ReadonlyRootfs': True,
                },
        }
        with closing(self._create_connection()) as create_conn:
            create_conn.request('POST', '/containers/create',
                                json.dumps(create_request),
                                {'Content-Type': 'application/json'})
            create_response = create_conn.getresponse()
            if create_response.status != 201:
                raise DockerException(create_response.read())
            container_id = json.loads(create_response.read())['Id']

        # Start the container.
        logger.debug('Starting Docker container with command %s, container ID %s',
            command, container_id)
        with closing(self._create_connection()) as start_conn:
            start_conn.request('POST', '/containers/%s/start' % container_id)
            start_response = start_conn.getresponse()
            if start_response.status != 204:
                raise DockerException(start_response.read())

        return container_id

    def get_container_stats(self, container_id):
        logger.debug('Getting statistics for container ID %s', container_id)
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
                return (True, inspect_json['State']['ExitCode'], None)
            return (False, None, None)   

    @wrap_exception('Unable to delete Docker container')
    def delete_container(self, container_id):
        logger.debug('Deleting container with ID %s', container_id)
        with closing(self._create_connection()) as conn:
            conn.request('DELETE', '/containers/%s?force=1' % container_id)
            delete_response = conn.getresponse()
            if delete_response.status == 500:
                raise DockerException(delete_response.read())


class DockerUnixConnection(httplib.HTTPConnection, object):
    """
    Connection to the Docker Unix socket.
    """

    def __init__(self):
        httplib.HTTPConnection.__init__(self, 'localhost')
 
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(60)
        self.sock.connect('//var/run/docker.sock')
