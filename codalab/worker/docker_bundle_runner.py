import datetime
import logging
import os
import traceback
import docker
from docker.models.containers import Container
from dateutil import parser, tz

from codalab.worker.bundle_container import BundleContainer, BundleRunner
from codalab.worker.docker_utils import NVIDIA_RUNTIME, DEFAULT_DOCKER_TIMEOUT, DEFAULT_RUNTIME, wrap_exception, \
    DEFAULT_CONTAINER_RUNNING_TIME, get_cpu_usage, get_memory_usage

logger = logging.getLogger(__name__)
client = docker.from_env(timeout=DEFAULT_DOCKER_TIMEOUT)


class DockerBundleContainer(BundleContainer):

    def __init__(self, container: Container):
        self.docker_container = container

    def check_finished(self):
        # Unfortunately docker SDK doesn't update the status of Container objects
        # so we re-fetch them from the API again to get the most recent state
        if self.docker_container is None:
            return True, None, 'Docker container not found'
        container = self.client.containers.get(self.docker_container.id)
        if self.docker_container.status != 'running':
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
            return True, exitcode, failure_msg
        return False, None, None

    def get_container_stats_with_stats_api(self):
        """Returns the cpu usage and memory limit of a container using the Docker Stats API."""
        if self.container_exists():
            try:
                container_stats: dict = client.containers.get(self.docker_container.name).stats(stream=False)

                cpu_usage: float = get_cpu_usage(container_stats)
                memory_usage: float = get_memory_usage(container_stats)

                return cpu_usage, memory_usage
            except docker.errors.NotFound:
                raise
        else:
            return 0.0, 0

    def get_container_stats_native(self):
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
            cpu_path = os.path.join(cgroup, 'cpuacct/docker', self.docker_container.id, 'cpuacct.stat')
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
            memory_path = os.path.join(cgroup, 'memory/docker', self.docker_container.id, 'memory.usage_in_bytes')
            with open(memory_path) as f:
                stats['memory'] = int(f.read())
        except Exception:
            pass

        return stats

    def container_exists(self):
        try:
            client.containers.get(self.docker_container.id)
            return True
        except docker.errors.NotFound:
            return False

    def get_container_running_time(self):
        # This usually happens when container gets accidentally removed or deleted
        if self.docker_container is None:
            return DEFAULT_CONTAINER_RUNNING_TIME
        # Get the current container
        container = self.client.containers.get(self.docker_container.id)
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

class DockerBundleRunner(BundleRunner):

    def run(self,
            path,
            uuid,
            dependencies,
            command,
            image,
            network=None,
            cpuset=None,
            gpuset=None,
            memory_bytes=0,
            detach=True,
            tty=False,
            runtime=DEFAULT_RUNTIME,
            shared_memory_size_gb=1,
            ) -> DockerBundleContainer:
        if not command.endswith(';'):
            command = '{};'.format(command)
        # Explicitly specifying "/bin/bash" instead of "bash" for bash shell to avoid the situation when
        # the program can't find the symbolic link (default is "/bin/bash") of bash in the environment
        # logger.info("adiprerepa: dependencies: {}".format(dependencies))
        # logger.info("adiprerepa cpuset {} gpuset {}".format(cpuset, gpuset))
        # logger.info("adiprerepa: bundle path {}".format(bundle_path))
        docker_command = ['/bin/bash', '-c', '( %s ) >stdout 2>stderr' % command]
        docker_bundle_path = '/' + uuid
        volumes = self.get_bundle_container_volume_binds(path, docker_bundle_path, dependencies)
        environment = {'HOME': docker_bundle_path, 'CODALAB': 'true'}
        working_dir = docker_bundle_path
        # Unset entrypoint regardless of image
        entrypoint = ''
        cpuset_str = ','.join(cpuset) if cpuset else ''
        # Get user/group that owns the bundle directory
        # Then we can ensure that any created files are owned by the user/group
        # that owns the bundle directory, not root.
        bundle_stat = os.stat(path)
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
            container = client.containers.run(
                image=image,
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
            logger.info(container)
            logger.debug('Started Docker container for UUID %s, container ID %s,', uuid, container.id)
        except docker.errors.APIError:
            # The container failed to start, so it's in the CREATED state
            # If we try to re-run the container again, we'll get a 409 CONFLICT
            # because a container with the same name already exists. So, we try to remove
            # the container here.
            try:
                client.api.remove_container(container_name, force=True)
            except Exception:
                logger.warning("Failed to clean up Docker container after failed launch.")
                traceback.print_exc()
            raise
        return DockerBundleContainer(container)

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