import logging
import os
import traceback

import docker

from codalab.worker.bundle_runner import BundleRunner
from codalab.worker.docker_utils import NVIDIA_RUNTIME, DEFAULT_DOCKER_TIMEOUT, DEFAULT_RUNTIME

logger = logging.getLogger(__name__)

class DockerBundleRunner(BundleRunner):

    def __init__(self):
        self.client = docker.from_env(timeout=DEFAULT_DOCKER_TIMEOUT)

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
            runtime=DEFAULT_RUNTIME):
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
            container = self.client.containers.run(
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
            logger.debug('Started Docker container for UUID %s, container ID %s,', uuid, container.id)
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
        return container

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