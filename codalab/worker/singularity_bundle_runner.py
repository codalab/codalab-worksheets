import os

from codalab.worker.bundle_runner import BundleRunner, DEFAULT_RUNTIME
from codalab.worker.docker_utils import NVIDIA_RUNTIME
from spython.main import Client


class SingularityBundleRunner(BundleRunner):

    def __init__(self):
        self.f = 1

    def run(self,
            path,
            uuid,
            dependencies,
            command,
            image_spec,
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
        singularity_command = ['/bin/bash', '-c', '( %s ) >stdout 2>stderr' % command]
        singularity_bundle_path = '/' + uuid
        volumes = self.get_bundle_container_volume_binds(path, singularity_bundle_path, dependencies)
        environment = {'HOME': singularity_bundle_path, 'CODALAB': 'true'}
        working_dir = singularity_bundle_path
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

        output = Client.execute(image_spec, singularity_command, bind=volumes)


    def get_bundle_container_volume_binds(self, bundle_path, singularity_bundle_path, dependencies):
        """
            Returns a volume bindings dict for the bundle path and dependencies given
            """
        binds = [
            "{}:{}".format(dep_path, docker_dep_path)
            for dep_path, docker_dep_path in dependencies
        ]
        binds.append("{}:{}".format(bundle_path, singularity_bundle_path))
        return binds

    class SingularityContainer:
        