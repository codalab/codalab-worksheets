import logging
import os
import datetime

from codalab.worker.bundle_container import DEFAULT_RUNTIME, BundleContainer, BundleRunner
from codalab.worker.docker_utils import NVIDIA_RUNTIME, DEFAULT_CONTAINER_RUNNING_TIME
from spython.main import Client

logger = logging.getLogger(__name__)

class SingularityContainer(BundleContainer):
    # todo aditya implement kill, remove
    def __init__(self, instance_name: str, image_spec: str, output_executor, path: str):
        self.instance_name = instance_name
        self.image_spec = image_spec
        self.output_executor = output_executor
        self.start_time = None
        self.end_time = None
        self.path = path

    def check_finished(self):
        instances = Client.instances(name=self.instance_name)
        # with singularity, if the instance exists, it is not finished
        if len(instances) > 0:
            return False, None, None
        # if the run finished, we can't get the exit code for now, singularity does not support when you stream
        # potential future solutions:
        #  - open PR for singularity supporting this (shouldn't be too much work)
        #  - have the exec run in a subprocess that tracks the exit code separately and does not use stream=True
        # todo get stderr -- https://singularityhub.github.io/singularity-cli/commands-instances#logs
        #  for now we just return raw logs
        return True, 0, None

    def get_container_stats_with_stats_api(self):
        return 0.0, 0

    def get_container_stats_native(self):
        return 0

    def container_exists(self):
        if len(Client.instances(name=self.instance_name)) > 0:
            return True
        return False

    def get_container_running_time(self):
        return DEFAULT_CONTAINER_RUNNING_TIME

    @property
    def id(self):
        return self.instance_name

    def start(self) -> None:
        self.start_time = datetime.datetime.now()

    def end(self) -> None:
        self.end_time = datetime.datetime.now()

    def elapsed(self) -> float:
        if self.start_time is None:
            return 0.0
        if self.end_time is None:
            self.end_time = datetime.datetime.now()
        return (self.end_time - self.start_time).total_seconds()

    def kill(self, signal=None) -> None:
        instance = Client.instances(name=self.instance_name).pop(0)
        os.kill(instance.pid, signal)

    # we should maybe return status from this
    def remove(self):
        os.remove(os.path.join(self.path, self.image_spec))

class SingularityBundleRunner(BundleRunner):

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
            runtime=DEFAULT_RUNTIME,
            shared_memory_size_gb=1):
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

        instance = Client.instance("codalab_singularity_images/" + image_spec + ".sif")
        logger.error("adi: spec {}".format(instance))
        output_executor = Client.execute(instance, singularity_command, bind=volumes, stream=True)
        logger.debug('Started singularity container for UUID %s, container ID %s,', uuid, instance)
        return SingularityContainer(instance, image_spec, output_executor, path)

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