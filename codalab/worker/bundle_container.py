import docker
from docker.models.containers import Container

DEFAULT_RUNTIME = 'runc'

class BundleContainer:

    def __init__(self):
        pass

    def check_finished(self):
        raise NotImplementedError

    def get_container_stats_with_stats_api(self):
        raise NotImplementedError

    def get_container_stats_native(self):
        raise NotImplementedError

    def container_exists(self):
        raise NotImplementedError

    def get_container_running_time(self):
        raise NotImplementedError

class BundleRunner:

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
        raise NotImplementedError