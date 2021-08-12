import docker
from docker.models.containers import Container

DEFAULT_RUNTIME = 'runc'

class BundleRunner:

    def __init__(self):
        pass


# todo some sort of container abstraction that is to be returned from this method
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
        raise NotImplementedError("there is no default implementation for run")

    def check_finished(self, container: Container):
        pass

    def get_container_stats_with_stats_api(self, container: Container):
        pass

    def get_container_stats_native(self, container: Container):
        pass

    def container_exists(self, container: Container):
        pass

    def get_container_running_time(self, container: Container):
        pass



