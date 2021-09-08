import docker
from docker.models.containers import Container

DEFAULT_RUNTIME = 'runc'

class BundleContainer:

    def __init__(self):
        pass

    def check_finished(self):
        """
        check_finished will check if a container has finished running.
        """
        raise NotImplementedError

    def get_container_stats_with_stats_api(self):
        """
        get_container_stats_with_stats_api will get a container's run statistics
        from the container statistics API, if there is one.
        """
        raise NotImplementedError

    def get_container_stats_native(self):
        """
        get_container_stats_native gets the statistics of a container's run through means other
        than the container API. An instance of this is cgroups.
        """
        raise NotImplementedError

    def container_exists(self):
        """
        container_exists checks whether a given container exists in the interface's container store.
        """
        raise NotImplementedError

    def get_container_running_time(self):
        """
        get_container_running_time gets the time it took for a container to run.
        """
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
            runtime=DEFAULT_RUNTIME,
            shared_memory_size_gb=1):
        """
        run will run a bundle given these run parameters.
        """
        raise NotImplementedError