from run_manager import BaseRunManager

class LocalRunManager(BaseRunManager):
    """
    LocalRunManager executes the runs locally, each one in its own Docker
    container. It manages its cache of local Docker images and its own local
    Docker network.
    """
    def __init__(self, worker, docker_manager, bundle_service, dependency_manager, state_committer, cpuset, gpuset):
        self._worker = worker
        self._docker_manager = docker_manager
        self._dependency_manager = dependency_manager
        self._state_committer = state_committer
        self._cpuset = cpuset
        self._gpuset = gpuset

    def start(self):
        """
        starts the RunManager, initializes from committed state, starts other
        dependent managers and initializes them as well.
        """
        self._docker_manager.start()
        self._dependency_manager.start()
        self._state_committer.load()

    def save_state(self):
        """
        makes the RunManager and all other managers commit their state to
        disk (including state of all runs)

        """
        self._state_committer.commit()# state)

    def stop(self):
        """
        Starts any necessary cleanup and propagates to its other managers
        Blocks until cleanup is complete and it is safe to quit
        """
        self._docker_manager.stop()
        self._dependency_manager.stop()

    def create_run(self, bundle, resources):
        """
        Creates and starts processing a new run with the given bundle and
        resources
        """
        raise NotImplementedError

    def get_run(self, uuid):
        """
        Returns the state of the run with the given UUID if it is managed
        by this RunManager, returns None otherwise
        """
        raise NotImplementedError

    def list_all_runs(self):
        """
        Returns a list of all the runs managed by this RunManager
        """
        raise NotImplementedError

    def list_all_dependencies(self):
        """
        Returns a list of all dependencies available in this RunManager
        """
        raise NotImplementedError

    def read(self, uuid, path, args):
        """
        Read contents of bundle with uuid at path with args.
        Returns a stream with the contents read
        """
        raise NotImplementedError

    def write(self, uuid, path, string):
        """
        Write string to path in bundle with uuid
        """
        raise NotImplementedError

    def netcat(self, uuid, port, message):
        """
        Write message to port of bundle with uuid and read the response.
        Returns a stream with the response contents
        """
        raise NotImplementedError

    def kill(self, uuid):
        """
        Kill bundle with uuid
        """
        raise NotImplementedError

    @property
    def cpus(self):
        """
        Total number of CPUs this RunManager has
        """
        raise NotImplementedError

    @property
    def gpus(self):
        """
        Total number of GPUs this RunManager has
        """
        raise NotImplementedError

    @property
    def memory_bytes(self):
        """
        Total installed memory of this RunManager
        """
        raise NotImplementedError
