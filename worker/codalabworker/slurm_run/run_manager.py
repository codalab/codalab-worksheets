import logging
from codalabworker.run_manager import BaseRunManager

logger = logging.getLogger(__name__)


class SlurmRunManager(BaseRunManager):

    def __init__(self):
        pass

    def start(self):
        """
        starts the RunManager, initializes from committed state, starts other
        dependent managers and initializes them as well.
        """
        raise NotImplementedError

    def stop(self):
        """
        Starts any necessary cleanup and propagates to its other managers
        Blocks until cleanup is complete and it is safe to quit
        """
        raise NotImplementedError

    def save_state(self):
        """
        makes the RunManager and all other managers commit their state to
        disk (including state of all runs)
        """
        raise NotImplementedError

    def process_runs(self):
        """
        Main event-loop call where the run manager should advance the state
        machine of all its runs
        """
        raise NotImplementedError

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

    def mark_finalized(self, uuid):
        """
        Marks the run with the given uuid as finalized server-side so the
        run manager can discard it completely
        """
        raise NotImplementedError

    def write(self, bundle_uuid, path, string):
        """
        Write string to path in bundle with uuid
        """
        raise NotImplementedError

    def netcat(self, bundle_uuid, port, message):
        """
        Write message to port of bundle with uuid and read the response.
        Returns a stream with the response contents
        """
        raise NotImplementedError

    def kill(self, bundle_uuid):
        """
        Kill bundle with uuid
        """
        raise NotImplementedError

    def all_runs(self):
        """
        Returns a list of all the runs managed by this RunManager
        """
        raise NotImplementedError

    def all_dependencies(self):
        """
        Returns a list of all dependencies available in this RunManager
        """
        raise NotImplementedError

    def cpus(self):
        """
        Total number of CPUs this RunManager has
        """
        raise NotImplementedError

    def gpus(self):
        """
        Total number of GPUs this RunManager has
        """
        raise NotImplementedError

    def memory_bytes(self):
        """
        Total installed memory of this RunManager
        """
        raise NotImplementedError
