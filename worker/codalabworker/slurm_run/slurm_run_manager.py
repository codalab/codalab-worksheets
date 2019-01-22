from codalabworker.run_manager import BaseRunManager


class SlurmRunManager(BaseRunManager):

    def __init__(self):
        pass

    def start(self):
        # TODO: Start whatever needs starting
        pass

    def stop(self):
        # TODO: Stop all
        pass

    def save_state(self):
        # TODO: Save to disk any state that needs saving
        pass

    def process_runs(self):
        # TODO: Work on your runs
        pass

    def create_run(self, bundle, resources):
        # TODO: Create a new run and start processing it
        pass

    def get_run(self, uuid):
        # TODO: Return state of run with uuid if you have it else None
        pass

    def mark_finalized(self, uuid):
        # TODO: Mark UUID as finalized so you can be done with it
        pass

    def write(self, bundle_uuid, path, string):
        """
        Write string to path in bundle with uuid
        """
        pass

    def netcat(self, bundle_uuid, port, message):
        """
        Write message to port of bundle with uuid and read the response.
        Returns a stream with the response contents
        """
        pass

    def kill(self, bundle_uuid):
        """
        Kill bundle with uuid
        """
        pass

    def all_runs(self):
        """
        Returns a list of all the runs managed by this RunManager
        """
        pass

    def all_dependencies(self):
        """
        Returns a list of all dependencies available in this RunManager
        """
        pass

    def cpus(self):
        """
        Total number of CPUs this RunManager has
        """
        pass

    def gpus(self):
        """
        Total number of GPUs this RunManager has
        """
        pass

    def memory_bytes(self):
        """
        Total installed memory of this RunManager
        """
        pass
