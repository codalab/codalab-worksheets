class RunManagerBase(object):
    """
    Base class for classes which manages individual runs on a worker.
    Each worker has a single run manager which it uses to create, start, resume, and monitor runs.

    Different implementation of this class will execute the runs in different ways.
    For example, one run manager may submit to the local docker socket while another submits to a managed cloud compute
    service.
    """

    @property
    def cpus(self):
        """
        :return: The total available cpus for this RunManager.
        """
        raise NotImplementedError

    @property
    def memory_bytes(self):
        """
        :return: The total available memory, in bytes, for this RunManager.
        """
        raise NotImplementedError

    @property
    def gpus(self):
        """
        :return: The total available cpus for this RunManager.
        """
        raise NotImplementedError

    def create_run(self, bundle, bundle_path, resources):
        """
        Creates a new run which when started will execute the provided run bundle
        :param bundle: the run bundle to execute
        :param bundle_path: path on the filesystem where the bundles data is stored
        :param resources: the resources requested for this run bundle, e.g. cpu, memory, docker image
        :return: a new Run which will execute the provided run bundle
        """
        raise NotImplementedError

    def serialize(self, run):
        """
        Serialize the run in order to persist its state.
        This is used so that workers can pickup where they left off if they are killed.
        :param run: the run to serialize
        :return: a dict of the serialized data for the run
        """
        raise NotImplementedError

    def deserialize(self, run_data):
        """
        Deserialize run data into a run instance.
        It is expected that the data is from a call to RunManager.serialize(run)
        :param run_data: the serialized run data
        :return: a new run instance which was represented by the data
        """
        raise NotImplementedError

    def worker_did_start(self):
        """Provides hook which can be overwritten to do something when the worker starts"""
        pass

    def worker_will_stop(self):
        """Provides hook which can be overwritten to do something when the worker will be stopping"""
        pass


class RunBase(object):
    """
    Base class for classes which represent an executable run bundle.
    These are returned from a RunManager and common methods for manipulating the run.
    """

    @property
    def bundle(self):
        raise NotImplementedError

    @property
    def resources(self):
        raise NotImplementedError

    @property
    def dependency_paths(self):
        """
        :return: A list of filesystem paths to all dependencies.
        """
        return set([dep['child_path'] for dep in self.bundle['dependencies']])

    @property
    def bundle_path(self):
        """
        :return: The filesystem path to the bundle.
        """
        raise NotImplementedError

    @property
    def requested_memory_bytes(self):
        """
        If request_memory is defined, then return that.
        Otherwise, this run's memory usage does not get checked, so return inf.
        """
        return self.resources.get('request_memory') or float('inf')

    def start(self):
        """
        Start this run asynchronously.
        :return: True if the run was started, False otherwise.
        """
        raise NotImplementedError

    def resume(self):
        """
        Resume this run asynchronously.
        This is used primarily after a worker has deserialized a saved run and then wants to continue it.
        :return: True if the run could be resumed, False otherwise
        """
        raise NotImplementedError

    def kill(self, reason):
        """
        Kill this run if it is started.
        :param reason: The reason the run is being killed
        :return: True if the run was killed, False otherwise
        """
        raise NotImplementedError

    def read(self, path, read_args, socket):
        """
        Read the data at the path and send it back over the socket.
        More than likely this is done asynchronously.
        It is the responsibility of the implementor of this method to send pertinent error information.
        :param path: The path to the data to be read. Refers to a path from this runs bundle.
        :param read_args: A dict with parameters about how to read the data.
        :param socket: A SocketConnection to send the read data to.
        """
        raise NotImplementedError

    def write(self, subpath, data):
        """
        Write the data to the specified subpath of this runs bundle.
        :param subpath: Path to write the data at.
        :param data: The data to be written.
        :return: True if success, False otherwise.
        """
        raise NotImplementedError

    def pre_start(self):
        """
        Hook which can be overwritten to run some logic before the run starts
        """
        pass

    def post_stop(self):
        """
        Hook which can be overwritten to run some logic after the run stops
        """
        pass


