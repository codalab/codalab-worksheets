from abc import ABCMeta, abstractmethod, abstractproperty
import http.client


class BaseRunManager(object, metaclass=ABCMeta):
    @abstractmethod
    def start(self):
        """
        starts the RunManager, initializes from committed state, starts other
        dependent managers and initializes them as well.
        """
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        """
        Starts any necessary cleanup and propagates to its other managers
        Blocks until cleanup is complete and it is safe to quit
        """
        raise NotImplementedError

    @abstractmethod
    def save_state(self):
        """
        makes the RunManager and all other managers commit their state to
        disk (including state of all runs)
        """
        raise NotImplementedError

    @abstractmethod
    def process_runs(self):
        """
        Main event-loop call where the run manager should advance the state
        machine of all its runs
        """
        raise NotImplementedError

    @abstractmethod
    def create_run(self, bundle, resources):
        """
        Creates and starts processing a new run with the given bundle and
        resources
        """
        raise NotImplementedError

    @abstractmethod
    def get_run(self, uuid):
        """
        Returns the state of the run with the given UUID if it is managed
        by this RunManager, returns None otherwise
        """
        raise NotImplementedError

    @abstractmethod
    def mark_finalized(self, uuid):
        """
        Marks the run with the given uuid as finalized server-side so the
        run manager can discard it completely
        """
        raise NotImplementedError

    @abstractmethod
    def write(self, bundle_uuid, path, string):
        """
        Write string to path in bundle with uuid
        """
        raise NotImplementedError

    @abstractmethod
    def netcat(self, bundle_uuid, port, message):
        """
        Write message to port of bundle with uuid and read the response.
        Returns a stream with the response contents
        """
        raise NotImplementedError

    @abstractmethod
    def kill(self, bundle_uuid):
        """
        Kill bundle with uuid
        """
        raise NotImplementedError

    @abstractproperty
    def all_runs(self):
        """
        Returns a list of all the runs managed by this RunManager
        """
        raise NotImplementedError

    @abstractproperty
    def all_dependencies(self):
        """
        Returns a list of all dependencies available in this RunManager
        """
        raise NotImplementedError

    @abstractproperty
    def cpus(self):
        """
        Total number of CPUs this RunManager has
        """
        raise NotImplementedError

    @abstractproperty
    def gpus(self):
        """
        Total number of GPUs this RunManager has
        """
        raise NotImplementedError

    @abstractproperty
    def memory_bytes(self):
        """
        Total installed memory of this RunManager
        """
        raise NotImplementedError


class Reader(object):
    def __init__(self):
        self.read_handlers = {
            'get_target_info': self.get_target_info,
            'stream_directory': self.stream_directory,
            'stream_file': self.stream_file,
            'read_file_section': self.read_file_section,
            'summarize_file': self.summarize_file,
        }

    def read(self, run_state, path, dep_paths, read_args, reply):
        dep_paths = set([dep['child_path'] for dep in run_state.bundle['dependencies']])
        read_type = read_args['type']
        handler = self.read_handlers.get(read_type, None)
        if handler:
            handler(run_state, path, dep_paths, read_args, reply)
        else:
            err = (http.client.BAD_REQUEST, "Unsupported read_type for read: %s" % read_type)
            reply(err)

    @abstractmethod
    def get_target_info(self, run_state, path, dep_paths, args, reply_fn):
        """
        Calls reply_fn(err, msg, data) with the target_info of the path
        in the msg field, or with err if there is an error
        """
        raise NotImplementedError

    @abstractmethod
    def stream_directory(self, run_state, path, dep_paths, args, reply_fn):
        """
        Calls reply_fn(err, msg, data) with the dir contents of the path
        in the data field, or with err if there is an error
        """
        raise NotImplementedError

    @abstractmethod
    def stream_file(self, run_state, path, dep_paths, args, reply_fn):
        """
        Calls reply_fn(err, msg, data) with the file contents of the path
        in the data field, or with err if there is an error
        """
        raise NotImplementedError

    @abstractmethod
    def read_file_section(self, run_state, path, dep_paths, args, reply_fn):
        """
        Calls reply_fn(err, msg, data) with the file section contents of the path
        in the data field, or with err if there is an error
        """
        raise NotImplementedError

    @abstractmethod
    def summarize_file(self, run_state, path, dep_paths, args, reply_fn):
        """
        Calls reply_fn(err, msg, data) with the file summary of the path
        in the data field, or with err if there is an error
        """
        raise NotImplementedError
