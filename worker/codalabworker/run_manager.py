import httplib

class BaseRunManager(object):
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

    def write(self, run_state, path, string):
        """
        Write string to path in bundle with uuid
        """
        raise NotImplementedError

    def netcat(self, run_state, port, message):
        """
        Write message to port of bundle with uuid and read the response.
        Returns a stream with the response contents
        """
        raise NotImplementedError

    def kill(self, run_state):
        """
        Kill bundle with uuid
        """
        raise NotImplementedError

    @property
    def all_runs(self):
        """
        Returns a list of all the runs managed by this RunManager
        """
        raise NotImplementedError

    @property
    def all_dependencies(self):
        """
        Returns a list of all dependencies available in this RunManager
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

class Reader(object):
    def __init__(self):
        pass

    def read(self, run_state, path, dep_paths, read_args, reply):
        bundle_uuid = run_state.bundle['uuid']
        dep_paths = set([dep['child_path'] for dep in run_state.bundle['dependencies']])
        read_type = read_args['type']
        if read_type == 'get_target_info':
            self.get_target_info(run_state, path, dep_paths, read_args, reply)
        elif read_type == 'stream_directory':
            self.stream_directory(run_state, path, dep_paths, read_args, reply)
        elif read_type == 'stream_file':
            self.stream_directory(run_state, path, dep_paths, read_args, reply)
        elif read_type == 'read_file_section':
            self.read_file_section(run_state, path, dep_paths, read_args, reply)
        elif read_type == 'summarize_file':
            self.summarize_file(run_state, path, dep_paths, read_args, reply)
        else:
            err = (httplib.BAD_REQUEST, "Unsupported read_type for read: %s" % read_type)
            reply(err)

    def get_target_info(self, run_state, path, dep_paths, args, reply_fn):
        """
        Calls reply_fn(err, msg, data) with the target_info of the path
        in the msg field, or with err if there is an error
        """
        raise NotImplementedError

    def stream_directory(self, run_state, path, dep_paths, args, reply_fn):
        """
        Calls reply_fn(err, msg, data) with the dir contents of the path
        in the data field, or with err if there is an error
        """
        raise NotImplementedError

    def stream_file(self, run_state, path, dep_paths, args, reply_fn):
        """
        Calls reply_fn(err, msg, data) with the file contents of the path
        in the data field, or with err if there is an error
        """
        raise NotImplementedError

    def read_file_section(self, run_state, path, dep_paths, args, reply_fn):
        """
        Calls reply_fn(err, msg, data) with the file section contents of the path
        in the data field, or with err if there is an error
        """
        raise NotImplementedError

    def summarize_file(self, run_state, path, dep_paths, args, reply_fn):
        """
        Calls reply_fn(err, msg, data) with the file summary of the path
        in the data field, or with err if there is an error
        """
        raise NotImplementedError

class RunState(object):
    def __init__(self, stage, run_status, bundle, bundle_path, resources, start_time,
            container_id, docker_image, is_killed, cpuset, gpuset, info):
        self.stage = stage
        self.run_status = run_status
        self.bundle = bundle
        self.bundle_path = bundle_path
        self.resources = resources
        self.start_time = start_time
        self.container_id = container_id
        self.docker_image = docker_image
        self.is_killed = is_killed
        self.cpuset = cpuset
        self.gpuset = gpuset
        self.info = info

    def _replace(self, **kwargs):
        obj_dict = vars(self)
        obj_dict.update(kwargs)
        return RunState(**obj_dict)
