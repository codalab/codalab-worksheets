import logging
import os
from subprocess import check_output
import threading
import time

from run_manager import BaseRunManager, RunState
from local_run_state import LocalRunStateMachine, LocalRunStage
from local_reader import LocalReader

logger = logging.getLogger(__name__)

class LocalRunManager(BaseRunManager):
    """
    LocalRunManager executes the runs locally, each one in its own Docker
    container. It manages its cache of local Docker images and its own local
    Docker network.
    """
    def __init__(self, worker, docker, image_manager, dependency_manager,
            state_committer, bundles_dir, cpuset, gpuset, docker_network_prefix='codalab_worker_network'):
        self._worker = worker
        self.docker = docker
        self.image_manager = image_manager
        self.dependency_manager = dependency_manager
        self.state_committer = state_committer
        self.cpuset = cpuset
        self.gpuset = gpuset
        self._docker_network_prefix = docker_network_prefix
        self._bundles_dir = bundles_dir

        self.runs = {}
        self.uploading = {}
        self.finalizing = {}
        self.lock = threading.RLock()

        self._run_state_manager = LocalRunStateMachine(self)
        self._reader = LocalReader()
        self._init_docker_networks()

    def _init_docker_networks(self):
        # set up docker networks for runs: one with external network access and one without
        self.docker_network_external_name = self._docker_network_prefix + "_ext"
        if self.docker_network_external_name not in self.docker.list_networks():
            logger.debug('Creating docker network: {}'.format(self.docker_network_external_name))
            self.docker.create_network(self.docker_network_external_name, internal=False)
        else:
            logger.debug('Docker network already exists, not creating: {}'.format(
                self.docker_network_external_name))

        self.docker_network_internal_name = self._docker_network_prefix + "_int"
        if self.docker_network_internal_name not in self.docker.list_networks():
            logger.debug('Creating docker network: {}'.format(self.docker_network_internal_name))
            self.docker.create_network(self.docker_network_internal_name)
        else:
            logger.debug('Docker network already exists, not creating: {}'.format(
                self.docker_network_internal_name))

    def save_state(self):
        self.state_committer.save_state(self.runs)

    def _load_state(self):
        self.runs = self.state_committer.load_state()

    def start(self):
        self._load_state()
        self.image_manager.start()
        self.dependency_manager.start()

    def stop(self):
        """
        Starts any necessary cleanup and propagates to its other managers
        Blocks until cleanup is complete and it is safe to quit
        """
        self.image_manager.stop()
        self.dependency_manager.stop()

    def process_runs(self):
        """ Transition each run then filter out finished runs """
        with self.lock:
            # transition all runs
            for bundle_uuid in self.runs.keys():
                run_state = self.runs[bundle_uuid]
                self.runs[bundle_uuid] = self._run_state_manager.transition(run_state)

            # filter out finished runs
            self.runs = {k: v for k, v in self.runs.items() if v.stage != LocalRunStage.FINISHED}

    def create_run(self, bundle, resources):
        """
        Creates and starts processing a new run with the given bundle and
        resources
        """
        bundle_uuid = bundle['uuid']
        bundle_path = self.dependency_manager.get_run_path(bundle_uuid)
        now = time.time()
        run_state = RunState(
                stage=LocalRunStage.STARTING, run_status='', bundle=bundle,
                bundle_path=os.path.realpath(bundle_path), resources=resources,
                start_time=now, container_id=None, docker_image=None, is_killed=False,
                cpuset=None, gpuset=None, info={},
        )
        with self.lock:
            self.runs[bundle_uuid] = run_state

    def _assign_cpu_and_gpu_sets(self, request_cpus, request_gpus):
        """
        Propose a cpuset and gpuset to a bundle based on given requested resources.
        Note: no side effects (this is important: we don't want to maintain more state than necessary)

        Arguments:
            request_cpus: integer
            request_gpus: integer

        Returns a 2-tuple:
            cpuset: assigned cpuset.
            gpuset: assigned gpuset.

        Throws an exception if unsuccessful.
        """
        cpuset, gpuset = set(self.cpuset), set(self.gpuset)

        with self.lock:
            for run_state in self.runs.values():
                if run_state.stage == LocalRunStage.RUNNING:
                    cpuset -= run_state.cpuset
                    gpuset -= run_state.gpuset

        if len(cpuset) < request_cpus or len(gpuset) < request_gpus:
            raise Exception("Not enough cpus or gpus to assign!")

        def propose_set(resource_set, request_count):
            return set(list(resource_set)[:request_count])

        return propose_set(cpuset, request_cpus), propose_set(gpuset, request_gpus)

    def get_run(self, uuid):
        """
        Returns the state of the run with the given UUID if it is managed
        by this RunManager, returns None otherwise
        """
        with self.lock:
            return self.runs.get(uuid, None)

    def finalize_bundle(self, bundle_uuid, finalize_message):
        self._worker.finalize_bundle(bundle_uuid, finalize_message)

    def upload_bundle_contents(self, bundle_uuid, bundle_path, update_status):
        self._worker.upload_bundle_contents(bundle_uuid, bundle_path, update_status)

    def read(self, run_state, path, dep_paths, args, reply):
        self._reader.read(run_state, path, dep_paths, args, reply)

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
        with self.lock:
            run_state.is_killed = True
            run_state.info['kill_message'] = 'Kill requested'

    @property
    def all_runs(self):
        """
        Returns a list of all the runs managed by this RunManager
        """
        with self.lock:
            result = {
                bundle_uuid: {
                    'run_status': run_state.run_status,
                    'start_time': run_state.start_time,
                    'docker_image': run_state.docker_image,
                    'info': run_state.info
                } for bundle_uuid, run_state in self.runs.items()
            }
            return result

    @property
    def all_dependencies(self):
        """
        Returns a list of all dependencies available in this RunManager
        """
        return self.dependency_manager.list_all()

    @property
    def cpus(self):
        """
        Total number of CPUs this RunManager has
        """
        return len(self.cpuset)

    @property
    def gpus(self):
        """
        Total number of GPUs this RunManager has
        """
        return len(self.gpuset)

    @property
    def memory_bytes(self):
        """
        Total installed memory of this RunManager
        """
        try:
            return os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
        except ValueError:
            # Fallback to sysctl when os.sysconf('SC_PHYS_PAGES') fails on OS X
            return int(check_output(['sysctl', '-n', 'hw.memsize']).strip())
