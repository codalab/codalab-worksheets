import logging
import os
from subprocess import check_output
import threading
import time
import socket

from codalabworker.worker_thread import ThreadDict
from codalabworker.state_committer import JsonStateCommitter
from codalabworker.run_manager import BaseRunManager
from local_run_state import LocalRunStateMachine, LocalRunStage, LocalRunState
from local_reader import LocalReader

logger = logging.getLogger(__name__)


class LocalRunManager(BaseRunManager):
    """
    LocalRunManager executes the runs locally, each one in its own Docker
    container. It manages its cache of local Docker images and its own local
    Docker network.
    """
    # Network buffer size to use while proxying with netcat
    NETCAT_BUFFER_SIZE = 4096
    # Number of seconds to wait for bundle kills to propagate before forcing kill
    KILL_TIMEOUT = 100
    # Directory name to store running bundles in worker filesystem
    BUNDLES_DIR_NAME = 'runs'

    def __init__(self, worker, docker, image_manager, dependency_manager,
                 commit_file, cpuset, gpuset, work_dir, docker_network_prefix='codalab_worker_network'):
        self._worker = worker
        self._state_committer = JsonStateCommitter(commit_file)
        self._run_state_manager = LocalRunStateMachine(self)
        self._reader = LocalReader()
        self._docker_network_prefix = docker_network_prefix
        self._bundles_dir = os.path.join(work_dir, LocalRunManager.BUNDLES_DIR_NAME)
        if not os.path.exists(self._bundles_dir):
            logger.info('{} doesn\'t exist, creating.'.format(self._bundles_dir))
            os.makedirs(self._bundles_dir, 0770)

        # These members are public as the run state manager needs access to them
        self.docker = docker
        self.image_manager = image_manager
        self.dependency_manager = dependency_manager
        self.cpuset = cpuset
        self.gpuset = gpuset
        self._stop = False

        self.runs = {}  # bundle_uuid -> LocalRunState
        # bundle_uuid -> {'thread': Thread, 'disk_utilization': int, 'running': bool}
        self.disk_utilization = ThreadDict(fields={'disk_utilization': 0,
                                                   'running': True,
                                                   'lock': None})
        # bundle_uuid -> {'thread': Thread, 'run_status': str}
        self.uploading = ThreadDict(fields={'run_status': 'Upload started'})
        self.lock = threading.RLock()
        self._init_docker_networks()

    def _init_docker_networks(self):
        """
        Set up docker networks for runs: one with external network access and one without
        """
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
        self._state_committer.commit(self.runs)

    def load_state(self):
        self.runs = self._state_committer.load()

    def start(self):
        """
        Load your state from disk, and start your sub-managers
        """
        self.load_state()
        self.image_manager.start()
        self.dependency_manager.start()

    def stop(self):
        """
        Starts any necessary cleanup and propagates to its other managers
        Blocks until cleanup is complete and it is safe to quit
        """
        logger.info("Stopping Local Run Manager")
        self._stop = True
        self.image_manager.stop()
        self.dependency_manager.stop()
        for uuid in self.disk_utilization.keys():
            self.disk_utilization[uuid]['running'] = False
        self.disk_utilization.stop()
        self.uploading.stop()
        self.save_state()
        logger.info("Stopped Local Run Manager. Exiting")

    def kill_all(self):
        """
        Kills all runs
        """
        logger.debug("Killing all bundles")
        # Set all bundle statuses to killed
        with self.lock:
            for uuid in self.runs.keys():
                run_state = self.runs[uuid]
                run_state.info['kill_message'] = 'Worker stopped'
                run_state = run_state._replace(info=run_state.info, is_killed=True)
                self.runs[uuid] = run_state
        # Wait until all runs finished or KILL_TIMEOUT seconds pas
        for attempt in range(LocalRunManager.KILL_TIMEOUT):
            with self.lock:
                self.runs = {k: v for k, v in self.runs.items() if v.stage != LocalRunStage.FINISHED}
                if len(self.runs) > 0:
                    logger.debug("Waiting for {} more bundles. {} seconds until force quit.".format(
                        len(self.runs), LocalRunManager.KILL_TIMEOUT - attempt))
            time.sleep(1)

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
        if self._stop:
            # Run Manager stopped, refuse more runs
            return
        bundle_uuid = bundle['uuid']
        bundle_path = os.path.join(self._bundles_dir, bundle_uuid)
        now = time.time()
        run_state = LocalRunState(stage=LocalRunStage.PREPARING,
                                  run_status='',
                                  bundle=bundle,
                                  bundle_path=os.path.realpath(bundle_path),
                                  resources=resources,
                                  start_time=now,
                                  container_id=None,
                                  docker_image=None,
                                  is_killed=False,
                                  has_contents=False,
                                  cpuset=None,
                                  gpuset=None,
                                  time_used=0,
                                  max_memory=0,
                                  disk_utilization=0,
                                  info={})
        with self.lock:
            self.runs[bundle_uuid] = run_state

    def assign_cpu_and_gpu_sets(self, request_cpus, request_gpus):
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

    def mark_finalized(self, uuid):
        """
        Marks the run as finalized server-side so it can be discarded
        """
        if uuid in self.runs:
            with self.lock:
                self.runs[uuid].info['finalized'] = True

    def upload_bundle_contents(self, bundle_uuid, bundle_path, progress_callback):
        """
        Use the Worker API to upload contents of bundle_path to bundle_uuid
        """
        self._worker.upload_bundle_contents(bundle_uuid, bundle_path, progress_callback)

    def read(self, run_state, path, dep_paths, args, reply):
        """
        Use your Reader helper to invoke the given read command
        """
        self._reader.read(run_state, path, dep_paths, args, reply)

    def write(self, run_state, path, dep_paths, string):
        """
        Write string to path in bundle with uuid
        """
        if os.path.normpath(path) in dep_paths:
            return
        with open(os.path.join(run_state.bundle_path, path), 'w') as f:
            f.write(string)

    def netcat(self, run_state, port, message, reply):
        """
        Write message to port of bundle with uuid and read the response.
        Returns a stream with the response contents
        """
        container_ip = self.docker.get_container_ip(self.docker_network_external_name,
                                                    run_state.container_id)
        if not container_ip:
            container_ip = self.docker.get_container_ip(self.docker_network_internal_name,
                                                        run_state.container_id)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((container_ip, port))
        s.sendall(message)

        total_data = []
        while True:
            data = s.recv(LocalRunManager.NETCAT_BUFFER_SIZE)
            if not data:
                break
            total_data.append(data)
        s.close()
        reply(None, {}, ''.join(total_data))

    def kill(self, run_state):
        """
        Kill bundle with uuid
        """
        with self.lock:
            run_state.info['kill_message'] = 'Kill requested'
            run_state = run_state._replace(info=run_state.info, is_killed=True)
            self.runs[run_state.bundle['uuid']] = run_state

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
                    'info': run_state.info,
                    'state': LocalRunStage.WORKER_STATE_TO_SERVER_STATE[run_state.stage]
                } for bundle_uuid, run_state in self.runs.items()
            }
            return result

    @property
    def all_dependencies(self):
        """
        Returns a list of all dependencies available in this RunManager
        """
        return self.dependency_manager.all_dependencies

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

    @property
    def dependencies_dir(self):
        return self.dependency_manager.dependencies_dir
