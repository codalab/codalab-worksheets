import logging
import os
import socket
from subprocess import PIPE, Popen
import threading
import time
import traceback
import http.client
import sys

import psutil
import docker

from codalab.worker.bundle_service_client import BundleServiceException
from codalab.worker.download_util import BUNDLE_NO_LONGER_RUNNING_MESSAGE
from codalab.worker.state_committer import JsonStateCommitter
from codalab.worker.reader import Reader
from codalab.worker.bundle_state import BundleInfo, RunResources, BundleCheckinState
from codalab.worker.worker_run_state import RunState, Preparing, Finished
from codalab.worker.worker_thread import ThreadDict

from codalab.worker import docker_utils

logger = logging.getLogger(__name__)

"""
Codalab Worker
Workers handle communications with the Codalab server. Their main role in Codalab execution
is syncing the job states with the server and passing on job-related commands from the server
to architecture-specific RunManagers that run the jobs. Workers are execution platform antagonistic
but they expect the platform specific RunManagers they use to implement a common interface
"""


class Worker:
    # Network buffer size to use while proxying with netcat
    NETCAT_BUFFER_SIZE = 4096
    # Number of seconds to wait for bundle kills to propagate before forcing kill
    KILL_TIMEOUT = 100
    # Directory name to store running bundles in worker filesystem
    BUNDLES_DIR_NAME = 'runs'
    # Number of loops to check for bundle directory creation by server on shared FS workers
    BUNDLE_DIR_WAIT_NUM_TRIES = 120
    # Number of seconds to retry a bundle service command
    COMMAND_RETRY_SECONDS = 60 * 12

    def __init__(
        self,
        commit_file,  # type: str
        worker_id,  # type: str
        tag,  # type: str
        work_dir,  # type: str
        local_bundles_dir,  # type: str
        exit_when_idle,  # type: str
        idle_seconds,  # type: int
        bundle_service,  # type: BundleServiceClient
        shared_file_system,  # type: bool
        image_manager,  # type: DockerImageManager
        dependency_manager,  # type: FileSystemDependencyManager
        cpuset,  # type: Set[str]
        gpuset,  # type: Set[str]
        docker_runtime=docker_utils.DEFAULT_RUNTIME,  # type: str
        docker_network_prefix='codalab_worker_network',  # type: str
    ):
        self.id = worker_id
        self.tag = tag
        self.work_dir = work_dir
        self.bundle_service = bundle_service
        self.exit_when_idle = exit_when_idle
        self.idle_seconds = idle_seconds
        self.shared_file_system = shared_file_system
        self.local_bundles_dir = local_bundles_dir
        self.image_manager = image_manager
        self.dependency_manager = dependency_manager
        self.cpuset = cpuset
        self.gpuset = gpuset
        self.stop = False
        self.last_checkin_successful = False
        self.last_time_ran = None  # When was the last time we ran something?

        self.docker = docker.from_env()
        self.docker_runtime = docker_runtime
        self.init_docker_networks(docker_network_prefix)

        self.state_committer = JsonStateCommitter(commit_file)
        self.reader = Reader()
        # bundle.uuid -> {'thread': Thread, 'run_status': str}
        self.uploading_threads = ThreadDict(
            fields={'run_status': 'Upload started', 'success': False}
        )
        # bundle.uuid -> {'thread': Thread, 'disk_utilization': int, 'running': bool}
        self.disk_utilization_threads = ThreadDict(
            fields={'disk_utilization': 0, 'running': True, 'lock': None}
        )
        self.runs = {}  # bundle_uuid -> RunState

    def init_docker_networks(self, docker_network_prefix):
        """
        Set up docker networks for runs: one with external network access and one without
        """

        def create_or_get_network(name, internal):
            try:
                logger.debug('Creating docker network %s', name)
                return self.docker.networks.create(name, internal=internal, check_duplicate=True)
            except docker.errors.APIError:
                logger.debug('Network %s already exists, reusing', name)
                return self.docker.networks.list(names=[name])[0]

        # Right now the suffix to the general worker network is hardcoded to manually match the suffix
        # in the docker-compose file, so make sure any changes here are synced to there.
        self.worker_docker_network = create_or_get_network(docker_network_prefix + "_general", True)
        self.docker_network_external = create_or_get_network(docker_network_prefix + "_ext", False)
        self.docker_network_internal = create_or_get_network(docker_network_prefix + "_int", True)

    def save_state(self):
        self.state_committer.commit({uuid: state.as_dict for uuid, state in self.runs.items()})

    def load_state(self):
        runs = self.state_committer.load()
        # Retrieve the complex container objects from the Docker API
        for uuid, run_state in runs.items():
            run_state = RunState.from_dict(run_state)
            if run_state.container_id:
                try:
                    run_state.container = self.docker.containers.get(run_state.container_id)
                except docker.errors.NotFound as ex:
                    logger.debug('Error getting the container for the run: %s', ex)
                    run_state = run_state.container_id = None
            self.runs[uuid] = run_state

    @property
    def should_stop_because_idle(self):
        now = time.time()
        if len(self.runs) > 0 or self.last_time_ran is None:
            self.last_time_ran = now
        is_idle = now - self.last_time_ran > self.idle_seconds
        return self.exit_when_idle and is_idle and self.last_checkin_successful

    def start(self):
        """Return whether we ran anything."""
        self.load_state()
        self.image_manager.start()
        if not self.shared_file_system:
            self.dependency_manager.start()
        while not self.stop:
            try:
                self.process_runs()
                self.save_state()
                self.checkin()
                self.save_state()
                if not self.last_checkin_successful:
                    logger.info('Connected! Successful check in!')
                self.last_checkin_successful = True
                if self.should_stop_because_idle:
                    self.stop = True
                    break

            except Exception:
                self.last_checkin_successful = False
                traceback.print_exc()
                # Sleep for a long time so we don't keep on failing.
                logger.error('Sleeping for 1 hour due to exception...please help me!')
                time.sleep(1 * 60 * 60)
        self.cleanup()

    def cleanup(self):
        self.image_manager.stop()
        if not self.shared_file_system:
            self.dependency_manager.stop()
        for uuid in self.disk_utilization_threads.keys():
            self.disk_utilization_threads[uuid]['running'] = False
        self.disk_utilization_threads.stop()
        self.uploading_threads.stop()
        self.save_state()
        try:
            self.worker_docker_network.remove()
            self.docker_network_internal.remove()
            self.docker_network_external.remove()
        except docker.errors.APIError as e:
            logger.error("Cannot clear docker networks: {}".format(str(e)))
        logger.info("Stopped Run Manager. Exiting")

    def signal(self):
        self.stop = True

    def checkin(self):
        """
        Checkin with the server and get a response. React to this response.
        This function must return fast to keep checkins frequent. Time consuming
        processes must be handled asynchronously.
        """
        request = {
            'tag': self.tag,
            'cpus': len(self.cpuset),
            'gpus': len(self.gpuset),
            'memory_bytes': psutil.virtual_memory().total,
            'free_disk_bytes': self.free_disk_bytes,
            'dependencies': [
                (dep_key.parent_uuid, dep_key.parent_path)
                for dep_key in self.dependency_manager.all_dependencies
                if self.dependency_manager
            ],
            'hostname': socket.gethostname(),
            'runs': [run_state.server_checkin_state for run_state in self.runs.values()],
            'shared_file_system': self.shared_file_system,
        }
        response = self.bundle_service.checkin(self.id, request)
        if response:
            action_type = response['type']
            logger.debug('Received %s message: %s', action_type, response)
            if action_type == 'run':
                self.run(response['bundle'], response['resources'])
            else:
                uuid = response['uuid']
                socket_id = response.get('socket_id', None)
                if uuid not in self.runs:
                    if action_type in ['read', 'netcat']:
                        self.read_run_missing(socket_id)
                    return
                if action_type == 'kill':
                    self.runs[uuid].kill_message = 'Kill requested'
                    self.runs[uuid].is_killed = True
                elif action_type == 'mark_finalized':
                    self.runs[uuid].finalized = True
                elif action_type == 'read':
                    self.read(socket_id, uuid, response['path'], response['read_args'])
                elif action_type == 'netcat':
                    self.netcat(socket_id, uuid, response['port'], response['message'])
                elif action_type == 'write':
                    self.write(uuid, response['subpath'], response['string'])
                else:
                    logger.warning("Unrecognized action type from server: %s", action_type)

    def run(self, bundle, resources):
        """
        First, checks in with the bundle service and sees if the bundle
        is still assigned to this worker. If not, returns immediately.
        Otherwise, tell RunManager to create the run.
        """
        now = time.time()
        start_message = {'hostname': socket.gethostname(), 'start_time': int(now)}

        if self.bundle_service.start_bundle(self.id, bundle['uuid'], start_message):
            bundle = BundleInfo.from_dict(bundle)
            resources = RunResources.from_dict(resources)
            if self.shared_file_system:
                local_bundle_path = bundle.location
            else:
                local_bundle_path = os.path.join(self.local_bundles_dir, bundle.uuid)
            now = time.time()
            run_state = Preparing(
                run_status='',
                bundle=bundle,
                local_bundle_path=os.path.realpath(local_bundle_path),
                remote=self.id,
                bundle_dir_wait_num_tries=Worker.BUNDLE_DIR_WAIT_NUM_TRIES,
                resources=resources,
                bundle_start_time=now,
                container_time_total=0,
                container_time_user=0,
                container_time_system=0,
                container_id=None,
                container=None,
                docker_image=None,
                is_killed=False,
                has_contents=False,
                cpuset=None,
                gpuset=None,
                max_memory=0,
                disk_utilization=0,
                exitcode=None,
                failure_message=None,
                kill_message=None,
                finished=False,
                finalized=False,
            )
            self.runs[bundle.uuid] = run_state
        else:
            print(
                'Bundle {} no longer assigned to this worker'.format(bundle['uuid']),
                file=sys.stdout,
            )

    def read(self, socket_id, uuid, path, args):
        def reply(err, message={}, data=None):
            self.bundle_service_reply(socket_id, err, message, data)

        try:
            run_state = self.runs[uuid]
            self.reader.read(run_state, path, args, reply)
        except BundleServiceException:
            traceback.print_exc()
        except Exception as e:
            traceback.print_exc()
            err = (http.client.INTERNAL_SERVER_ERROR, str(e))
            reply(err)

    def netcat(self, socket_id, uuid, port, message):
        """
        Write `message` (string) to port of bundle with uuid and read the response.
        Returns a stream with the response contents (bytes).
        """
        run_state = self.runs[uuid]
        container_ip = docker_utils.get_container_ip(
            self.worker_docker_network.name, run_state.container
        )

        def bundle_netcat():
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((container_ip, port))
                s.sendall(message.encode())
                total_data = []
                while True:
                    data = s.recv(Worker.NETCAT_BUFFER_SIZE)
                    if not data:
                        break
                    total_data.append(data)
                s.close()
                self.bundle_service_reply(socket_id, None, {}, b''.join(total_data))
            except BundleServiceException:
                traceback.print_exc()
            except Exception as e:
                traceback.print_exc()
                err = (http.client.INTERNAL_SERVER_ERROR, str(e))
                self.bundle_service_reply(socket_id, err, {}, None)

        netcat_thread = threading.Thread(target=bundle_netcat)
        netcat_thread.start()

    def write(self, uuid, path, string):
        run_state = self.runs[uuid]
        if os.path.normpath(path) in set(dep.child_path for dep in run_state.bundle.dependencies):
            return

        def bundle_write():
            with open(os.path.join(run_state.local_bundle_path, path), 'w') as f:
                f.write(string)

        write_thread = threading.Thread(target=bundle_write)
        write_thread.start()

    def upload_bundle_contents(self, bundle_uuid, bundle_path, update_status):
        self.execute_bundle_service_command_with_retry(
            lambda: self.bundle_service.update_bundle_contents(
                self.id, bundle_uuid, bundle_path, update_status
            )
        )

    def read_run_missing(self, socket_id):
        message = {
            'error_code': http.client.INTERNAL_SERVER_ERROR,
            'error_message': BUNDLE_NO_LONGER_RUNNING_MESSAGE,
        }
        self.bundle_service.reply(self.id, socket_id, message)

    def bundle_service_reply(self, socket_id, err, message, data):
        if err:
            err = {'error_code': err[0], 'error_message': err[1]}
            self.bundle_service.reply(self.id, socket_id, err)
        elif data:
            self.bundle_service.reply_data(self.id, socket_id, message, data)
        else:
            self.bundle_service.reply(self.id, socket_id, message)

    @staticmethod
    def execute_bundle_service_command_with_retry(cmd):
        retries_left = Worker.COMMAND_RETRY_SECONDS
        while True:
            try:
                retries_left -= 1
                cmd()
                return
            except BundleServiceException as e:
                if not e.client_error and retries_left > 0:
                    traceback.print_exc()
                    time.sleep(30)
                    continue
                raise

    def process_runs(self):
        """ Transition each run then filter out finished runs """
        # transition all runs
        for bundle_uuid in self.runs.keys():
            self.runs[bundle_uuid] = self.runs[bundle_uuid].update(self)

        # filter out finished runs
        finished_container_ids = [
            run.container_id
            for run in self.runs.values()
            if run.is_active and run.container_id is not None
        ]
        for container_id in finished_container_ids:
            try:
                container = self.docker.containers.get(container_id)
                container.remove(force=True)
            except (docker.errors.NotFound, docker.errors.NullResource):
                pass
        self.runs = {k: v for k, v in self.runs.items() if not isinstance(v, Finished)}

    def assign_cpu_and_gpu_sets(self, request_cpus, request_gpus):
        """
        Propose a cpuset and gpuset to a bundle based on given requested resources.
        Note: no side effects (this is important: we don't want to maintain more state than necessary)

        Arguments:
            request_cpus: integer
            request_gpus: integer

        Returns a 2-tuple:
            cpuset: assigned cpuset (str indices).
            gpuset: assigned gpuset (str indices).

        Throws an exception if unsuccessful.
        """
        cpuset, gpuset = set(self.cpuset), set(self.gpuset)

        for run_state in self.runs.values():
            if run_state.is_active:
                if run_state.cpuset:
                    cpuset -= run_state.cpuset
                if run_state.gpuset:
                    gpuset -= run_state.gpuset

        if len(cpuset) < request_cpus:
            raise Exception(
                "Requested more CPUs (%d) than available (%d currently out of %d on the machine)"
                % (request_cpus, len(cpuset), len(self.cpuset))
            )
        if len(gpuset) < request_gpus:
            raise Exception(
                "Requested more GPUs (%d) than available (%d currently out of %d on the machine)"
                % (request_gpus, len(gpuset), len(self.gpuset))
            )

        def propose_set(resource_set, request_count):
            return set(str(el) for el in list(resource_set)[:request_count])

        return propose_set(cpuset, request_cpus), propose_set(gpuset, request_gpus)

    @property
    def free_disk_bytes(self):
        """
        Available disk space by bytes of this RunManager.
        """
        error_msg = "Failed to run command {}".format("df " + self.work_dir)
        try:
            p = Popen(["df", self.work_dir], stdout=PIPE)
            output, error = p.communicate()
            # Return None when there is an error.
            if error:
                logger.error(error.strip())
                return None

            if output:
                lines = output.decode().split("\n")
                index = lines[0].split().index("Available")
                # We convert the original result from df command in unit of 1KB blocks into bytes.
                return int(lines[1].split()[index]) * 1024

        except Exception as e:
            logger.error("%s: %s", error_msg, str(e))
            return None
