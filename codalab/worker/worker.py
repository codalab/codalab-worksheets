import logging
import os
import shutil
from subprocess import PIPE, Popen
import threading
import time
import traceback
import socket
import http.client
import sys
from typing import Optional, Set, Dict

import psutil

import docker
from codalab.lib.telemetry_util import capture_exception, using_sentry
from codalab.worker.runtime import Runtime
import requests

from .bundle_service_client import BundleServiceException, BundleServiceClient
from .dependency_manager import DependencyManager
from .docker_utils import DEFAULT_DOCKER_TIMEOUT, DEFAULT_RUNTIME
from .image_manager import ImageManager
from .download_util import BUNDLE_NO_LONGER_RUNNING_MESSAGE
from .state_committer import JsonStateCommitter
from .bundle_state import BundleInfo, RunResources, BundleCheckinState
from .worker_run_state import RunStateMachine, RunStage, RunState
from .reader import Reader

logger = logging.getLogger(__name__)
"""
Codalab Worker
Workers handle communications with the Codalab server. Their main role in Codalab execution
is syncing the job states with the server and passing on job-related commands from the server
to architecture-specific RunManagers that run the jobs. Workers are execution platform antagonistic
but they expect the platform specific RunManagers they use to implement a common interface
"""


class Worker:
    # Number of retries when a bundle service client command failed to execute. Defining a large number here
    # would allow offline workers to patiently wait until connection to server is re-established.
    COMMAND_RETRY_ATTEMPTS = 720
    # Network buffer size to use while proxying with netcat
    NETCAT_BUFFER_SIZE = 4096
    # Number of seconds to wait for bundle kills to propagate before forcing kill
    KILL_TIMEOUT = 100
    # Number of loops to check for bundle directory creation by server on shared FS workers
    BUNDLE_DIR_WAIT_NUM_TRIES = 120
    # Number of seconds to sleep if checking in with server fails two times in a row
    CHECKIN_COOLDOWN = 5

    def __init__(
        self,
        image_manager,  # type: ImageManager
        dependency_manager,  # type: Optional[DependencyManager]
        commit_file,  # type: str
        cpuset,  # type: Set[str]
        gpuset,  # type: Set[str]
        max_memory,  # type: Optional[int]
        worker_id,  # type: str
        tag,  # type: str
        work_dir,  # type: str
        local_bundles_dir,  # type: Optional[str]
        exit_when_idle,  # type: str
        exit_after_num_runs,  # type: int
        idle_seconds,  # type: int
        checkin_frequency_seconds,  # type: int
        bundle_service,  # type: BundleServiceClient
        shared_file_system,  # type: bool
        tag_exclusive,  # type: bool
        group_name,  # type: str
        bundle_runtime,  # type: Runtime
        docker_runtime=DEFAULT_RUNTIME,  # type: str
        docker_network_prefix='codalab_worker_network',  # type: str
        # A flag indicating if all the existing running bundles will be killed along with the worker.
        pass_down_termination=False,  # type: bool
        # A flag indicating if the work_dir will be deleted when the worker exits.
        delete_work_dir_on_exit=False,  # type: bool
        # A flag indicating if the worker will exit if it encounters an exception
        exit_on_exception=False,  # type: bool
        shared_memory_size_gb=1,  # type: int
        preemptible=False,  # type: bool
    ):
        self.image_manager = image_manager
        self.dependency_manager = dependency_manager
        self.reader = Reader()
        self.state_committer = JsonStateCommitter(commit_file)
        self.bundle_service = bundle_service

        self.docker = docker.from_env(timeout=DEFAULT_DOCKER_TIMEOUT)
        self.cpuset = cpuset
        self.gpuset = gpuset
        self.max_memory = (
            min(max_memory, psutil.virtual_memory().total)
            if max_memory is not None
            else psutil.virtual_memory().total
        )

        self.id = worker_id
        self.group_name = group_name
        self.tag = tag
        self.tag_exclusive = tag_exclusive

        self.work_dir = work_dir
        self.local_bundles_dir = local_bundles_dir
        self.shared_file_system = shared_file_system
        self.delete_work_dir_on_exit = delete_work_dir_on_exit

        self.exit_when_idle = exit_when_idle
        self.exit_after_num_runs = exit_after_num_runs
        self.num_runs = 0
        self.idle_seconds = idle_seconds

        self.terminate = False
        self.terminate_and_restage = False
        self.pass_down_termination = pass_down_termination
        self.exit_on_exception = exit_on_exception
        self.preemptible = preemptible
        self.bundle_runtime = bundle_runtime

        self.checkin_frequency_seconds = checkin_frequency_seconds
        self.last_checkin_successful = False
        self.last_time_ran = None  # type: Optional[bool]

        self.runs = {}  # type: Dict[str, RunState]
        self.docker_network_prefix = docker_network_prefix
        self.init_docker_networks(docker_network_prefix)
        self.run_state_manager = RunStateMachine(
            image_manager=self.image_manager,
            dependency_manager=self.dependency_manager,
            worker_docker_network=self.worker_docker_network,
            docker_network_internal=self.docker_network_internal,
            docker_network_external=self.docker_network_external,
            docker_runtime=docker_runtime,
            upload_bundle_callback=self.upload_bundle_contents,
            assign_cpu_and_gpu_sets_fn=self.assign_cpu_and_gpu_sets,
            shared_file_system=self.shared_file_system,
            shared_memory_size_gb=shared_memory_size_gb,
            bundle_runtime=bundle_runtime,
        )

    def init_docker_networks(self, docker_network_prefix, verbose=True):
        """
        Set up docker networks for runs: one with external network access and one without
        """

        def create_or_get_network(name, internal, verbose):
            try:
                if verbose:
                    logger.debug('Creating docker network %s', name)
                network = self.docker.networks.create(name, internal=internal, check_duplicate=True)
                # This logging statement is only run if a network is created.
                logger.debug('Created docker network %s', name)
                return network
            except docker.errors.APIError:
                if verbose:
                    logger.debug('Network %s already exists, reusing', name)
                return self.docker.networks.list(names=[name])[0]

        # Docker's default local bridge network only supports 30 different networks
        # (each one of them uniquely identifiable by their name), so we prune old,
        # unused docker networks, or network creation might fail. We only prune docker networks
        # older than 1h, to avoid interfering with any newly-created (but still unused) networks
        # that might have been created by other workers.
        try:
            self.docker.networks.prune(filters={"until": "1h"})
        except (docker.errors.APIError, requests.exceptions.RequestException) as e:
            # docker.errors.APIError is raised when a prune is already running:
            # https://github.com/codalab/codalab-worksheets/issues/2635
            # docker.errors.APIError: 409 Client Error: Conflict ("a prune operation is already running").
            # Any number of requests.exceptions.RequestException s are raised when the request to
            # the Docker socket times out or otherwise fails.
            # For example: https://github.com/docker/docker-py/issues/2266
            # Since pruning is a relatively non-essential routine (i.e., it's ok if pruning fails
            # on one or two iterations), we just ignore this issue.
            logger.warning("Cannot prune docker networks: %s", str(e))

        # Right now the suffix to the general worker network is hardcoded to manually match the suffix
        # in the docker-compose file, so make sure any changes here are synced to there.
        self.worker_docker_network = create_or_get_network(
            docker_network_prefix + "_general", internal=True, verbose=verbose
        )
        self.docker_network_external = create_or_get_network(
            docker_network_prefix + "_ext", internal=False, verbose=verbose
        )
        self.docker_network_internal = create_or_get_network(
            docker_network_prefix + "_int", internal=True, verbose=verbose
        )

    def save_state(self):
        # Remove complex container objects from state before serializing, these can be retrieved
        runs = {
            uuid: state._replace(
                container=None, bundle=state.bundle.as_dict, resources=state.resources.as_dict,
            )
            for uuid, state in self.runs.items()
        }
        self.state_committer.commit(runs)

    def load_state(self):
        # If the state file doesn't exist yet, have the state committer return an empty state.
        runs = self.state_committer.load(default=dict())
        # Retrieve the complex container objects from the Docker API
        for uuid, run_state in runs.items():
            if run_state.container_id:
                try:
                    run_state = run_state._replace(
                        container=self.docker.containers.get(run_state.container_id)
                    )
                except docker.errors.NotFound as ex:
                    logger.debug('Error getting the container for the run: %s', ex)
                    run_state = run_state._replace(container_id=None)
            self.runs[uuid] = run_state._replace(
                bundle=BundleInfo.from_dict(run_state.bundle),
                resources=RunResources.from_dict(run_state.resources),
            )

    def sync_state(self):
        """
        Sync worker run state by matching the fields that are read from worker-state.json with the RunState object.
        """
        for uuid, run_state in self.runs.items():
            if run_state._fields == RunState._fields:
                continue
            values = []
            for field in RunState._fields:
                # When there are additional new fields or missing fields detected, recreate the run_state
                # object to include or delete those fields specified from the RunState object
                if field in run_state._fields:
                    values.append(getattr(run_state, field))
                else:
                    values.append(None)
            self.runs[uuid] = RunState(*values)

    def check_idle_stop(self):
        """
        Checks whether the worker is idle (ie if it hasn't had runs for longer than the configured
        number of idle seconds) and if so, checks whether it is configured to exit when idle.

        :returns: True if the worker should stop because it is idle.
        In other words, True if the worker is configured to exit when idle,
        it is idle, and it has checked in at least once with the server.
        """
        now = time.time()
        if len(self.runs) > 0 or self.last_time_ran is None:
            self.last_time_ran = now

        idle_duration_seconds = now - self.last_time_ran
        if (
            self.exit_when_idle
            and idle_duration_seconds > self.idle_seconds
            and self.last_checkin_successful
        ):
            logger.warning(
                "Worker was idle for {} seconds. Exiting...".format(idle_duration_seconds)
            )
            return True
        return False

    def check_num_runs_stop(self):
        """
        Checks whether the worker has finished the number of job allowed to run.
        :return: True if the number of jobs allowed to run is 0 and all those runs are finished.
                 False if neither of the two conditions are met.
        """
        return self.exit_after_num_runs == self.num_runs and len(self.runs) == 0

    def start(self):
        """Return whether we ran anything."""
        self.load_state()
        self.sync_state()
        self.image_manager.start()
        if not self.shared_file_system:
            self.dependency_manager.start()
        while not self.terminate:
            try:
                self.checkin()
                last_checkin = time.time()
                # Process runs until it's time for the next checkin.
                while not self.terminate and (
                    time.time() - last_checkin <= self.checkin_frequency_seconds
                ):
                    self.check_termination()
                    self.save_state()
                    if self.check_idle_stop() or self.check_num_runs_stop():
                        self.terminate = True
                        break
                    self.process_runs()
                    time.sleep(0.003)
                    self.save_state()
            except Exception:
                self.last_checkin_successful = False
                if using_sentry():
                    capture_exception()
                traceback.print_exc()
                if self.exit_on_exception:
                    logger.warning(
                        'Encountered exception, terminating the worker after sleeping for 5 minutes...'
                    )
                    self.terminate = True
                    # Sleep for 5 minutes
                    time.sleep(5 * 60)
                else:
                    # Sleep for a long time so we don't keep on failing.
                    # We sleep in 5-second increments to check
                    # if the worker needs to terminate (say, if it's received
                    # a SIGTERM signal).
                    logger.warning('Sleeping for 1 hour due to exception...please help me!')
                    for _ in range(12 * 60):
                        # We run this here, instead of going through another iteration of the
                        # while loop, to minimize the code that's run---the reason we ended up here
                        # in the first place is because of an exception, so we don't want to
                        # re-trigger that exception.
                        if self.terminate_and_restage:
                            # If self.terminate_and_restage is true, self.check_termination()
                            # restages bundles. We surround this in a try-except block,
                            # so we can still properly terminate and clean up
                            # even if self.check_termination() fails for some reason.
                            try:
                                self.check_termination()
                            except Exception:
                                traceback.print_exc()
                            self.terminate = True
                        if self.terminate:
                            break
                        time.sleep(5)
        self.cleanup()

    def cleanup(self):
        """
        Starts any necessary cleanup and propagates to its other managers
        Blocks until cleanup is complete and it is safe to quit
        """
        logger.info("Stopping Worker")
        self.image_manager.stop()
        if not self.shared_file_system:
            self.dependency_manager.stop()
        self.run_state_manager.stop()
        self.save_state()
        if self.delete_work_dir_on_exit:
            shutil.rmtree(self.work_dir)
        try:
            self.worker_docker_network.remove()
            self.docker_network_internal.remove()
            self.docker_network_external.remove()
        except docker.errors.APIError as e:
            logger.warning("Cannot clear docker networks: %s", str(e))

        logger.info("Stopped Worker. Exiting")

    def signal(self):
        """
        When the pass_down_termination flag is False, set the stop flag to stop running
        the worker without changing the status of existing running bundles. Otherwise,
        set the terminate_and_restage flag to restage all bundles that are not in the
        terminal states [FINISHED, RESTAGED].
        """
        if not self.pass_down_termination:
            self.terminate = True
        else:
            self.terminate_and_restage = True

    def check_termination(self):
        """
        If received pass_down_termination signal from CLI to terminate the worker, wait until
        all the existing unfinished bundles are restaged, reset runs, then stop the worker.
        """
        if self.terminate_and_restage:
            if self.restage_bundles() == 0:
                # Stop the worker
                self.terminate = True
                # Reset the current runs to exclude bundles in terminal states
                # before save state one last time to worker-state.json
                self.runs = {
                    uuid: run_state
                    for uuid, run_state in self.runs.items()
                    if run_state.stage not in [RunStage.FINISHED, RunStage.RESTAGED]
                }

    def restage_bundles(self):
        """
        Restage bundles not in the final states [FINISHED and RESTAGED] from worker to server.
        :return: the number of restaged bundles
        """
        restaged_bundles = []
        terminal_stages = [RunStage.FINISHED, RunStage.RESTAGED]
        for uuid in self.runs:
            run_state = self.runs[uuid]
            if run_state.stage not in terminal_stages:
                self.restage_bundle(uuid)
                restaged_bundles.append(uuid)
        if len(restaged_bundles) > 0:
            logger.info(
                "Sending bundles back to the staged state: {}.".format(','.join(restaged_bundles))
            )
        return len(restaged_bundles)

    @property
    def cached_dependencies(self):
        """
        Returns a list of the keys (as tuples) of all bundle dependencies this worker
        has cached, in the format the server expects it in the worker check-in.
        If the worker is on shared file system, it doesn't cache any dependencies and an
        empty list is returned even though all dependencies are accessible on the shared
        file system.
        """
        if self.shared_file_system:
            return []
        else:
            return [
                (dep_key.parent_uuid, dep_key.parent_path)
                for dep_key in self.dependency_manager.all_dependencies
            ]

    def checkin(self):
        """
        Checkin with the server and get a response. React to this response.
        This function must return fast to keep checkins frequent. Time consuming
        processes must be handled asynchronously.
        """
        request = {
            'tag': self.tag,
            'group_name': self.group_name,
            'cpus': len(self.cpuset),
            'gpus': len(self.gpuset),
            'memory_bytes': self.max_memory,
            'free_disk_bytes': self.free_disk_bytes,
            'dependencies': self.cached_dependencies,
            'hostname': socket.gethostname(),
            'runs': [run.as_dict for run in self.all_runs],
            'shared_file_system': self.shared_file_system,
            'tag_exclusive': self.tag_exclusive,
            'exit_after_num_runs': self.exit_after_num_runs - self.num_runs,
            'is_terminating': self.terminate or self.terminate_and_restage,
            'preemptible': self.preemptible,
        }
        try:
            response = self.bundle_service.checkin(self.id, request)
            logger.info('Connected! Successful check in!')
            self.last_checkin_successful = True
        except BundleServiceException as ex:
            logger.warning("Disconnected from server! Failed check in: %s", ex)
            if not self.last_checkin_successful:
                logger.info(
                    "Checkin failed twice in a row, sleeping %d seconds", self.CHECKIN_COOLDOWN
                )
                time.sleep(self.CHECKIN_COOLDOWN)
            self.last_checkin_successful = False
            response = None
        # Stop processing any new runs received from server
        if not response or self.terminate_and_restage or self.terminate:
            return
        action_type = response['type']
        logger.debug('Received %s message: %s', action_type, response)
        if action_type == 'run':
            self.initialize_run(response['bundle'], response['resources'])
        else:
            uuid = response['uuid']
            socket_id = response.get('socket_id', None)
            if uuid not in self.runs:
                if action_type in ['read', 'netcat']:
                    self.read_run_missing(socket_id)
                return
            if action_type == 'kill':
                self.kill(uuid)
            elif action_type == 'mark_finalized':
                self.mark_finalized(uuid)
            elif action_type == 'read':
                self.read(socket_id, uuid, response['path'], response['read_args'])
            elif action_type == 'netcat':
                self.netcat(socket_id, uuid, response['port'], response['message'])
            elif action_type == 'write':
                self.write(uuid, response['subpath'], response['string'])
            else:
                logger.warning("Unrecognized action type from server: %s", action_type)

    def process_runs(self):
        """ Transition each run then filter out finished runs """
        # We (re-)initialize the Docker networks here, in case they've been removed.
        # For any networks that exist, this is essentially a no-op.
        self.init_docker_networks(self.docker_network_prefix, verbose=False)
        # In case the docker networks have changed, we also update them in the RunStateMachine
        self.run_state_manager.worker_docker_network = self.worker_docker_network
        self.run_state_manager.docker_network_external = self.docker_network_external
        self.run_state_manager.docker_network_internal = self.docker_network_internal

        # 1. transition all runs
        for uuid in self.runs:
            prev_state = self.runs[uuid]
            self.runs[uuid] = self.run_state_manager.transition(prev_state)
            # Only start saving stats for a new stage when the run has actually transitioned to that stage.
            if prev_state.stage != self.runs[uuid].stage:
                self.end_stage_stats(uuid, prev_state.stage)
                if self.runs[uuid].stage not in [RunStage.FINISHED, RunStage.RESTAGED]:
                    self.start_stage_stats(uuid, self.runs[uuid].stage)

        # 2. filter out finished runs and clean up containers
        finished_container_ids = [
            run.container_id
            for run in self.runs.values()
            if (run.stage == RunStage.FINISHED or run.stage == RunStage.FINALIZING)
            and run.container_id is not None
        ]
        for container_id in finished_container_ids:
            self.bundle_runtime.remove(container_id)

        # 3. reset runs for the current worker
        self.runs = {
            uuid: run_state
            for uuid, run_state in self.runs.items()
            if run_state.stage != RunStage.FINISHED
        }

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
        cpuset, gpuset = set(map(str, self.cpuset)), set(map(str, self.gpuset))

        for run_state in self.runs.values():
            if run_state.stage == RunStage.RUNNING:
                cpuset -= run_state.cpuset
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
    def all_runs(self):
        """
        Returns a list of all the runs managed by this RunManager
        """
        return [
            BundleCheckinState(
                uuid=run_state.bundle.uuid,
                run_status=run_state.run_status,
                bundle_start_time=run_state.bundle_start_time,
                container_time_total=run_state.container_time_total,
                container_time_user=run_state.container_time_user,
                container_time_system=run_state.container_time_system,
                docker_image=run_state.docker_image,
                state=RunStage.WORKER_STATE_TO_SERVER_STATE[run_state.stage],
                remote=self.id,
                exitcode=run_state.exitcode,
                failure_message=run_state.failure_message,
                bundle_profile_stats=run_state.bundle_profile_stats,
                cpu_usage=run_state.cpu_usage,
                memory_usage=run_state.memory_usage,
            )
            for run_state in self.runs.values()
        ]

    @property
    def free_disk_bytes(self):
        """
        Available disk space by bytes of this RunManager.
        """
        error_msg = "Failed to run command {}".format("df -k" + self.work_dir)
        try:
            # Option "-k" will ensure us with the returning disk space in 1KB units
            p = Popen(["df", "-k", self.work_dir], stdout=PIPE)
            output, error = p.communicate()
            # Return None when there is an error.
            if error:
                logger.error(error.strip() + ": {}".format(error))
                return None

            if output:
                lines = output.decode().split("\n")
                headers = lines[0].split()
                # The machine being attached as a worker may be using a different language other than
                # English, so check the 4th header if "Available" is not present.
                index = headers.index("Available") if "Available" in headers else 3
                # We convert the original result from df command in unit of 1KB units into bytes.
                return int(lines[1].split()[index]) * 1024

        except Exception as e:
            logger.error("{}: {}".format(error_msg, str(e)))
            return None

    def initialize_run(self, bundle, resources):
        """
        First, checks if the worker has already finished receiving/starting the number of jobs allowed to run.
        If not, returns immediately.
        Then, checks in with the bundle service and sees if the bundle is still assigned to this worker.
        If not, returns immediately.
        Otherwise, tell RunManager to create the run.
        """
        if self.exit_after_num_runs == self.num_runs:
            print(
                'Worker has finished starting the number of jobs allowed to run on: {}. '
                'Stop starting further runs.'.format(self.exit_after_num_runs),
                file=sys.stdout,
            )
            return

        now = time.time()
        start_message = {'hostname': socket.gethostname(), 'start_time': int(now)}

        if self.bundle_service.start_bundle(self.id, bundle['uuid'], start_message):
            bundle = BundleInfo.from_dict(bundle)
            resources = RunResources.from_dict(resources)
            if self.terminate:
                # Run Manager stopped, refuse more runs
                return
            bundle_path = (
                bundle.location
                if self.shared_file_system
                else os.path.join(self.local_bundles_dir, bundle.uuid)
            )
            self.runs[bundle.uuid] = RunState(
                stage=RunStage.PREPARING,
                run_status='',
                bundle=bundle,
                bundle_path=os.path.realpath(bundle_path),
                bundle_dir_wait_num_tries=Worker.BUNDLE_DIR_WAIT_NUM_TRIES,
                bundle_profile_stats={
                    RunStage.PREPARING: self.init_stage_stats(),
                    RunStage.RUNNING: self.init_stage_stats(),
                    RunStage.CLEANING_UP: self.init_stage_stats(),
                    RunStage.UPLOADING_RESULTS: self.init_stage_stats(),
                    RunStage.FINALIZING: self.init_stage_stats(),
                },
                resources=resources,
                bundle_start_time=time.time(),
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
                is_restaged=False,
                cpu_usage=0.0,
                memory_usage=0.0,
                paths_to_remove=[],
            )
            # Start measuring bundle stats for the initial bundle state.
            self.start_stage_stats(bundle.uuid, RunStage.PREPARING)
            # Increment the number of runs that have been successfully started on this worker
            self.num_runs += 1
        else:
            print(
                'Bundle {} no longer assigned to this worker'.format(bundle['uuid']),
                file=sys.stdout,
            )

    def kill(self, uuid):
        """
        Marks the run as killed so that the next time its state is processed it is terminated.
        """
        self.runs[uuid] = self.runs[uuid]._replace(kill_message='Kill requested', is_killed=True)

    def restage_bundle(self, uuid):
        """
        Marks the run as restaged so that it can be sent back to the STAGED state before the worker is terminated.
        """
        self.runs[uuid] = self.runs[uuid]._replace(is_restaged=True)

    def mark_finalized(self, uuid):
        """
        Marks the run with uuid as finalized so it might be purged from the worker state
        """
        self.runs[uuid] = self.runs[uuid]._replace(finalized=True)

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
        Sends `message` to `port` of the Docker container of the run with `uuid` and
        streams the response on `socket_id`.

        This is all done on an unmanaged thread (ie launched and forgotten) because
        the thread has no further effects on the run as far as the worker is concerned
        and we do not need to terminate/join the thread from the worker process. It just
        terminates when the user is done with their connection or the Docker container for
        the run terminates.
        """

        def reply(err, message={}, data=None):
            self.bundle_service_reply(socket_id, err, message, data)

        def netcat_fn():
            try:
                run_state = self.runs[uuid]
                container_ip = self.bundle_runtime.get_container_ip(
                    self.worker_docker_network.name, run_state.container_id
                )
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
                reply(None, {}, b''.join(total_data))
            except BundleServiceException:
                traceback.print_exc()
            except Exception as e:
                traceback.print_exc()
                err = (http.client.INTERNAL_SERVER_ERROR, str(e))
                reply(err)

        threading.Thread(target=netcat_fn).start()

    def write(self, uuid, path, string):
        run_state = self.runs[uuid]
        if os.path.normpath(path) in set(dep.child_path for dep in run_state.bundle.dependencies):
            return

        def write_fn():
            with open(os.path.join(run_state.bundle_path, path), 'w') as f:
                f.write(string)

        threading.Thread(target=write_fn).start()

    def upload_bundle_contents(
        self, bundle_uuid, bundle_path, exclude_patterns, store, update_status
    ):
        self.execute_bundle_service_command_with_retry(
            lambda: self.bundle_service.update_bundle_contents(
                self.id, bundle_uuid, bundle_path, exclude_patterns, store, update_status
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

    def start_stage_stats(self, uuid: str, stage: str) -> None:
        """
        Set the start time for a bundle in a certain stage.
        """
        self.runs[uuid].bundle_profile_stats[stage]['start'] = time.time()

    def end_stage_stats(self, uuid: str, stage: str) -> None:
        """
        Set the end time for a bundle finishing a stage.
        Set the elapsed time to the end time minus the start time.
        """
        self.runs[uuid].bundle_profile_stats[stage]['end'] = time.time()
        self.runs[uuid].bundle_profile_stats[stage]['elapsed'] = (
            self.runs[uuid].bundle_profile_stats[stage]['end']
            - self.runs[uuid].bundle_profile_stats[stage]['start']
        )

    def init_stage_stats(self) -> Dict:
        """
        Returns a stage stats dict with default empty values for start, end, and elapsed.
        """
        return {'start': None, 'end': None, 'elapsed': None}

    @staticmethod
    def execute_bundle_service_command_with_retry(cmd):
        retries_left = Worker.COMMAND_RETRY_ATTEMPTS
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
