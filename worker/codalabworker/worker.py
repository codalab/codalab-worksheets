from contextlib import closing
from subprocess import check_output
from collections import namedtuple
import logging
import multiprocessing
import os
import shutil
import threading
import time
import traceback
import re
import json
import socket
import httplib

from bundle_service_client import BundleServiceException
from dependency_manager import LocalFileSystemDependencyManager
from worker_state_manager import WorkerStateManager
from docker_client import DockerException
from download_util import BUNDLE_NO_LONGER_RUNNING_MESSAGE, get_target_info, get_target_path, PathException
from file_util import (
    un_tar_directory,
    get_path_size,
    gzip_file,
    gzip_string,
    read_file_section,
    summarize_file,
    tar_gzip_directory,
    remove_path,
)
from formatting import duration_str, size_str, parse_size
from run import Run
from docker_image_manager import DockerImageManager
from synchronized import synchronized
from fsm import (
    JsonStateCommitter,
    BaseStateHandler,
)


VERSION = 18

logger = logging.getLogger(__name__)

"""
Resumable Workers

    If the worker process of a worker machine terminates and restarts while a
    bundle is running, the worker process is able to keep track of the running
    bundle once again, as long as the state is intact and the bundle container
    is still running or has finished running.
"""

RunState = namedtuple('RunState',
    'status bundle bundle_path resources start_time container_id cpuset gpuset info')

class Worker(object):
    def __init__(self, state_committer, dependency_manager, image_manager,
                id, tag, work_dir, cpuset, gpuset, bundle_service,
                docker, docker_network_prefix='codalab_worker_network'):

        self._state_committer = state_committer
        self._dependency_manager = dependency_manager
        self._image_manager = image_manager

        self.id = id
        self._tag = tag
        self._work_dir = work_dir
        self._bundle_service = bundle_service
        self._docker = docker
        self._docker_network_prefix = docker_network_prefix
        self._stop = False
        self._should_upgrade = False
        self._last_checkin_successful = False

        self._cpuset = cpuset
        self._gpuset = gpuset
        self._runs = {}
        self._uploading = {}
        self._finalizing = {}

        # set up docker networks for running bundles: one with external network access and one without
        self.docker_network_external_name = self._docker_network_prefix + "_ext"
        if self.docker_network_external_name not in self._docker.list_networks():
            logger.debug('Creating docker network: {}'.format(self.docker_network_external_name))
            self._docker.create_network(self.docker_network_external_name, internal=False)
        else:
            logger.debug('Docker network already exists, not creating: {}'.format(self.docker_network_external_name))

        self.docker_network_internal_name = self._docker_network_prefix + "_int"
        if self.docker_network_internal_name not in self._docker.list_networks():
            logger.debug('Creating docker network: {}'.format(self.docker_network_internal_name))
            self._docker.create_network(self.docker_network_internal_name)
        else:
            logger.debug('Docker network already exists, not creating: {}'.format(self.docker_network_internal_name))

    def _save_state(self):
        with synchronized(self):
            self._state_committer.commit(self._runs)

    def _load_state(self):
        with synchronized(self):
            self._runs = self._state_committer.load()
            self.reset()

    def reset(self):
        """ reset each run (assume recovering from an interrupt) """

        with synchronized(self):
            for bundle_uuid in self._runs.keys():
                run_state = self._runs[bundle_uuid]
                self._runs[bundle_uuid] = self._reset_run_state(run_state)

    def run(self):
        while not self._stop:
            try:
                self._checkin()
                self._save_state()
                self._process_runs()
                self._save_state()

                if not self._last_checkin_successful:
                    logger.info('Connected! Successful check in.')
                self._last_checkin_successful = True

            except Exception:
                self._last_checkin_successful = False
                traceback.print_exc()
                time.sleep(1)

        self._image_manager.stop()
        self._dependency_manager.stop()

    def _get_runs_for_checkin(self):
        with synchronized(self):
            result = {
                bundle_uuid: {
                    'status': run_state.status,
                    'start_time': run_state.start_time,
                    'info': run_state.info
                } for bundle_uuid, run_state in self._runs.items()
            }
            return result

    def _get_installed_memory_bytes(self):
        try:
            return os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
        except ValueError:
            # Fallback to sysctl when os.sysconf('SC_PHYS_PAGES') fails on OS X
            return int(check_output(['sysctl', '-n', 'hw.memsize']).strip())

    def _get_memory_bytes(self):
        return self._get_installed_memory_bytes()

    def _get_gpu_count(self):
        info = self._docker.get_nvidia_devices_info()
        count = 0 if info is None else len(info['Devices'])
        return count

    def _checkin(self):
        """
        Checkin with the server and get a response. React to this response.
        This function must return fast to keep checkins frequent. Time consuming
        processes must be handled asyncronously.
        """
        request = {
            'version': VERSION,
            'will_upgrade': self._should_upgrade,
            'tag': self._tag,
            'cpus': len(self._cpuset),
            'gpus': len(self._gpuset),
            'memory_bytes': self._get_memory_bytes(),
            'dependencies': self._dependency_manager.list_all(),
            'hostname': socket.gethostname(),
            'runs': self._get_runs_for_checkin()
        }
        response = self._bundle_service.checkin(self.id, request)
        if response:
            type = response['type']
            logger.debug('Received %s message: %s', type, response)
            if type == 'run':
                self._run(response['bundle'], response['resources'])
            elif type == 'read':
                self._read(response['socket_id'], response['uuid'], response['path'],
                           response['read_args'])
            elif type == 'netcat':
                self._netcat(response['socket_id'], response['uuid'], response['port'],
                           response['message'])
            elif type == 'write':
                self._write(response['uuid'], response['subpath'],
                            response['string'])
            elif type == 'kill':
                self._kill(response['uuid'])
            elif type == 'upgrade':
                with self._exiting_lock:
                    self._exiting = True
                self._should_upgrade = True

    @staticmethod
    def read_run_missing(bundle_service, worker, socket_id):
        message = {
            'error_code': httplib.INTERNAL_SERVER_ERROR,
            'error_message': BUNDLE_NO_LONGER_RUNNING_MESSAGE,
        }
        bundle_service.reply(worker.id, socket_id, message)

    def _assign_cpu_and_gpu_sets(self, request_cpus, request_gpus):
        """
        Propose a cpuset and gpuset to a bundle based on given requested resources.
        No side effects.

        Arguments:
            request_cpus: integer
            request_gpus: integer

        Returns a 2-tuple:
            cpuset: assigned cpuset.
            gpuset: assigned gpuset.

        Throws an exception if unsuccessful.
        """
        cpuset, gpuset = set(self._cpuset), set(self._gpuset)

        with synchronized(self):
            for run_state in self._runs():
                if run_state.status == RunStatus.RUNNING:
                    cpuset -= run_state.cpuset
                    gpuset -= run_state.gpuset

        if len(cpuset) < request_cpus or len(gpuset) < request_gpus:
            raise Exception("Not enough cpus or gpus to assign!")

        def propose_set(resource_set, request_count):
            return set(list(resource_set)[:request_count])

        return propose_set(cpuset, request_cpus), propose_set(gpuset, request_gpus)

    def _run(self, bundle, resources):
        """
        First, checks in with the bundle service and sees if the bundle
        is still assigned to this worker. If not, returns immediately.
        Otherwise, create RunState and put into self._runs
        """

        bundle_path = self._dependency_manager.get_run_path(bundle['uuid'])
        bundle_uuid = bundle['uuid']
        now = time.time()
        start_message = {
            'hostname': socket.gethostname(),
            'start_time': int(now),
        }
        if self._bundle_service.start_bundle(self.id, bundle_uuid, start_message):

            run_state = RunState(
                    RunStatus.STARTING, bundle, bundle_path, resources,
                    now, None, None, None, None)

            with synchronized(self):
                self._runs[bundle_uuid] = run_state

    def _get_run(self, uuid):
        with synchronized(self):
            return self._runs.get(uuid, None)

    def _read(self, socket_id, uuid, path, read_args):
        def read(self, run_state, socket_id, path, read_args):
            def reply_error(code, message):
                message = {
                    'error_code': code,
                    'error_message': message,
                }
                self._bundle_service.reply(self.id, socket_id, message)

            dep_paths = set([dep['child_path'] for dep in run_state.bundle['dependencies']])
            try:
                read_type = read_args['type']
                if read_type == 'get_target_info':
                    # At the top-level directory, we should ignore dependencies.
                    if path and os.path.normpath(path) in dep_paths:
                        target_info = None
                    else:
                        try:
                            target_info = get_target_info(
                                run_state.bundle_path, self._uuid, path, read_args['depth'])
                        except PathException as e:
                            reply_error(httplib.BAD_REQUEST, e.message)
                            return

                        if not path and read_args['depth'] > 0:
                            target_info['contents'] = [
                                child for child in target_info['contents']
                                if child['name'] not in dep_paths]

                    self._bundle_service.reply(self.id, socket_id, {'target_info': target_info})
                else:
                    try:
                        final_path = get_target_path(run_state.bundle_path, run_state.bundle['uuid'], path)
                    except PathException as e:
                        reply_error(httplib.BAD_REQUEST, e.message)
                        return

                    if read_type == 'stream_directory':
                        if path:
                            exclude_names = []
                        else:
                            exclude_names = dep_paths
                        with closing(tar_gzip_directory(final_path, exclude_names=exclude_names)) as fileobj:
                            self._bundle_service.reply_data(self.id, socket_id, {}, fileobj)
                    elif read_type == 'stream_file':
                        with closing(gzip_file(final_path)) as fileobj:
                            self._bundle_service.reply_data(self.id, socket_id, {}, fileobj)
                    elif read_type == 'read_file_section':
                        string = gzip_string(read_file_section(
                            final_path, read_args['offset'], read_args['length']))
                        self._bundle_service.reply_data(self.id, socket_id, {}, string)
                    elif read_type == 'summarize_file':
                        string = gzip_string(summarize_file(
                            final_path, read_args['num_head_lines'],
                            read_args['num_tail_lines'], read_args['max_line_length'],
                            read_args['truncation_text']))
                        self._bundle_service.reply_data(self.id, socket_id, {}, string)
            except BundleServiceException:
                traceback.print_exc()
            except Exception as e:
                traceback.print_exc()
                reply_error(httplib.INTERNAL_SERVER_ERROR, e.message)

        run_state = self._get_run(uuid)
        if run_state is None:
            Worker.read_run_missing(self._bundle_service, self, socket_id)
        else:
            # Reads may take a long time, so do the read in a separate thread.
            threading.Thread(target=read, args=(self, run_state, socket_id, path, read_args)).start()

    def _netcat(self, socket_id, uuid, port, message):
        run = self._get_run(uuid)
        if run is None:
            Worker.read_run_missing(self._bundle_service, self, socket_id)
        else:
            # Reads may take a long time, so do the read in a separate thread.
            threading.Thread(target=Run.netcat,
                             args=(run, socket_id, port, message)).start()

    def _write(self, uuid, subpath, string):
        run = self._get_run(uuid)
        if run is not None:
            run.write(subpath, string)

    def _kill(self, uuid):
        run = self._get_run(uuid)
        if run is not None:
            run.kill('Kill requested')

    def _upgrade(self):
        logger.debug('Upgrading')
        worker_dir = os.path.dirname(os.path.realpath(__file__))

        while True:
            try:
                with closing(self._bundle_service.get_code()) as code:
                    remove_path(worker_dir)
                    un_tar_directory(code, worker_dir, 'gz')
                    break
            except Exception:
                traceback.print_exc()
                time.sleep(1)

        exit(123)

    def _process_runs(self):
        """ First, filter out finished runs then transition each run """
        with synchronized(self): # filter out finished runs
            self._runs = {k: v for k, v in self._runs.items() if v.status == RunStatus.FINISHED}
            for bundle_uuid in self._runs.keys():
                run_state = self._runs[bundle_uuid]
                self._runs[bundle_uuid] = self._transition_run_state(run_state)

    def _reset_run_state(self, run_state):
        status = run_state.status
        fns = [val for key, val in vars().items() if key == '_reset_run_state_from_' + status]
        return fns[0]

    def _transition_run_state(self, run_state):
        status = run_state.status
        fns = [val for key, val in vars().items() if key == '_transition_run_state_from_' + status]
        return fns[0]

    def _reset_run_state_from_STARTING(self, run_state):
        return run_state

    def _transition_run_state_from_STARTING(self, run_state):
        return run_state._replace(status=RunStatus.DOWNLOADING_DEPENDENCIES)

    def _reset_run_state_from_DOWNLOADING_DEPENDENCIES(self, run_state):
        return run_state

    def _transition_run_state_from_DOWNLOADING_DEPENDENCIES(self, run_state):
        #TODO: download status message?

        all_ready = True
        for entry in run_state.dependencies:
            dependency = (entry['parent_uuid'], entry['parent_path'])
            dependency_state = self._dependency_manager.get(dependency)
            all_ready = all_ready and (dependency_state.status == DependencyStatus.READY)

        image_state = self._image_manager.get(run_state.resources['docker_image'])
        all_ready = all_ready and (image_state.status == DependencyStatus.READY)

        bundle_uuid = run_state.bundle['uuid']
        if all_ready:
            return run_state._replace(status=RunStatus.LINKING)
        else:
            return run_state #TODO

    def _reset_run_state_from_LINKING(self, run_state):
        return run_state

    def _transition_run_state_from_LINKING(self, run_state):
        # Set up a directory to store the bundle.
        remove_path(run_state.bundle_path)
        os.mkdir(run_state.bundle_path)

        dependencies = []
        docker_dependencies_path = '/' + self._uuid + '_dependencies'
        for dep in run_state.bundle['dependencies']:
            child_path = os.path.normpath(os.path.join(run_state.bundle_path, dep['child_path']))
            if not child_path.startswith(run_state.bundle_path):
                raise Exception('Invalid key for dependency: %s' % (dep['child_path']))

            docker_dependency_path = os.path.join(docker_dependencies_path, dep['child_path'])
            os.symlink(docker_dependency_path, child_path)
            dependencies.append((dependency_path, docker_dependency_path))

        if run_state.resources['request_network']:
            docker_network = self.docker_network_external_name
        else:
            docker_network = self.docker_network_internal_name

        resources = run_state.resources
        cpuset, gpuset = self._assign_cpu_and_gpu_sets(
                resources['request_cpus'], resources['request_gpus'])

        container_id = self._docker.start_container(
            run_state.bundle_path, run_state.bundle['uuid'], run_state.bundle['command'],
            resources['docker_image'], docker_network, dependencies,
            cpuset, gpuset, resources['request_memory']
        )

        digest = self._docker.get_image_repo_digest(resources['docker_image'])
        ##self._safe_update_docker_image(digest)
        #self._image_manager.touch_image(digest)

        bundle_uuid = run_state.bundle['uuid']
        return run_state._replace(status=RunStatus.RUNNING,
            container_id=container_id, docker_image=digest, cpuset=cpuset, gpuset=gpuset)

    def _reset_run_state_from_RUNNING(self, run_state):
        return run_state

    def _transition_run_state_from_RUNNING(self, run_state):
        bundle_uuid = run_state.bundle['uuid']

        def check_and_report_finished(run_state):
            try:
                finished, exitcode, failure_msg = self._docker.check_finished(run_state.container_id)
            except DockerException:
                traceback.print_exc()
            return dict(finished=finished, exitcode=exitcode, failure_message=failure_msg)

        if finished:
            return run_state._replace(
                    status=RunStatus.UPLOADING_RESULTS, info=check_and_report_finished(run_state))
            logger.debug('Finished run with UUID %s, exitcode %s, failure_message %s',
                 bundle_uuid, info['exitcode'], info['failure_message'])
        else:
            return run_state #TODO

    def _reset_run_state_from_UPLOADING_RESULTS(self, run_state):
        return run_state

    def _transition_run_state_from_UPLOADING_RESULTS(self, run_state):
        bundle_uuid = run_state.bundle['uuid']
        def upload_results():
            try:
                # Delete the container.
                if run_state.container_id is not None:
                    while True:
                        try:
                            self._docker.delete_container(run_state.container_id)
                            break
                        except DockerException:
                            traceback.print_exc()
                            time.sleep(1)

                # Clean-up dependencies.
                for dep in self._bundle['dependencies']:
                    #self._dependency_manager.remove_dependency(
                    #    dep['parent_uuid'], dep['parent_path'], bundle_uuid)

                    # Clean-up the symlinks we created.
                    child_path = os.path.join(run_state.bundle_path, dep['child_path'])
                    remove_path(child_path)

                # Upload results
                logger.debug('Uploading results for run with UUID %s', self._uuid)
                updater = self._throttled_updater()
                def update_status(bytes_uploaded):
                    updater('Uploading results: %s done (archived size)' %
                        size_str(bytes_uploaded))
                self._execute_bundle_service_command_with_retry(
                    lambda: self._bundle_service.update_bundle_contents(
                        self.id, bundle_uuid, run_state.bundle_path,
                        update_status))
            except Exception:
                traceback.print_exc()

        if bundle_uuid not in self._uploading:
            self._uploading[bundle_uuid] = {
                'thread': threading.Thread(target=upload_results, args=[]),
            }
            self._uploading[bundle_uuid]['thread'].start()

        if self._uploading[bundle_uuid]['thread'].is_alive():
            return run_state
        else:
            return run_state._replace(status=RunStatus.FINALIZING)

    def _reset_run_state_from_FINALIZING(self, run_state):
        return run_state

    def _transition_run_state_from_FINALIZING(self, run_state):
        def finalize():
            try:
                logger.debug('Finalizing run with UUID %s', bundle_uuid)
                self._safe_update_run_status('Finished')  # Also, reports the finish time.
                if failure_message is None and self._is_killed():
                    failure_message = self._get_kill_message()
                finalize_message = {
                    'exitcode': exitcode,
                    'failure_message': failure_message,
                }
                self._execute_bundle_service_command_with_retry(
                    lambda: self._bundle_service.finalize_bundle(
                        self.id, bundle_uuid, finalize_message))
            except Exception:
                traceback.print_exc()

        if bundle_uuid not in self._finalizing:
            self._finalizing[bundle_uuid] = {
                'thread': threading.Thread(target=finalize, args=[]),
            }
            self._finalizing[bundle_uuid]['thread'].start()

        if self._finalizing[bundle_uuid]['thread'].is_alive():
            return run_state
        else:
            return run_state._replace(status=RunStatus.FINISHED)

    def _reset_run_state_from_FINISHED(self, run_state):
        return run_state

    def _transition_run_state_from_FINISHED(self, run_state):
        return run_state

class RunStatus(object):
    # reset: -> STARTING
    # transition: -> DOWNLOADING_DEPENDENCIES
    STARTING = 'STARTING'

    # reset: -> DOWNLOADING_DEPENDENCIES
    # transition: get each dep, if all ready -> LINKING, else DOWNLOADING_DEPENDENCIES
    DOWNLOADING_DEPENDENCIES = 'DOWNLOADING_DEPENDENCIES'

    # reset: -> LINKING
    # transition: recreate directories, symlinks and container. Start container -> RUNNING
    LINKING = 'LINKING'

    # reset: RUNNING
    # transition: If container finished -> UPLOADING_RESULTS else -> RUNNING
    RUNNING = 'RUNNING'

    # reset: -> UPLOADING_RESULTS
    # transition: if not in _uploading, create and start _uploading thread.
    # thread deletes container and uploads results
    # If not done -> UPLOADING_RESULTS else -> FINALIZING
    UPLOADING_RESULTS = 'UPLOADING_RESULTS'

    # reset: -> FINALIZING
    # transition: if not in _finalizing, create and start _finalizing thread
    # thread cleans up symlinks and finalizes bundle
    # If not done -> FINALIZING else -> FINISHED
    FINALIZING = 'FINALIZING'

    # reset: -> FINISHED
    # transition: -> FINISHED
    FINISHED = 'FINISHED'
