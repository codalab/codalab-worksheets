from contextlib import closing
from subprocess import check_output
from collections import namedtuple
import logging
import os
import shutil
import threading
import time
import traceback
import re
import json
import socket
import httplib
import sys

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
from docker_image_manager import DockerImageManager
from synchronized import synchronized
from fsm import (
    JsonStateCommitter,
    DependencyStage
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
    ['stage', 'run_status', 'bundle', 'bundle_path', 'resources', 'start_time',
        'container_id', 'docker_image', 'is_killed', 'cpuset', 'gpuset', 'info'])

class Worker(object):
    def __init__(self, state_committer, dependency_manager, image_manager,
                id, tag, work_dir, cpuset, gpuset, bundle_service,
                docker, docker_network_prefix='codalab_worker_network'):

        self._state_committer = state_committer
        self._dependency_manager = dependency_manager
        self._image_manager = image_manager

        dependency_manager.run()
        image_manager.run()

        self.id = id
        self._tag = tag
        self._work_dir = work_dir
        self._bundles_dir = os.path.join(work_dir, 'bundles')
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

    def run(self):
        while not self._stop:
            try:
                self._process_runs()
                self._save_state()
                self._checkin()
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
                    'run_status': run_state.run_status,
                    'start_time': run_state.start_time,
                    'docker_image': run_state.docker_image,
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

    def _execute_bundle_service_command_with_retry(self, f):
        # Retry for 6 hours before giving up.
        retries_left = 2 * 60 * 6
        while True:
            try:
                retries_left -= 1
                f()
                return
            except BundleServiceException as e:
                if not e.client_error and retries_left > 0:
                    traceback.print_exc()
                    time.sleep(30)
                    continue
                raise

    def _assign_cpu_and_gpu_sets(self, request_cpus, request_gpus):
        """
        Propose a cpuset and gpuset to a bundle based on given requested resources.
        Note: no side effects (this is important, we don't want to keep more state than necessary)

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
            for run_state in self._runs.values():
                if run_state.stage == RunStage.RUNNING:
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

        bundle_uuid = bundle['uuid']
        bundle_path = self._dependency_manager.get_run_path(bundle_uuid)
        now = time.time()
        start_message = {
            'hostname': socket.gethostname(),
            'start_time': int(now),
        }
        if self._bundle_service.start_bundle(self.id, bundle_uuid, start_message):
            run_state = RunState(
                    stage=RunStage.STARTING, run_status='', bundle=bundle,
                    bundle_path=os.path.realpath(bundle_path), resources=resources,
                    start_time=now, container_id=None, docker_image=None, is_killed=False,
                    cpuset=None, gpuset=None, info={},
            )

            with synchronized(self):
                self._runs[bundle_uuid] = run_state
        else:
            print >>sys.stderr, "Not allowed to start"

    def _get_run(self, uuid):
        with synchronized(self):
            return self._runs.get(uuid, None)

    @staticmethod
    def read_run_missing(bundle_service, worker, socket_id):
        message = {
            'error_code': httplib.INTERNAL_SERVER_ERROR,
            'error_message': BUNDLE_NO_LONGER_RUNNING_MESSAGE,
        }
        bundle_service.reply(worker.id, socket_id, message)

    def _read(self, socket_id, uuid, path, read_args):
        def read(self, run_state, socket_id, path, read_args):
            def reply_error(code, message):
                message = {
                    'error_code': code,
                    'error_message': message,
                }
                self._bundle_service.reply(self.id, socket_id, message)

            dep_paths = set([dep['child_path'] for dep in run_state.bundle['dependencies']])
            bundle_uuid = run_state.bundle['uuid']
            try:
                read_type = read_args['type']
                if read_type == 'get_target_info':
                    # At the top-level directory, we should ignore dependencies.
                    if path and os.path.normpath(path) in dep_paths:
                        target_info = None
                    else:
                        try:
                            target_info = get_target_info(
                                run_state.bundle_path, bundle_uuid, path, read_args['depth'])
                        except PathException as e:
                            reply_error(httplib.BAD_REQUEST, e.message)
                            return

                        if target_info is not None and not path and read_args['depth'] > 0:
                            target_info['contents'] = [
                                child for child in target_info['contents']
                                if child['name'] not in dep_paths]

                    self._bundle_service.reply(self.id, socket_id, {'target_info': target_info})
                else:
                    try:
                        final_path = get_target_path(run_state.bundle_path, bundle_uuid, path)
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
        with synchronized(self):
            run_state = self._get_run(uuid)
            if run_state is not None:
                run_state.is_killed = True
                run_state.info['kill_message'] = 'Kill requested'

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

    def _handle_kill(self, run_state):
        if run_state.is_killed() and run_state.container_id is not None:
            try:
                self._docker.kill_container(run_state.container_id)
            except DockerException:
                traceback.print_exc()

    def _process_runs(self):
        """ Transition each run then filter out finished runs """
        with synchronized(self):
            # transition all runs
            for bundle_uuid in self._runs.keys():
                run_state = self._runs[bundle_uuid]
                self._runs[bundle_uuid] = self._transition_run_state(run_state)

            # filter out finished runs
            self._runs = {k: v for k, v in self._runs.items() if v.stage != RunStage.FINISHED}

    def _transition_run_state(self, run_state):
        stage = run_state.stage.upper()
        return getattr(self, '_transition_run_state_from_' + stage)(run_state)

    def _transition_run_state_from_STARTING(self, run_state):
        status_message = ''
        all_ready = True
        for entry in run_state.bundle['dependencies']:
            dependency = (entry['parent_uuid'], entry['parent_path'])
            dependency_state = self._dependency_manager.get(dependency)
            if not all_ready:
                pass # already got run_status
            elif dependency_state.stage == DependencyStage.DOWNLOADING:
                all_ready = False
                status_message = 'Downloading dependency %s: %s done (archived size)' % (
                            entry['child_path'], size_str(dependency_state.size_bytes))
            elif dependency_state.stage == DependencyStage.FAILED:
                # Failed to download dependency; -> FINALIZING
                failure_message = 'Failed to download dependency %s: %s' % (
                        entry['child_path'], '') #TODO

                run_state.info['failure_message'] = failure_message
                return run_state._replace(stage=RunStage.FINALIZING, info=run_state.info)

        image_state = self._image_manager.get(run_state.resources['docker_image'])
        if not all_ready:
            pass # already got run_status
        elif image_state.stage == DependencyStage.DOWNLOADING:
            all_ready = False
            status_message = 'Pulling docker image: ' + image_state.message
        elif image_state.stage == DependencyStage.FAILED:
            # Failed to pull image; -> FINALIZING
            run_state.info['failure_message'] = image_state.message
            return run_state._replace(stage=RunStage.FINALIZING, info=run_state.info)

        bundle_uuid = run_state.bundle['uuid']
        if not all_ready:
            return run_state._replace(run_status=status_message)

        # All dependencies ready! Set up directories, symlinks, container. Start container.
        # 1) Set up a directory to store the bundle.
        remove_path(run_state.bundle_path)
        os.mkdir(run_state.bundle_path)

        # 2) Set up symlinks
        dependencies = []
        docker_dependencies_path = '/' + run_state.bundle['uuid'] + '_dependencies'
        for dep in run_state.bundle['dependencies']:
            child_path = os.path.normpath(os.path.join(run_state.bundle_path, dep['child_path']))
            if not child_path.startswith(run_state.bundle_path):
                raise Exception('Invalid key for dependency: %s' % (dep['child_path']))

            dependency_path = self._dependency_manager.get(
                        (dep['parent_uuid'], dep['parent_path'])).path
            dependency_path = os.path.join(self._bundles_dir, dependency_path)

            docker_dependency_path = os.path.join(docker_dependencies_path, dep['child_path'])
            os.symlink(docker_dependency_path, child_path)
            dependencies.append((dependency_path, docker_dependency_path))

        # 3) Set up container
        if run_state.resources['request_network']:
            docker_network = self.docker_network_external_name
        else:
            docker_network = self.docker_network_internal_name

        resources = run_state.resources
        cpuset, gpuset = self._assign_cpu_and_gpu_sets(
                resources['request_cpus'], resources['request_gpus'])

        # 4) Start container
        container_id = self._docker.start_container(
            run_state.bundle_path, run_state.bundle['uuid'], run_state.bundle['command'],
            resources['docker_image'], docker_network, dependencies,
            cpuset, gpuset, resources['request_memory']
        )

        digest = self._docker.get_image_repo_digest(resources['docker_image'])
        ##self._safe_update_docker_image(digest)
        #self._image_manager.touch_image(digest)

        bundle_uuid = run_state.bundle['uuid']
        return run_state._replace(stage=RunStage.RUNNING, run_status='Running',
            container_id=container_id, docker_image=digest, cpuset=cpuset, gpuset=gpuset)

    def _transition_run_state_from_RUNNING(self, run_state):
        bundle_uuid = run_state.bundle['uuid']

        def check_and_report_finished(run_state):
            try:
                finished, exitcode, failure_msg = self._docker.check_finished(run_state.container_id)
            except DockerException:
                traceback.print_exc()
                finished, exitcode, failure_msg = False, None, None
            return dict(finished=finished, exitcode=exitcode, failure_message=failure_msg)

        new_info = check_and_report_finished(run_state)
        run_state.info.update(new_info)
        run_state = run_state._replace(info=run_state.info)
        if run_state.info['finished']:
            return run_state._replace(stage=RunStage.UPLOADING_RESULTS, run_status='Uploading results')
            logger.debug('Finished run with UUID %s, exitcode %s, failure_message %s',
                 bundle_uuid, run_state.info['exitcode'], run_state.info['failure_message'])
        else:
            return run_state #._replace(run_status='Running')

    def _transition_run_state_from_UPLOADING_RESULTS(self, run_state):
        bundle_uuid = run_state.bundle['uuid']
        def upload_results():
            try:
                # Delete the container.
                if run_state.container_id is not None:
                    while True:
                        try:
                            finished, _, _ = self._docker.check_finished(run_state.container_id)
                            if finished:
                                self._docker.delete_container(run_state.container_id)
                                break
                        except DockerException:
                            traceback.print_exc()
                            time.sleep(1)

                # Clean-up dependencies.
                for dep in run_state.bundle['dependencies']:
                    #self._dependency_manager.remove_dependency(
                    #    dep['parent_uuid'], dep['parent_path'], bundle_uuid)

                    # Clean-up the symlinks we created.
                    child_path = os.path.join(run_state.bundle_path, dep['child_path'])
                    remove_path(child_path)

                # Upload results
                logger.debug('Uploading results for run with UUID %s', bundle_uuid)
                def update_status(bytes_uploaded):
                    run_status = 'Uploading results: %s done (archived size)' % size_str(bytes_uploaded)
                    with synchronized(self):
                        self._uploading[bundle_uuid]['run_status'] = run_status

                self._execute_bundle_service_command_with_retry(
                    lambda: self._bundle_service.update_bundle_contents(
                        self.id, bundle_uuid, run_state.bundle_path, update_status))
            except Exception:
                traceback.print_exc()

        if bundle_uuid not in self._uploading:
            self._uploading[bundle_uuid] = {
                'thread': threading.Thread(target=upload_results, args=[]),
                'run_status': ''
            }
            self._uploading[bundle_uuid]['thread'].start()

        if self._uploading[bundle_uuid]['thread'].is_alive():
            return run_state._replace(run_status=self._uploading[bundle_uuid]['run_status'])
        else: # thread finished
            return run_state._replace(stage=RunStage.FINALIZING, container_id=None)

    def _transition_run_state_from_FINALIZING(self, run_state):
        bundle_uuid = run_state.bundle['uuid']
        def finalize():
            try:
                logger.debug('Finalizing run with UUID %s', bundle_uuid)
                failure_message = run_state.info.get('failure_message', None)
                exitcode = run_state.info.get('exitcode', None)
                if failure_message is None and run_state.is_killed:
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
        else: # thread finished
            return run_state._replace(stage=RunStage.FINISHED, run_status='Finished')
        return run_state

    def _transition_run_state_from_FINISHED(self, run_state):
        return run_state

class RunStage(object):
    '''
    Defines the finite set of possible stages and transition functions
    Note that it is important that each state be able to be re-executed
    without unintended adverse effects (which happens upon worker resume)
    '''

    # (a) get each dep, if all ready, proceed to (b) else -> STARTING
    # (b) recreate directories, symlinks and container. Start container -> RUNNING
    STARTING = 'STARTING'

    # if container finished -> UPLOADING_RESULTS else -> RUNNING
    RUNNING = 'RUNNING'

    # if not in _uploading, create and start _uploading thread.
    # (thread deletes container, uploads results and cleans up symlinks)
    # if thread still alive -> UPLOADING_RESULTS, else -> FINALIZING
    UPLOADING_RESULTS = 'UPLOADING_RESULTS'

    # if not in _finalizing, create and start _finalizing thread.
    # (thread finalizes bundle)
    # if thread still alive -> FINISHED, else -> FINALIZING
    FINALIZING = 'FINALIZING'

    # -> FINISHED
    FINISHED = 'FINISHED'
