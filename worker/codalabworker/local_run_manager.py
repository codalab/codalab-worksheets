from contextlib import closing
from collections import namedtuple
import httplib
import logging
import os
from subprocess import check_output
import threading
import time
import traceback

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
from run_manager import BaseRunManager, Reader, RunState

from fsm import (
    DependencyStage,
    StateTransitioner
)

logger = logging.getLogger(__name__)

class LocalReader(Reader):
    def _threaded_read(self, run_state, path, stream_fn, reply_fn):
        try:
            final_path = get_target_path(run_state.bundle_path, run_state.bundle['uuid'], path)
        except PathException as e:
            reply_fn((httplib.BAD_REQUEST, e.message), None, None)
        threading.Thread(target=stream_fn, args=(final_path)).start()

    def get_target_info(self, run_state, path, dep_paths, args, reply_fn):
        bundle_uuid = run_state.bundle['uuid']
        # At the top-level directory, we should ignore dependencies.
        if path and os.path.normpath(path) in dep_paths:
            target_info = None
        else:
            try:
                target_info = get_target_info(
                    run_state.bundle_path, bundle_uuid, path, args['depth'])
            except PathException as e:
                err = (httplib.BAD_REQUEST, e.message)
                reply_fn(err, None, None)

            if target_info is not None and not path and args['depth'] > 0:
                target_info['contents'] = [
                    child for child in target_info['contents']
                    if child['name'] not in dep_paths]

        reply_fn(None, {'target_info': target_info}, None)

    def stream_directory(self, run_state, path, dep_paths, args, reply_fn):
        exclude_names = [] if path else dep_paths
        def stream_thread(final_path):
            with closing(tar_gzip_directory(final_path, exclude_names=exclude_names)) as fileobj:
                reply_fn(None, {}, fileobj)
        self._threaded_read(run_state, path, stream_thread, reply_fn)

    def stream_file(self, run_state, path, dep_paths, args, reply_fn):
        def stream_file(final_path):
            with closing(gzip_file(final_path)) as fileobj:
                reply_fn(None, {}, fileobj)
        self._threaded_read(run_state, path, stream_file, reply_fn)

    def read_file_section(self, run_state, path, dep_paths, args, reply_fn):
        def read_file_section_thread(final_path):
            string = gzip_string(read_file_section(
                final_path, args['offset'], args['length']))
            reply_fn(None, {}, string)
        self._threaded_read(run_state, path, read_file_section_thread, reply_fn)

    def summarize_file(self, run_state, path, dep_paths, args, reply_fn):
        def summarize_file_thread(final_path):
            string = gzip_string(summarize_file(
                final_path, args['num_head_lines'],
                args['num_tail_lines'], args['max_line_length'],
                args['truncation_text']))
            reply_fn(None, {}, string)
        self._threaded_read(run_state, path, summarize_file_thread, reply_fn)

class LocalRunStage(object):
    """
    Defines the finite set of possible stages and transition functions
    Note that it is important that each state be able to be re-executed
    without unintended adverse effects (which happens upon worker resume)
    """

    """
    Starting should encompass any state of a run where the actual user
    submitted job isn't running yet, but the worker is working on it
    (setting up dependencies, setting up local filesystem, setting up a job
    submission to a shared compute queue)
    """
    STARTING = 'STARTING'

    """
    Running encompasses the state where the user's job is running
    """
    RUNNING = 'RUNNING'

    """
    Uploading results means the job's results are getting uploaded to the server
    """
    UPLOADING_RESULTS = 'UPLOADING_RESULTS'

    """
    Finalizing means the worker is finalizing the bundle metadata with the server
    """
    FINALIZING = 'FINALIZING'

    """
    Finished means the worker is done with this run
    """
    FINISHED = 'FINISHED'

class LocalRunStateMachine(StateTransitioner):
    """
    Manages the state machine of the runs running on the local machine
    """

    def __init__(self, run_manager):
        super(LocalRunStateMachine, self).__init__()
        self._run_manager = run_manager
        self.add_check(self._handle_kill)
        self.add_transition(LocalRunStage.STARTING, self._transition_from_STARTING)
        self.add_transition(LocalRunStage.RUNNING, self._transition_from_RUNNING)
        self.add_transition(LocalRunStage.UPLOADING_RESULTS, self._transition_from_UPLOADING_RESULTS)
        self.add_transition(LocalRunStage.FINALIZING, self._transition_from_FINALIZING)
        self.add_transition(LocalRunStage.FINISHED, self._transition_from_FINISHED)

    def _handle_kill(self, run_state):
        bundle_uuid = run_state.bundle['uuid']
        if bundle_uuid in self._run_manager.uploading or bundle_uuid in self._run_manager.finalizing:
            # TODO: Can't kill uploading/finalizing bundles
            return run_state

        if run_state.is_killed() and run_state.container_id is not None:
            try:
                self._run_manager._docker.kill_container(run_state.container_id)
            except DockerException:
                traceback.print_exc()
            return run_state._replace(stage=LocalRunStage.FINALIZING, container_id=None)

    def _transition_from_STARTING(self, run_state):
        # first attempt to get() every dependency/image so that downloads start in parallel
        for dep in run_state.bundle['dependencies']:
            dependency = (dep['parent_uuid'], dep['parent_path'])
            dependency_state = self._run_manager.dependency_manager.get(dependency)
        docker_image = run_state.resources['docker_image']
        image_state = self._run_manager.image_manager.get(docker_image)

        # then inspect the state of every dependency/image to see whether all of them are ready
        for dep in run_state.bundle['dependencies']:
            dependency = (dep['parent_uuid'], dep['parent_path'])
            dependency_state = self._run_manager.dependency_manager.get(dependency)
            if dependency_state.stage == DependencyStage.DOWNLOADING:
                status_message = 'Downloading dependency %s: %s done (archived size)' % (
                            dep['child_path'], size_str(dependency_state.size_bytes))
                return run_state._replace(run_status=status_message)
            elif dependency_state.stage == DependencyStage.FAILED:
                # Failed to download dependency; -> FINALIZING
                run_state.info['failure_message'] = 'Failed to download dependency %s: %s' % (
                        dep['child_path'], '') #TODO: get more specific message
                return run_state._replace(stage=LocalRunStage.FINALIZING, info=run_state.info)

        docker_image = run_state.resources['docker_image']
        image_state = self._run_manager.image_manager.get(docker_image)
        if image_state.stage == DependencyStage.DOWNLOADING:
            status_message = 'Pulling docker image: ' + (image_state.message or docker_image)
            return run_state._replace(run_status=status_message)
        elif image_state.stage == DependencyStage.FAILED:
            # Failed to pull image; -> FINALIZING
            run_state.info['failure_message'] = image_state.message
            return run_state._replace(stage=LocalRunStage.FINALIZING, info=run_state.info)

        # All dependencies ready! Set up directories, symlinks and container. Start container.
        # 1) Set up a directory to store the bundle.
        remove_path(run_state.bundle_path)
        os.mkdir(run_state.bundle_path)

        # 2) Set up symlinks
        bundle_uuid = run_state.bundle['uuid']
        dependencies = []
        docker_dependencies_path = '/' + bundle_uuid + '_dependencies'
        for dep in run_state.bundle['dependencies']:
            child_path = os.path.normpath(os.path.join(run_state.bundle_path, dep['child_path']))
            if not child_path.startswith(run_state.bundle_path):
                raise Exception('Invalid key for dependency: %s' % (dep['child_path']))

            dependency_path = self._run_manager.dependency_manager.get(
                        (dep['parent_uuid'], dep['parent_path'])).path
            dependency_path = os.path.join(self._run_manager.bundles_dir, dependency_path)

            docker_dependency_path = os.path.join(docker_dependencies_path, dep['child_path'])
            os.symlink(docker_dependency_path, child_path)
            dependencies.append((dependency_path, docker_dependency_path))

        # 3) Set up container
        if run_state.resources['request_network']:
            docker_network = self._run_manager.docker_network_external_name
        else:
            docker_network = self._run_manager.docker_network_internal_name

        cpuset, gpuset = self._run_manager.assign_cpu_and_gpu_sets(
                run_state.resources['request_cpus'], run_state.resources['request_gpus'])

        # 4) Start container
        container_id = self._run_manager.docker.start_container(
            run_state.bundle_path, bundle_uuid, run_state.bundle['command'],
            run_state.resources['docker_image'], docker_network, dependencies,
            cpuset, gpuset, run_state.resources['request_memory']
        )

        digest = self._run_manager.docker.get_image_repo_digest(run_state.resources['docker_image'])

        return run_state._replace(stage=LocalRunStage.RUNNING, run_status='Running',
            container_id=container_id, docker_image=digest, cpuset=cpuset, gpuset=gpuset)

    def _transition_from_RUNNING(self, run_state):
        def check_and_report_finished(run_state):
            try:
                finished, exitcode, failure_msg = self._run_manager.docker.check_finished(run_state.container_id)
            except DockerException:
                traceback.print_exc()
                finished, exitcode, failure_msg = False, None, None
            return dict(finished=finished, exitcode=exitcode, failure_message=failure_msg)

        bundle_uuid = run_state.bundle['uuid']

        new_info = check_and_report_finished(run_state)
        run_state.info.update(new_info)
        run_state = run_state._replace(info=run_state.info)
        if run_state.info['finished']:
            logger.debug('Finished run with UUID %s, exitcode %s, failure_message %s',
                 bundle_uuid, run_state.info['exitcode'], run_state.info['failure_message'])
            return run_state._replace(stage=LocalRunStage.UPLOADING_RESULTS, run_status='Uploading results')
        else:
            return run_state

    def _transition_from_UPLOADING_RESULTS(self, run_state):
        def upload_results():
            try:
                # Delete the container.
                if run_state.container_id is not None:
                    while True:
                        try:
                            finished, _, _ = self._run_manager.docker.check_finished(run_state.container_id)
                            if finished:
                                self._run_manager.docker.delete_container(run_state.container_id)
                                break
                        except DockerException:
                            traceback.print_exc()
                            time.sleep(1)

                # Clean-up dependencies.
                for dep in run_state.bundle['dependencies']:
                    #self._run_manager.dependency_manager.remove_dependency(
                    #    dep['parent_uuid'], dep['parent_path'], bundle_uuid)

                    # Clean-up the symlinks we created.
                    child_path = os.path.join(run_state.bundle_path, dep['child_path'])
                    remove_path(child_path)

                # Upload results
                logger.debug('Uploading results for run with UUID %s', bundle_uuid)
                def update_status(bytes_uploaded):
                    run_status = 'Uploading results: %s done (archived size)' % size_str(bytes_uploaded)
                    with self._run_manager.lock:
                        self._run_manager.uploading[bundle_uuid]['run_status'] = run_status

                self._run_manager.upload_bundle_contents(bundle_uuid, run_state.bundle_path, update_status)
            except Exception:
                traceback.print_exc()

        bundle_uuid = run_state.bundle['uuid']
        if bundle_uuid not in self._run_manager.uploading:
            self._run_manager.uploading[bundle_uuid] = {
                'thread': threading.Thread(target=upload_results, args=[]),
                'run_status': ''
            }
            self._run_manager.uploading[bundle_uuid]['thread'].start()

        if self._run_manager.uploading[bundle_uuid]['thread'].is_alive():
            return run_state._replace(run_status=self._run_manager.uploading[bundle_uuid]['run_status'])
        else: # thread finished
            del self._run_manager.uploading[bundle_uuid]
            return run_state._replace(stage=LocalRunStage.FINALIZING, container_id=None)

    def _transition_from_FINALIZING(self, run_state):
        def finalize():
            try:
                logger.debug('Finalizing run with UUID %s', bundle_uuid)
                failure_message = run_state.info.get('failure_message', None)
                exitcode = run_state.info.get('exitcode', None)
                if failure_message is None and run_state.is_killed:
                    failure_message = self._run_manager._get_kill_message()
                finalize_message = {
                    'exitcode': exitcode,
                    'failure_message': failure_message,
                }
                self._run_manager._finalize_bundle(bundle_uuid, finalize_message)
            except Exception:
                traceback.print_exc()

        bundle_uuid = run_state.bundle['uuid']
        if bundle_uuid not in self._run_manager.finalizing:
            self._run_manager.finalizing[bundle_uuid] = {
                'thread': threading.Thread(target=finalize, args=[]),
            }
            self._run_manager.finalizing[bundle_uuid]['thread'].start()

        if self._run_manager.finalizing[bundle_uuid]['thread'].is_alive():
            return run_state
        else: # thread finished
            del self._run_manager.finalizing[bundle_uuid]['thread']
            return run_state._replace(stage=LocalRunStage.FINISHED, run_status='Finished')

    def _transition_from_FINISHED(self, run_state):
        return run_state

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
        self._init_docker_networks()

    def _init_docker_networks(self):
        # set up docker networks for runs: one with external network access and one without
        self.docker_network_external_name = self._docker_network_prefix + "_ext"
        if self.docker_network_external_name not in self._docker.list_networks():
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
        with self._lock:
            # transition all runs
            for bundle_uuid in self._runs.keys():
                run_state = self._runs[bundle_uuid]
                self._runs[bundle_uuid] = self._run_state_manager.transition(run_state)

            # filter out finished runs
            self._runs = {k: v for k, v in self._runs.items() if v.stage != LocalRunStage.FINISHED}

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

        with self._lock:
            for run_state in self._runs.values():
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
        with self._lock:
            return self._runs.get(uuid, None)

    def finalize_bundle(self, bundle_uuid, finalize_message):
        self._worker.finalize_bundle(bundle_uuid, finalize_message)

    def upload_bundle_contents(self, bundle_uuid, bundle_path, update_status):
        self._worker.upload_bundle_contents(bundle_uuid, bundle_path, update_status)

    def read(self, run_state, path, dep_paths, args, reply):
        self.reader.read(run_state, path, dep_paths, args, reply)

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
        with self._lock:
            run_state.is_killed = True
            run_state.info['kill_message'] = 'Kill requested'

    @property
    def all_runs(self):
        """
        Returns a list of all the runs managed by this RunManager
        """
        with self._lock:
            result = {
                bundle_uuid: {
                    'run_status': run_state.run_status,
                    'start_time': run_state.start_time,
                    'docker_image': run_state.docker_image,
                    'info': run_state.info
                } for bundle_uuid, run_state in self._runs.items()
            }
            return result

    @property
    def all_dependencies(self):
        """
        Returns a list of all dependencies available in this RunManager
        """
        return self._dependency_manager.list_all()

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
        info = self.docker.get_nvidia_devices_info()
        count = 0 if info is None else len(info['Devices'])
        return count

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
