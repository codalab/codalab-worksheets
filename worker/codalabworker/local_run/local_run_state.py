from collections import namedtuple
import logging
import os
import threading
import time
import traceback

from codalabworker.docker_client import DockerException
from codalabworker.file_util import remove_path
from codalabworker.formatting import size_str
from codalabworker.fsm import (
    DependencyStage,
    StateTransitioner
)

logger = logging.getLogger(__name__)


class LocalRunStage(object):
    """
    Defines the finite set of possible stages and transition functions
    Note that it is important that each state be able to be re-executed
    without unintended adverse effects (which happens upon worker resume)
    """

    """
    This stage involves setting up the directory structure for the run
    and preapring to start the container
    """
    PREPARING = 'LOCAL_RUN.PREPARING'

    """
    Running encompasses the state where the user's job is running
    """
    RUNNING = 'LOCAL_RUN.RUNNING'

    """
    This stage encompasses cleaning up intermediary components like
    the dependency symlinks and also the releasing of dependencies
    """
    CLEANING_UP = 'LOCAL_RUN.CLEANING_UP'

    """
    Uploading results means the job's results are getting uploaded to the server
    """
    UPLOADING_RESULTS = 'LOCAL_RUN.UPLOADING_RESULTS'

    """
    Finalizing means the worker is finalizing the bundle metadata with the server
    """
    FINALIZING = 'LOCAL_RUN.FINALIZING'

    """
    Finished means the worker is done with this run
    """
    FINISHED = 'LOCAL_RUN.FINISHED'


LocalRunState = namedtuple(
    'RunState',
    ['stage', 'run_status', 'bundle', 'bundle_path', 'resources', 'start_time',
     'container_id', 'docker_image', 'is_killed', 'has_contents', 'cpuset',
     'gpuset', 'info'])


class LocalRunStateMachine(StateTransitioner):
    """
    Manages the state machine of the runs running on the local machine
    """

    def __init__(self, run_manager):
        super(LocalRunStateMachine, self).__init__()
        self._run_manager = run_manager
        self.add_transition(LocalRunStage.PREPARING, self._transition_from_PREPARING)
        self.add_transition(LocalRunStage.RUNNING, self._transition_from_RUNNING)
        self.add_transition(LocalRunStage.CLEANING_UP, self._transition_from_CLEANING_UP)
        self.add_transition(LocalRunStage.UPLOADING_RESULTS, self._transition_from_UPLOADING_RESULTS)
        self.add_transition(LocalRunStage.FINALIZING, self._transition_from_FINALIZING)
        self.add_terminal(LocalRunStage.FINISHED)

    def _transition_from_PREPARING(self, run_state):
        """
        1- Request the docker image from docker image manager
            - if image is failed, move to CLEANING_UP state
        2- Request the dependencies from dependency manager
            - if any are failed, move to CLEANING_UP state
        3- If all dependencies and docker image are ready:
            - Set up the local filesystem for the run
            - Create symlinks to dependencies
            - Allocate resources and prepare the docker container
            - Start the docker container
        4- If all is successful, move to RUNNING state
        """
        if run_state.is_killed:
            return run_state._replace(stage=LocalRunStage.CLEANING_UP, container_id=None)

        dependencies_ready = True
        status_messages = []

        # get dependencies
        for dep in run_state.bundle['dependencies']:
            dependency = (dep['parent_uuid'], dep['parent_path'])
            dependency_state = self._run_manager.dependency_manager.get(run_state.bundle['uuid'], dependency)
            if dependency_state.stage == DependencyStage.DOWNLOADING:
                status_messages.append('Downloading dependency %s: %s done (archived size)' % (
                    dep['child_path'], size_str(dependency_state.size_bytes)))
                dependencies_ready = False
            elif dependency_state.stage == DependencyStage.FAILED:
                # Failed to download dependency; -> CLEANING_UP
                run_state.info['failure_message'] = 'Failed to download dependency %s: %s' % (
                    dep['child_path'], '')
                return run_state._replace(stage=LocalRunStage.CLEANING_UP, info=run_state.info)

        # get the docker image
        docker_image = run_state.resources['docker_image']
        image_state = self._run_manager.image_manager.get(docker_image)
        if image_state.stage == DependencyStage.DOWNLOADING:
            status_messages.append('Pulling docker image: ' + (image_state.message or docker_image))
            dependencies_ready = False
        elif image_state.stage == DependencyStage.FAILED:
            # Failed to pull image; -> CLEANING_UP
            run_state.info['failure_message'] = image_state.message
            return run_state._replace(stage=LocalRunStage.CLEANING_UP, info=run_state.info)

        # stop proceeding if dependency and image downloads aren't all done
        if not dependencies_ready:
            status_message = status_messages.pop()
            if status_messages:
                status_message += "(and downloading %d others)" % len(status_messages)
            return run_state._replace(run_status=status_message)

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
                bundle_uuid,
                (dep['parent_uuid'], dep['parent_path'])).path
            dependency_path = os.path.join(self._run_manager.dependencies_dir, dependency_path)

            docker_dependency_path = os.path.join(docker_dependencies_path, dep['child_path'])

            os.symlink(docker_dependency_path, child_path)
            # These are turned into docker volume bindings like:
            #   dependency_path:docker_dependency_path:ro
            dependencies.append((dependency_path, docker_dependency_path))

        # 3) Set up container
        if run_state.resources['request_network']:
            docker_network = self._run_manager.docker_network_external_name
        else:
            docker_network = self._run_manager.docker_network_internal_name

        try:
            cpuset, gpuset = self._run_manager.assign_cpu_and_gpu_sets(
                run_state.resources['request_cpus'], run_state.resources['request_gpus'])
        except:
            run_state.info['failure_message'] = "Cannot assign enough resources"
            return run_state._replace(stage=LocalRunStage.CLEANING_UP, info=run_state.info)

        # 4) Start container
        container_id = self._run_manager.docker.start_container(
            run_state.bundle_path, bundle_uuid, run_state.bundle['command'],
            run_state.resources['docker_image'], docker_network, dependencies,
            cpuset, gpuset, run_state.resources['request_memory']
        )

        digest = self._run_manager.docker.get_image_repo_digest(run_state.resources['docker_image'])

        return run_state._replace(stage=LocalRunStage.RUNNING,
                                  run_status='Running job in Docker container',
                                  container_id=container_id,
                                  docker_image=digest,
                                  has_contents=True,
                                  cpuset=cpuset,
                                  gpuset=gpuset)

    def _transition_from_RUNNING(self, run_state):
        """
        1- Check run status of the docker container
        2- If run is killed, kill the container
        3- If run is finished, move to CLEANING_UP state
        """
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

        if run_state.is_killed and run_state.container_id is not None:
            try:
                self._run_manager.docker.kill_container(run_state.container_id)
            except DockerException:
                traceback.print_exc()
            return run_state._replace(stage=LocalRunStage.CLEANING_UP, container_id=None)
        if run_state.info['finished']:
            logger.debug('Finished run with UUID %s, exitcode %s, failure_message %s',
                         bundle_uuid, run_state.info['exitcode'], run_state.info['failure_message'])
            return run_state._replace(stage=LocalRunStage.CLEANING_UP, run_status='Uploading results')
        else:
            return run_state

    def _transition_from_CLEANING_UP(self, run_state):
        """
        1- delete the container if still existent
        2- clean up the dependencies from bundle folder
        3- release the dependencies in dependency manager
        4- If bundle has contents to upload (i.e. was RUNNING at some point),
            move to UPLOADING_RESULTS state
           Otherwise move to FINALIZING state
        """
        bundle_uuid = run_state.bundle['uuid']
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

        for dep in run_state.bundle['dependencies']:
            self._run_manager.dependency_manager.release(
                bundle_uuid, (dep['parent_uuid'], dep['parent_path']))

            child_path = os.path.join(run_state.bundle_path, dep['child_path'])
            try:
                remove_path(child_path)
            except Exception:
                traceback.print_exc()

        if run_state.has_contents:
            return run_state._replace(stage=LocalRunStage.UPLOADING_RESULTS, run_status='Uploading results')
        else:
            return run_state._replace(stage=LocalRunStage.FINALIZING, run_status='Finalizing bundle')

    def _transition_from_UPLOADING_RESULTS(self, run_state):
        """
        If bundle not already uploading:
            Use the RunManager API to upload contents at bundle_path to the server
            Pass the callback to that API such that if the bundle is killed during the upload,
            the callback returns false, allowing killable uploads.
        If uploading and not finished:
            Update run_status with upload progress
        If uploading and finished:
            Move to FINALIZING state
        """

        def upload_results():
            try:
                # Upload results
                logger.debug('Uploading results for run with UUID %s', bundle_uuid)

                def progress_callback(bytes_uploaded):
                    run_status = 'Uploading results: %s done (archived size)' % size_str(bytes_uploaded)
                    with self._run_manager.lock:
                        self._run_manager.uploading[bundle_uuid]['run_status'] = run_status
                        return not self._run_manager.runs[bundle_uuid].is_killed

                self._run_manager.upload_bundle_contents(bundle_uuid, run_state.bundle_path, progress_callback)
            except Exception:
                traceback.print_exc()

        bundle_uuid = run_state.bundle['uuid']
        self._run_manager.uploading.add_if_new(bundle_uuid, threading.Thread(target=upload_results, args=[]))

        if self._run_manager.uploading[bundle_uuid].is_alive():
            return run_state._replace(run_status=self._run_manager.uploading[bundle_uuid]['run_status'])
        else:  # thread finished
            self._run_manager.uploading.remove(bundle_uuid)
            return run_state._replace(stage=LocalRunStage.FINALIZING, container_id=None, run_status='Finalizing bundle')

    def _transition_from_FINALIZING(self, run_state):
        """
        Use the RunManager API to make a finalize call to the server
        """
        def finalize():
            try:
                logger.debug('Finalizing run with UUID %s', bundle_uuid)
                failure_message = run_state.info.get('failure_message', None)
                exitcode = run_state.info.get('exitcode', None)
                if failure_message is None and run_state.is_killed:
                    failure_message = run_state.info['kill_message']
                finalize_message = {
                    'exitcode': exitcode,
                    'failure_message': failure_message,
                }
                self._run_manager.finalize_bundle(bundle_uuid, finalize_message)
            except Exception:
                traceback.print_exc()

        bundle_uuid = run_state.bundle['uuid']
        self._run_manager.finalizing.add_if_new(bundle_uuid, threading.Thread(target=finalize, args=[]))

        if self._run_manager.finalizing[bundle_uuid].is_alive():
            return run_state
        else:  # thread finished
            self._run_manager.finalizing.remove(bundle_uuid)
            return run_state._replace(stage=LocalRunStage.FINISHED, run_status='Finished')
