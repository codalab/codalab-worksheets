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
        self.add_transition(LocalRunStage.STARTING, self._transition_from_STARTING)
        self.add_transition(LocalRunStage.RUNNING, self._transition_from_RUNNING)
        self.add_transition(LocalRunStage.UPLOADING_RESULTS, self._transition_from_UPLOADING_RESULTS)
        self.add_transition(LocalRunStage.FINALIZING, self._transition_from_FINALIZING)
        self.add_transition(LocalRunStage.FINISHED, self._transition_from_FINISHED)

    def _transition_from_STARTING(self, run_state):
        if run_state.is_killed:
            return run_state._replace(stage=LocalRunStage.FINALIZING, container_id=None)
        # first attempt to get() every dependency/image so that downloads start in parallel
        for dep in run_state.bundle['dependencies']:
            dependency = (dep['parent_uuid'], dep['parent_path'])
            dependency_state = self._run_manager.dependency_manager.get(run_state.bundle['uuid'], dependency)
        docker_image = run_state.resources['docker_image']
        image_state = self._run_manager.image_manager.get(docker_image)

        # then inspect the state of every dependency/image to see whether all of them are ready
        for dep in run_state.bundle['dependencies']:
            dependency = (dep['parent_uuid'], dep['parent_path'])
            dependency_state = self._run_manager.dependency_manager.get(run_state.bundle['uuid'], dependency)
            if dependency_state.stage == DependencyStage.DOWNLOADING:
                status_message = 'Downloading dependency %s: %s done (archived size)' % (
                            dep['child_path'], size_str(dependency_state.size_bytes))
                return run_state._replace(run_status=status_message)
            elif dependency_state.stage == DependencyStage.FAILED:
                # Failed to download dependency; -> FINALIZING
                run_state.info['failure_message'] = 'Failed to download dependency %s: %s' % (
                        dep['child_path'], '') #TODO: get more specific message
                return run_state._replace(stage=LocalRunStage.FINALIZING, info=run_state.info)

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
                        bundle_uuid,
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

        if run_state.is_killed and run_state.container_id is not None:
            try:
                self._run_manager.docker.kill_container(run_state.container_id)
            except DockerException:
                traceback.print_exc()
            return run_state._replace(stage=LocalRunStage.FINALIZING, container_id=None)
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
                    failure_message = self._run_manager.get_kill_message()
                finalize_message = {
                    'exitcode': exitcode,
                    'failure_message': failure_message,
                }
                self._run_manager.finalize_bundle(bundle_uuid, finalize_message)
            except Exception:
                traceback.print_exc()
            finally:
                # Clean-up dependencies.
                for dep in run_state.bundle['dependencies']:
                    self._run_manager.dependency_manager.release(
                        bundle_uuid, (dep['parent_uuid'], dep['parent_path']))

                    # Clean-up the symlinks we created.
                    child_path = os.path.join(run_state.bundle_path, dep['child_path'])
                    try:
                        remove_path(child_path)
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
