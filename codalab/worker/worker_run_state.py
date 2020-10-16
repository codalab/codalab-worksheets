import docker
import glob
import logging
import os
import threading
import time
import traceback

import codalab.worker.docker_utils as docker_utils

from collections import namedtuple
from pathlib import Path

from codalab.lib.formatting import size_str, duration_str
from codalab.worker.file_util import remove_path, get_path_size, path_is_parent
from codalab.worker.bundle_state import State, DependencyKey
from codalab.worker.fsm import DependencyStage, StateTransitioner
from codalab.worker.worker_thread import ThreadDict

logger = logging.getLogger(__name__)


class RunStage(object):
    """
    Defines the finite set of possible stages and transition functions
    Note that it is important that each state be able to be re-executed
    without unintended adverse effects (which happens upon worker resume)
    """

    WORKER_STATE_TO_SERVER_STATE = {}

    """
    This stage involves setting up the directory structure for the run
    and preparing to start the container
    """
    PREPARING = 'RUN_STAGE.PREPARING'
    WORKER_STATE_TO_SERVER_STATE[PREPARING] = State.PREPARING

    """
    Running encompasses the state where the user's job is running
    """
    RUNNING = 'RUN_STAGE.RUNNING'
    WORKER_STATE_TO_SERVER_STATE[RUNNING] = State.RUNNING

    """
    This stage encompasses cleaning up intermediary components like
    the dependency symlinks and also the releasing of dependencies
    """
    CLEANING_UP = 'RUN_STAGE.CLEANING_UP'
    WORKER_STATE_TO_SERVER_STATE[CLEANING_UP] = State.RUNNING

    """
    Uploading results means the job's results are getting uploaded to the server
    """
    UPLOADING_RESULTS = 'RUN_STAGE.UPLOADING_RESULTS'
    WORKER_STATE_TO_SERVER_STATE[UPLOADING_RESULTS] = State.RUNNING

    """
    Finalizing means the worker is finalizing the bundle metadata with the server
    """
    FINALIZING = 'RUN_STAGE.FINALIZING'
    WORKER_STATE_TO_SERVER_STATE[FINALIZING] = State.FINALIZING

    """
    Finished means the worker is done with this run
    """
    FINISHED = 'RUN_STAGE.FINISHED'
    WORKER_STATE_TO_SERVER_STATE[FINISHED] = State.READY

    """
    This stage will collect bundles in terminal states and
    sent them back to the server with RESTAGED state
    """
    RESTAGED = 'RUN_STAGE.RESTAGED'
    WORKER_STATE_TO_SERVER_STATE[RESTAGED] = State.STAGED


RunState = namedtuple(
    'RunState',
    [
        'stage',  # RunStage
        'run_status',  # str
        'bundle',  # BundleInfo
        'bundle_path',  # str
        'bundle_dir_wait_num_tries',  # Optional[int]
        'resources',  # RunResources
        'bundle_start_time',  # int
        'container_time_total',  # int
        'container_time_user',  # int
        'container_time_system',  # int
        'container',  # Optional[docker.Container]
        'container_id',  # Optional[str]
        'docker_image',  # Optional[str]
        'is_killed',  # bool
        'has_contents',  # bool
        'cpuset',  # Optional[Set[str]]
        'gpuset',  # Optional[Set[str]]
        'max_memory',  # int
        'disk_utilization',  # int
        'exitcode',  # Optionall[str]
        'failure_message',  # Optional[str]
        'kill_message',  # Optional[str]
        'finished',  # bool
        'finalized',  # bool
        'is_restaged',  # bool
    ],
)

"""Dependency that is mounted.

TODO(Ashwin): document this better

docker_path - path on the Docker container where the dependency is mounted
    example (shared file system):       /0x0fbb927dc0e54544bbc2d439a6805951/foo
    example (non-shared file system):   .../codalab-worksheets/var/codalab/worker/dependencies/0x6b5bfdca99b6423ea36327102b19d0af
child_path - path inside the bundle folder from where the dependency is mounted
    example (shared file system):       .../codalab-worksheets/var/codalab/home/partitions/default/bundles/0x0fbb927dc0e54544bbc2d439a6805951/foo
    example (non-shared file system):   .../codalab-worksheets/var/codalab/home/partitions/default/bundles/0x0fbb927dc0e54544bbc2d439a6805951/foo
parent_path - path of the dependency
    example (shared file system):       /opt/codalab-worksheets/tests/files/a.txt
    example (non-shared file system):   .../codalab-worksheets/var/codalab/worker/dependencies/0x6b5bfdca99b6423ea36327102b19d0af

"""

DependencyToMount = namedtuple('DependencyToMount', 'docker_path, child_path, parent_path')


class RunStateMachine(StateTransitioner):
    """
    Manages the state machine of the runs running on the local machine

    Note that in general there are two types of errors:
    - User errors (fault of bundle) - we fail the bundle (move to CLEANING_UP state).
    - System errors (fault of worker) - we freeze this worker (Exception is thrown up).
    It's not always clear where the line is.
    """

    _ROOT = '/'
    _CURRENT_DIRECTORY = '.'

    def __init__(
        self,
        docker_image_manager,  # Component to request docker images from
        dependency_manager,  # Component to request dependency downloads from
        worker_docker_network,  # Docker network to add all bundles to
        docker_network_internal,  # Docker network to add non-net connected bundles to
        docker_network_external,  # Docker network to add internet connected bundles to
        docker_runtime,  # Docker runtime to use for containers (nvidia or runc)
        upload_bundle_callback,  # Function to call to upload bundle results to the server
        assign_cpu_and_gpu_sets_fn,  # Function to call to assign CPU and GPU resources to each run
        shared_file_system,  # If True, bundle mount is shared with server
    ):
        super(RunStateMachine, self).__init__()
        self.add_transition(RunStage.PREPARING, self._transition_from_PREPARING)
        self.add_transition(RunStage.RUNNING, self._transition_from_RUNNING)
        self.add_transition(RunStage.CLEANING_UP, self._transition_from_CLEANING_UP)
        self.add_transition(RunStage.UPLOADING_RESULTS, self._transition_from_UPLOADING_RESULTS)
        self.add_transition(RunStage.FINALIZING, self._transition_from_FINALIZING)
        self.add_terminal(RunStage.FINISHED)
        self.add_terminal(RunStage.RESTAGED)

        self.dependency_manager = dependency_manager
        self.docker_image_manager = docker_image_manager
        self.worker_docker_network = worker_docker_network
        self.docker_network_external = docker_network_external
        self.docker_network_internal = docker_network_internal
        self.docker_runtime = docker_runtime
        # bundle.uuid -> {'thread': Thread, 'run_status': str}
        self.uploading = ThreadDict(fields={'run_status': 'Upload started', 'success': False})
        # bundle.uuid -> {'thread': Thread, 'disk_utilization': int, 'running': bool}
        self.disk_utilization = ThreadDict(
            fields={'disk_utilization': 0, 'running': True, 'lock': None}
        )
        self.upload_bundle_callback = upload_bundle_callback
        self.assign_cpu_and_gpu_sets_fn = assign_cpu_and_gpu_sets_fn
        self.shared_file_system = shared_file_system
        self.paths_to_remove = []

    def stop(self):
        for uuid in self.disk_utilization.keys():
            self.disk_utilization[uuid]['running'] = False
        self.disk_utilization.stop()
        self.uploading.stop()

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

        def mount_dependency(dependency, shared_file_system):
            if not shared_file_system:
                # Set up symlinks for the content at dependency path
                Path(dependency.child_path).parent.mkdir(parents=True, exist_ok=True)
                os.symlink(dependency.docker_path, dependency.child_path)
            # The following will be converted into a Docker volume binding like:
            #   dependency_path:docker_dependency_path:ro
            docker_dependencies.append((dependency.parent_path, dependency.docker_path))

        if run_state.is_killed or run_state.is_restaged:
            return run_state._replace(stage=RunStage.CLEANING_UP)

        # Check CPU and GPU availability
        try:
            cpuset, gpuset = self.assign_cpu_and_gpu_sets_fn(
                run_state.resources.cpus, run_state.resources.gpus
            )
        except Exception as e:
            message = "Unexpectedly unable to assign enough resources to bundle {}: {}".format(
                run_state.bundle.uuid, str(e)
            )
            logger.error(message)
            logger.error(traceback.format_exc())
            return run_state._replace(run_status=message)

        dependencies_ready = True
        status_messages = []

        if not self.shared_file_system:
            # No need to download dependencies if we're in the shared FS,
            # since they're already in our FS
            for dep in run_state.bundle.dependencies:
                dep_key = DependencyKey(dep.parent_uuid, dep.parent_path)
                dependency_state = self.dependency_manager.get(run_state.bundle.uuid, dep_key)
                if dependency_state.stage == DependencyStage.DOWNLOADING:
                    status_messages.append(
                        'Downloading dependency %s: %s done (archived size)'
                        % (dep.child_path, size_str(dependency_state.size_bytes))
                    )
                    dependencies_ready = False
                elif dependency_state.stage == DependencyStage.FAILED:
                    # Failed to download dependency; -> CLEANING_UP
                    return run_state._replace(
                        stage=RunStage.CLEANING_UP,
                        failure_message='Failed to download dependency %s: %s'
                        % (dep.child_path, dependency_state.message),
                    )

        # get the docker image
        docker_image = run_state.resources.docker_image
        image_state = self.docker_image_manager.get(docker_image)
        if image_state.stage == DependencyStage.DOWNLOADING:
            status_messages.append(
                'Pulling docker image: ' + (image_state.message or docker_image or "")
            )
            dependencies_ready = False
        elif image_state.stage == DependencyStage.FAILED:
            # Failed to pull image; -> CLEANING_UP
            message = 'Failed to download Docker image: %s' % image_state.message
            logger.error(message)
            return run_state._replace(stage=RunStage.CLEANING_UP, failure_message=message)

        # stop proceeding if dependency and image downloads aren't all done
        if not dependencies_ready:
            status_message = status_messages.pop()
            if status_messages:
                status_message += "(and downloading %d other dependencies and docker images)" % len(
                    status_messages
                )
            return run_state._replace(run_status=status_message)

        # All dependencies ready! Set up directories, symlinks and container. Start container.
        # 1) Set up a directory to store the bundle.
        if self.shared_file_system:
            if not os.path.exists(run_state.bundle_path):
                if run_state.bundle_dir_wait_num_tries == 0:
                    message = (
                        "Bundle directory cannot be found on the shared filesystem. "
                        "Please ensure the shared fileystem between the server and "
                        "your worker is mounted properly or contact your administrators."
                    )
                    logger.error(message)
                    return run_state._replace(stage=RunStage.CLEANING_UP, failure_message=message)
                return run_state._replace(
                    run_status="Waiting for bundle directory to be created by the server",
                    bundle_dir_wait_num_tries=run_state.bundle_dir_wait_num_tries - 1,
                )
        else:
            remove_path(run_state.bundle_path)
            os.makedirs(run_state.bundle_path)

        # 2) Set up symlinks
        docker_dependencies = []
        docker_dependencies_path = (
            RunStateMachine._ROOT
            + run_state.bundle.uuid
            + ('_dependencies' if not self.shared_file_system else '')
        )

        for dep in run_state.bundle.dependencies:
            full_child_path = os.path.normpath(os.path.join(run_state.bundle_path, dep.child_path))
            to_mount = []
            dependency_path = self._get_dependency_path(run_state, dep)

            if dep.child_path == RunStateMachine._CURRENT_DIRECTORY:
                # Mount all the content of the dependency_path to the top-level of the bundle
                for child in os.listdir(dependency_path):
                    child_path = os.path.normpath(os.path.join(run_state.bundle_path, child))
                    to_mount.append(
                        DependencyToMount(
                            docker_path=os.path.join(docker_dependencies_path, child),
                            child_path=child_path,
                            parent_path=os.path.join(dependency_path, child),
                        )
                    )
                    self.paths_to_remove.append(child_path)
            else:
                to_mount.append(
                    DependencyToMount(
                        docker_path=os.path.join(docker_dependencies_path, dep.child_path),
                        child_path=full_child_path,
                        parent_path=dependency_path,
                    )
                )

                first_element_of_path = Path(dep.child_path).parts[0]
                if first_element_of_path == RunStateMachine._ROOT:
                    self.paths_to_remove.append(full_child_path)
                else:
                    # child_path can be a nested path, so later remove everything from the first element of the path
                    self.paths_to_remove.append(
                        os.path.join(run_state.bundle_path, first_element_of_path)
                    )
            for dependency in to_mount:
                try:
                    mount_dependency(dependency, self.shared_file_system)
                except OSError as e:
                    return run_state._replace(stage=RunStage.CLEANING_UP, failure_message=str(e))

        if run_state.resources.network:
            docker_network = self.docker_network_external.name
        else:
            docker_network = self.docker_network_internal.name

        # 3) Start container
        try:
            container = docker_utils.start_bundle_container(
                run_state.bundle_path,
                run_state.bundle.uuid,
                docker_dependencies,
                run_state.bundle.command,
                run_state.resources.docker_image,
                network=docker_network,
                cpuset=cpuset,
                gpuset=gpuset,
                memory_bytes=run_state.resources.memory,
                runtime=self.docker_runtime,
            )
            self.worker_docker_network.connect(container)
        except docker_utils.DockerUserErrorException as e:
            message = 'Cannot start Docker container: {}'.format(e)
            logger.warning(message)
            return run_state._replace(stage=RunStage.CLEANING_UP, failure_message=message)
        except Exception as e:
            message = 'Cannot start Docker container: {}'.format(e)
            logger.error(message)
            logger.error(traceback.format_exc())
            raise

        return run_state._replace(
            stage=RunStage.RUNNING,
            run_status='Running job in Docker container',
            container_id=container.id,
            container=container,
            docker_image=image_state.digest,
            has_contents=True,
            cpuset=cpuset,
            gpuset=gpuset,
        )

    def _get_dependency_path(self, run_state, dependency):
        if self.shared_file_system:
            # TODO(Ashwin): make this not fs-specific.
            # On a shared FS, we know where the dependency is stored and can get the contents directly
            return os.path.realpath(os.path.join(dependency.location, dependency.parent_path))
        else:
            # On a dependency_manager setup, ask the manager where the dependency is
            dep_key = DependencyKey(dependency.parent_uuid, dependency.parent_path)
            return os.path.join(
                self.dependency_manager.dependencies_dir,
                self.dependency_manager.get(run_state.bundle.uuid, dep_key).path,
            )

    def _transition_from_RUNNING(self, run_state):
        """
        1- Check run status of the docker container
        2- If run is killed, kill the container
        3- If run is finished, move to CLEANING_UP state
        """

        def check_and_report_finished(run_state):
            try:
                finished, exitcode, failure_msg = docker_utils.check_finished(run_state.container)
            except docker_utils.DockerException:
                logger.error(traceback.format_exc())
                finished, exitcode, failure_msg = False, None, None
            return run_state._replace(
                finished=finished, exitcode=exitcode, failure_message=failure_msg
            )

        def check_resource_utilization(run_state):
            kill_messages = []

            run_stats = docker_utils.get_container_stats(run_state.container)

            run_state = run_state._replace(
                max_memory=max(run_state.max_memory, run_stats.get('memory', 0))
            )
            run_state = run_state._replace(
                disk_utilization=self.disk_utilization[run_state.bundle.uuid]['disk_utilization']
            )

            container_time_total = docker_utils.get_container_running_time(run_state.container)
            run_state = run_state._replace(
                container_time_total=container_time_total,
                container_time_user=run_stats.get(
                    'container_time_user', run_state.container_time_user
                ),
                container_time_system=run_stats.get(
                    'container_time_system', run_state.container_time_system
                ),
            )

            if run_state.resources.time and container_time_total > run_state.resources.time:
                kill_messages.append(
                    'Time limit exceeded. (Container uptime %s > time limit %s)'
                    % (duration_str(container_time_total), duration_str(run_state.resources.time))
                )

            if run_state.max_memory > run_state.resources.memory or run_state.exitcode == 137:
                kill_messages.append(
                    'Memory limit %s exceeded.' % size_str(run_state.resources.memory)
                )

            if run_state.resources.disk and run_state.disk_utilization > run_state.resources.disk:
                kill_messages.append(
                    'Disk limit %sb exceeded.' % size_str(run_state.resources.disk)
                )

            if kill_messages:
                run_state = run_state._replace(kill_message=' '.join(kill_messages), is_killed=True)
            return run_state

        def check_disk_utilization():
            running = True
            while running:
                start_time = time.time()
                try:
                    disk_utilization = get_path_size(run_state.bundle_path)
                    self.disk_utilization[run_state.bundle.uuid][
                        'disk_utilization'
                    ] = disk_utilization
                    running = self.disk_utilization[run_state.bundle.uuid]['running']
                except Exception:
                    logger.error(traceback.format_exc())
                end_time = time.time()

                # To ensure that we don't hammer the disk for this computation when
                # there are lots of files, we run it at most 10% of the time.
                time.sleep(max((end_time - start_time) * 10, 1.0))

        self.disk_utilization.add_if_new(
            run_state.bundle.uuid, threading.Thread(target=check_disk_utilization, args=[])
        )
        run_state = check_and_report_finished(run_state)
        run_state = check_resource_utilization(run_state)

        if run_state.is_killed or run_state.is_restaged:
            if docker_utils.container_exists(run_state.container):
                try:
                    run_state.container.kill()
                except docker.errors.APIError:
                    finished, _, _ = docker_utils.check_finished(run_state.container)
                    if not finished:
                        logger.error(traceback.format_exc())
            self.disk_utilization[run_state.bundle.uuid]['running'] = False
            self.disk_utilization.remove(run_state.bundle.uuid)
            return run_state._replace(stage=RunStage.CLEANING_UP)
        if run_state.finished:
            logger.debug(
                'Finished run with UUID %s, exitcode %s, failure_message %s',
                run_state.bundle.uuid,
                run_state.exitcode,
                run_state.failure_message,
            )
            self.disk_utilization[run_state.bundle.uuid]['running'] = False
            self.disk_utilization.remove(run_state.bundle.uuid)
            return run_state._replace(stage=RunStage.CLEANING_UP, run_status='Uploading results')
        else:
            return run_state

    def _transition_from_CLEANING_UP(self, run_state):
        """
        1- delete the container if still existent
        2- clean up the dependencies from bundle directory
        3- release the dependencies in dependency manager
        4- If bundle has contents to upload (i.e. was RUNNING at some point),
            move to UPLOADING_RESULTS state
           Otherwise move to FINALIZING state
        """

        def remove_path_no_fail(path):
            try:
                remove_path(path)
            except Exception:
                logger.error(traceback.format_exc())

        if run_state.container_id is not None:
            while docker_utils.container_exists(run_state.container):
                try:
                    finished, _, _ = docker_utils.check_finished(run_state.container)
                    if finished:
                        run_state.container.remove(force=True)
                        run_state = run_state._replace(container=None, container_id=None)
                        break
                    else:
                        try:
                            run_state.container.kill()
                        except docker.errors.APIError:
                            logger.error(traceback.format_exc())
                            time.sleep(1)
                except docker.errors.APIError:
                    logger.error(traceback.format_exc())
                    time.sleep(1)

        for dep in run_state.bundle.dependencies:
            if not self.shared_file_system:  # No dependencies if shared fs worker
                dep_key = DependencyKey(dep.parent_uuid, dep.parent_path)
                self.dependency_manager.release(run_state.bundle.uuid, dep_key)

        # Clean up dependencies paths
        for path in self.paths_to_remove:
            remove_path_no_fail(path)
        self.paths_to_remove = []

        if run_state.is_restaged:
            return run_state._replace(stage=RunStage.RESTAGED)

        if not self.shared_file_system and run_state.has_contents:
            return run_state._replace(
                stage=RunStage.UPLOADING_RESULTS, run_status='Uploading results', container=None
            )
        else:
            # No need to upload results since results are directly written to bundle store
            # Delete any files that match the exclude_patterns .
            for exclude_pattern in run_state.bundle.metadata["exclude_patterns"]:
                full_pattern = os.path.join(run_state.bundle_path, exclude_pattern)
                for file_path in glob.glob(full_pattern, recursive=True):
                    # Only remove files that are subpaths of run_state.bundle_path, in case
                    # that exclude_pattern is something like "../../../".
                    if path_is_parent(parent_path=run_state.bundle_path, child_path=file_path):
                        remove_path(file_path)
            return self.finalize_run(run_state)

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
        if run_state.is_restaged:
            return run_state._replace(stage=RunStage.RESTAGED)

        def upload_results():
            try:
                # Upload results
                logger.debug('Uploading results for run with UUID %s', run_state.bundle.uuid)

                def progress_callback(bytes_uploaded):
                    run_status = 'Uploading results: %s done (archived size)' % size_str(
                        bytes_uploaded
                    )
                    self.uploading[run_state.bundle.uuid]['run_status'] = run_status
                    return True

                self.upload_bundle_callback(
                    run_state.bundle.uuid,
                    run_state.bundle_path,
                    run_state.bundle.metadata["exclude_patterns"],
                    progress_callback,
                )
                self.uploading[run_state.bundle.uuid]['success'] = True
            except Exception as e:
                self.uploading[run_state.bundle.uuid]['run_status'] = (
                    "Error while uploading: %s" % e
                )
                logger.error(traceback.format_exc())

        self.uploading.add_if_new(
            run_state.bundle.uuid, threading.Thread(target=upload_results, args=[])
        )

        if self.uploading[run_state.bundle.uuid].is_alive():
            return run_state._replace(
                run_status=self.uploading[run_state.bundle.uuid]['run_status']
            )
        elif not self.uploading[run_state.bundle.uuid]['success']:
            # upload failed
            failure_message = run_state.failure_message
            if failure_message:
                run_state = run_state._replace(
                    failure_message=(
                        failure_message + '. ' + self.uploading[run_state.bundle.uuid]['run_status']
                    )
                )
            else:
                run_state = run_state._replace(
                    failure_message=self.uploading[run_state.bundle.uuid]['run_status']
                )

        self.uploading.remove(run_state.bundle.uuid)
        return self.finalize_run(run_state)

    def finalize_run(self, run_state):
        """
        Prepare the finalize message to be sent with the next checkin
        """
        if run_state.is_killed:
            # Append kill_message, which contains more useful info on why a run was killed, to the failure message.
            failure_message = (
                "{}. {}".format(run_state.failure_message, run_state.kill_message)
                if run_state.failure_message
                else run_state.kill_message
            )
            run_state = run_state._replace(failure_message=failure_message)
        return run_state._replace(stage=RunStage.FINALIZING, run_status="Finalizing bundle")

    def _transition_from_FINALIZING(self, run_state):
        """
        If a full worker cycle has passed since we got into the FINALIZING state we already reported to
        server, if bundle is going be sent back to the server, move on to the RESTAGED state. Otherwise,
        move on to the FINISHED state. Can also remove bundle_path now.
        """
        if run_state.is_restaged:
            return run_state._replace(stage=RunStage.RESTAGED)
        elif run_state.finalized:
            if not self.shared_file_system:
                remove_path(run_state.bundle_path)  # don't remove bundle if shared FS
            return run_state._replace(stage=RunStage.FINISHED, run_status='Finished')
        else:
            return run_state
