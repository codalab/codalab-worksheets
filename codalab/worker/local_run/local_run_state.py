from collections import namedtuple
import logging
import os
import threading
import time
import traceback

import docker
import codalab.worker.docker_utils as docker_utils

from codalab.lib.formatting import size_str, duration_str
from codalab.worker.file_util import remove_path, get_path_size
from codalab.worker.bundle_state import State
from codalab.worker.fsm import DependencyStage, StateTransitioner
from codalab.worker.worker_thread import ThreadDict

logger = logging.getLogger(__name__)


class LocalRunStage(object):
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
    PREPARING = 'LOCAL_RUN.PREPARING'
    WORKER_STATE_TO_SERVER_STATE[PREPARING] = State.PREPARING

    """
    Running encompasses the state where the user's job is running
    """
    RUNNING = 'LOCAL_RUN.RUNNING'
    WORKER_STATE_TO_SERVER_STATE[RUNNING] = State.RUNNING

    """
    This stage encompasses cleaning up intermediary components like
    the dependency symlinks and also the releasing of dependencies
    """
    CLEANING_UP = 'LOCAL_RUN.CLEANING_UP'
    WORKER_STATE_TO_SERVER_STATE[CLEANING_UP] = State.RUNNING

    """
    Uploading results means the job's results are getting uploaded to the server
    """
    UPLOADING_RESULTS = 'LOCAL_RUN.UPLOADING_RESULTS'
    WORKER_STATE_TO_SERVER_STATE[UPLOADING_RESULTS] = State.RUNNING

    """
    Finalizing means the worker is finalizing the bundle metadata with the server
    """
    FINALIZING = 'LOCAL_RUN.FINALIZING'
    WORKER_STATE_TO_SERVER_STATE[FINALIZING] = State.FINALIZING

    """
    Finished means the worker is done with this run
    """
    FINISHED = 'LOCAL_RUN.FINISHED'
    WORKER_STATE_TO_SERVER_STATE[FINISHED] = State.READY


LocalRunState = namedtuple(
    'RunState',
    [
        'stage',
        'run_status',
        'bundle',
        'bundle_path',
        'resources',
        'bundle_start_time',
        'container_start_time',
        'container_time_total',
        'container_time_user',
        'container_time_system',
        'container',
        'container_id',
        'docker_image',
        'is_killed',
        'has_contents',
        'cpuset',
        'gpuset',
        'max_memory',
        'disk_utilization',
        'info',
    ],
)


class LocalRunStateMachine(StateTransitioner):
    """
    Manages the state machine of the runs running on the local machine

    Note that in general there are two types of errors:
    - User errors (fault of bundle) - we fail the bundle (move to CLEANING_UP state).
    - System errors (fault of worker) - we freeze this worker (Exception is thrown up).
    It's not always clear where the line is.
    """

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
    ):
        super(LocalRunStateMachine, self).__init__()
        self.add_transition(LocalRunStage.PREPARING, self._transition_from_PREPARING)
        self.add_transition(LocalRunStage.RUNNING, self._transition_from_RUNNING)
        self.add_transition(LocalRunStage.CLEANING_UP, self._transition_from_CLEANING_UP)
        self.add_transition(
            LocalRunStage.UPLOADING_RESULTS, self._transition_from_UPLOADING_RESULTS
        )
        self.add_transition(LocalRunStage.FINALIZING, self._transition_from_FINALIZING)
        self.add_terminal(LocalRunStage.FINISHED)

        self.dependency_manager = dependency_manager
        self.docker_image_manager = docker_image_manager
        self.worker_docker_network = worker_docker_network
        self.docker_network_external = docker_network_external
        self.docker_network_internal = docker_network_internal
        self.docker_runtime = docker_runtime
        # bundle_uuid -> {'thread': Thread, 'run_status': str}
        self.uploading = ThreadDict(fields={'run_status': 'Upload started', 'success': False})
        # bundle_uuid -> {'thread': Thread, 'disk_utilization': int, 'running': bool}
        self.disk_utilization = ThreadDict(
            fields={'disk_utilization': 0, 'running': True, 'lock': None}
        )
        self.upload_bundle_callback = upload_bundle_callback
        self.assign_cpu_and_gpu_sets_fn = assign_cpu_and_gpu_sets_fn

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
        if run_state.is_killed:
            return run_state._replace(stage=LocalRunStage.CLEANING_UP)

        dependencies_ready = True
        status_messages = []
        bundle_uuid = run_state.bundle['uuid']

        # get dependencies
        for dep in run_state.bundle['dependencies']:
            dependency = (dep['parent_uuid'], dep['parent_path'])
            dependency_state = self.dependency_manager.get(bundle_uuid, dependency)
            if dependency_state.stage == DependencyStage.DOWNLOADING:
                status_messages.append(
                    'Downloading dependency %s: %s done (archived size)'
                    % (dep['child_path'], size_str(dependency_state.size_bytes))
                )
                dependencies_ready = False
            elif dependency_state.stage == DependencyStage.FAILED:
                # Failed to download dependency; -> CLEANING_UP
                run_state.info['failure_message'] = 'Failed to download dependency %s: %s' % (
                    dep['child_path'],
                    '',
                )
                return run_state._replace(stage=LocalRunStage.CLEANING_UP, info=run_state.info)

        # get the docker image
        docker_image = run_state.resources['docker_image']
        image_state = self.docker_image_manager.get(docker_image)
        if image_state.stage == DependencyStage.DOWNLOADING:
            status_messages.append(
                'Pulling docker image: ' + (image_state.message or docker_image or "")
            )
            dependencies_ready = False
        elif image_state.stage == DependencyStage.FAILED:
            # Failed to pull image; -> CLEANING_UP
            message = 'Failed to download Docker image: %s' % image_state.message
            run_state.info['failure_message'] = message
            logger.error(message)
            return run_state._replace(stage=LocalRunStage.CLEANING_UP, info=run_state.info)

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
        remove_path(run_state.bundle_path)
        os.mkdir(run_state.bundle_path)

        # 2) Set up symlinks
        dependencies = []
        docker_dependencies_path = '/' + bundle_uuid + '_dependencies'
        for dep in run_state.bundle['dependencies']:
            child_path = os.path.normpath(os.path.join(run_state.bundle_path, dep['child_path']))
            if not child_path.startswith(run_state.bundle_path):
                message = 'Invalid key for dependency: %s' % (dep['child_path'])
                run_state.info['failure_message'] = message
                logger.error(message)
                return run_state._replace(stage=LocalRunStage.CLEANING_UP, info=run_state.info)

            dependency_path = self.dependency_manager.get(
                bundle_uuid, (dep['parent_uuid'], dep['parent_path'])
            ).path
            dependency_path = os.path.join(
                self.dependency_manager.dependencies_dir, dependency_path
            )

            docker_dependency_path = os.path.join(docker_dependencies_path, dep['child_path'])

            os.symlink(docker_dependency_path, child_path)
            # These are turned into docker volume bindings like:
            #   dependency_path:docker_dependency_path:ro
            dependencies.append((dependency_path, docker_dependency_path))

        # 3) Set up container
        if run_state.resources['request_network']:
            docker_network = self.docker_network_external.name
        else:
            docker_network = self.docker_network_internal.name

        try:
            cpuset, gpuset = self.assign_cpu_and_gpu_sets_fn(
                run_state.resources['request_cpus'], run_state.resources['request_gpus']
            )
        except Exception:
            message = "Cannot assign enough resources"
            run_state.info['failure_message'] = message
            logger.error(message)
            logger.error(traceback.format_exc())
            return run_state._replace(stage=LocalRunStage.CLEANING_UP, info=run_state.info)

        # 4) Start container
        try:
            container = docker_utils.start_bundle_container(
                run_state.bundle_path,
                bundle_uuid,
                dependencies,
                run_state.bundle['command'],
                run_state.resources['docker_image'],
                network=docker_network,
                cpuset=cpuset,
                gpuset=gpuset,
                memory_bytes=run_state.resources['request_memory'],
                runtime=self.docker_runtime,
            )
            self.worker_docker_network.connect(container)
        except Exception as e:
            message = 'Cannot start Docker container: {}'.format(e)
            logger.error(message)
            logger.error(traceback.format_exc())
            raise

        return run_state._replace(
            stage=LocalRunStage.RUNNING,
            container_start_time=time.time(),
            run_status='Running job in Docker container',
            container_id=container.id,
            container=container,
            docker_image=image_state.digest,
            has_contents=True,
            cpuset=cpuset,
            gpuset=gpuset,
        )

    def _transition_from_RUNNING(self, run_state):
        """
        1- Check run status of the docker container
        2- If run is killed, kill the container
        3- If run is finished, move to CLEANING_UP state
        """
        bundle_uuid = run_state.bundle['uuid']

        def check_and_report_finished(run_state):
            try:
                finished, exitcode, failure_msg = docker_utils.check_finished(run_state.container)
            except docker_utils.DockerException:
                logger.error(traceback.format_exc())
                finished, exitcode, failure_msg = False, None, None
            new_info = dict(finished=finished, exitcode=exitcode, failure_message=failure_msg)
            run_state.info.update(new_info)
            run_state = run_state._replace(info=run_state.info)
            return run_state

        def check_resource_utilization(run_state):
            kill_messages = []

            run_stats = docker_utils.get_container_stats(run_state.container)

            run_state = run_state._replace(
                max_memory=max(run_state.max_memory, run_stats.get('memory', 0))
            )
            run_state = run_state._replace(
                disk_utilization=self.disk_utilization[bundle_uuid]['disk_utilization']
            )

            container_time_total = time.time() - run_state.container_start_time
            run_state = run_state._replace(
                container_time_total=container_time_total,
                container_time_user=run_stats.get(
                    'container_time_user', run_state.container_time_user
                ),
                container_time_system=run_stats.get(
                    'container_time_system', run_state.container_time_system
                ),
            )

            if (
                run_state.resources['request_time']
                and container_time_total > run_state.resources['request_time']
            ):
                kill_messages.append(
                    'Time limit exceeded. (Container uptime %s > time limit %s)'
                    % (
                        duration_str(container_time_total),
                        duration_str(run_state.resources['request_time']),
                    )
                )

            if (
                run_state.max_memory > run_state.resources['request_memory']
                or run_state.info.get('exitcode', '0') == '137'
            ):
                kill_messages.append(
                    'Memory limit %s exceeded.' % size_str(run_state.resources['request_memory'])
                )

            if (
                run_state.resources['request_disk']
                and run_state.disk_utilization > run_state.resources['request_disk']
            ):
                kill_messages.append(
                    'Disk limit %sb exceeded.' % size_str(run_state.resources['request_disk'])
                )

            if kill_messages:
                new_info = run_state.info
                new_info['kill_message'] = ' '.join(kill_messages)
                run_state = run_state._replace(info=new_info, is_killed=True)

            return run_state

        def check_disk_utilization():
            running = True
            while running:
                start_time = time.time()
                try:
                    disk_utilization = get_path_size(run_state.bundle_path)
                    self.disk_utilization[bundle_uuid]['disk_utilization'] = disk_utilization
                    running = self.disk_utilization[bundle_uuid]['running']
                except Exception:
                    logger.error(traceback.format_exc())
                end_time = time.time()

                # To ensure that we don't hammer the disk for this computation when
                # there are lots of files, we run it at most 10% of the time.
                time.sleep(max((end_time - start_time) * 10, 1.0))

        self.disk_utilization.add_if_new(
            bundle_uuid, threading.Thread(target=check_disk_utilization, args=[])
        )
        run_state = check_and_report_finished(run_state)
        run_state = check_resource_utilization(run_state)

        if run_state.is_killed:
            if docker_utils.container_exists(run_state.container):
                try:
                    run_state.container.kill()
                except docker.errors.APIError:
                    finished, _, _ = docker_utils.check_finished(run_state.container)
                    if not finished:
                        logger.error(traceback.format_exc())
            self.disk_utilization[bundle_uuid]['running'] = False
            self.disk_utilization.remove(bundle_uuid)
            return run_state._replace(stage=LocalRunStage.CLEANING_UP)
        if run_state.info['finished']:
            logger.debug(
                'Finished run with UUID %s, exitcode %s, failure_message %s',
                bundle_uuid,
                run_state.info['exitcode'],
                run_state.info['failure_message'],
            )
            self.disk_utilization[bundle_uuid]['running'] = False
            self.disk_utilization.remove(bundle_uuid)
            return run_state._replace(
                stage=LocalRunStage.CLEANING_UP, run_status='Uploading results'
            )
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

        for dep in run_state.bundle['dependencies']:
            self.dependency_manager.release(bundle_uuid, (dep['parent_uuid'], dep['parent_path']))

            child_path = os.path.join(run_state.bundle_path, dep['child_path'])
            try:
                remove_path(child_path)
            except Exception:
                logger.error(traceback.format_exc())

        if run_state.has_contents:
            return run_state._replace(
                stage=LocalRunStage.UPLOADING_RESULTS,
                run_status='Uploading results',
                container=None,
            )
        else:
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

        def upload_results():
            try:
                # Upload results
                logger.debug('Uploading results for run with UUID %s', bundle_uuid)

                def progress_callback(bytes_uploaded):
                    run_status = 'Uploading results: %s done (archived size)' % size_str(
                        bytes_uploaded
                    )
                    self.uploading[bundle_uuid]['run_status'] = run_status
                    return True

                self.upload_bundle_callback(bundle_uuid, run_state.bundle_path, progress_callback)
                self.uploading[bundle_uuid]['success'] = True
            except Exception as e:
                self.uploading[bundle_uuid]['run_status'] = "Error while uploading: %s" % e
                logger.error(traceback.format_exc())

        bundle_uuid = run_state.bundle['uuid']
        self.uploading.add_if_new(bundle_uuid, threading.Thread(target=upload_results, args=[]))

        if self.uploading[bundle_uuid].is_alive():
            return run_state._replace(run_status=self.uploading[bundle_uuid]['run_status'])
        elif not self.uploading[bundle_uuid]['success']:
            # upload failed
            failure_message = run_state.info.get('failure_message', None)
            if failure_message:
                run_state.info['failure_message'] = (
                    failure_message + '. ' + self.uploading[bundle_uuid]['run_status']
                )
            else:
                run_state.info['failure_message'] = self.uploading[bundle_uuid]['run_status']

        self.uploading.remove(bundle_uuid)
        return self.finalize_run(run_state)

    def finalize_run(self, run_state):
        """
        Prepare the finalize message to be sent with the next checkin
        """
        failure_message = run_state.info.get('failure_message', None)
        if 'exitcode' not in run_state.info:
            run_state.info['exitcode'] = None
        if not failure_message and run_state.is_killed:
            run_state.info['failure_message'] = run_state.info['kill_message']
        run_state.info['finalized'] = False
        return run_state._replace(
            stage=LocalRunStage.FINALIZING, info=run_state.info, run_status="Finalizing bundle"
        )

    def _transition_from_FINALIZING(self, run_state):
        """
        If a full worker cycle has passed since we got into FINALIZING we already reported to
        server so can move on to FINISHED. Can also remove bundle_path now
        """
        if run_state.info['finalized']:
            remove_path(run_state.bundle_path)
            return run_state._replace(stage=LocalRunStage.FINISHED, run_status='Finished')
        else:
            return run_state
