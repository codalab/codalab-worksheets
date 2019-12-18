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
from codalab.worker.fsm import DependencyStage

logger = logging.getLogger(__name__)


class RunState:
    def __init__(
        self,
        run_status,  # type: str
        bundle,  # type: BundleInfo
        local_bundle_path,  # type: str
        bundle_dir_wait_num_tries,  # type: Optional[int]
        resources,  # type: RunResources
        bundle_start_time,  # type: int
        container_time_total,  # type: int
        container_time_user,  # type: int
        container_time_system,  # type: int
        container,  # type: Optional[docker.Container]
        container_id,  # type: Optional[str]
        docker_image,  # type: Optional[str]
        is_killed,  # type: bool
        has_contents,  # type: bool
        cpuset,  # type: Optional[Set[str]]
        gpuset,  # type: Optional[Set[str]]
        max_memory,  # type: int
        disk_utilization,  # type: int
        exitcode,  # type: Optionall[str]
        failure_message,  # type: Optional[str]
        kill_message,  # type: Optional[str]
        finished,  # type: bool
        finalized,  # type: bool
    ):
        self.run_status = run_status
        self.bundle = bundle
        self.local_bundle_path = local_bundle_path
        self.bundle_dir_wait_num_tries = bundle_dir_wait_num_tries
        self.resources = resources
        self.bundle_start_time = bundle_start_time
        self.container_time_total = container_time_total
        self.container_time_user = container_time_user
        self.container_time_system = container_time_system
        self.container = container
        self.container_id = container_id
        self.docker_image = docker_image
        self.is_killed = is_killed
        self.has_contents = has_contents
        self.cpuset = cpuset
        self.gpuset = gpuset
        self.max_memory = max_memory
        self.disk_utilization = disk_utilization
        self.exitcode = exitcode
        self.failure_message = failure_message
        self.kill_message = kill_message
        self.finished = finished
        self.finalized = finalized

    def transition(self, new_state):
        return new_state(
            self.run_status,
            self.bundle,
            self.local_bundle_path,
            self.bundle_dir_wait_num_tries,
            self.resources,
            self.bundle_start_time,
            self.container_time_total,
            self.container_time_user,
            self.container_time_system,
            self.container,
            self.container_id,
            self.docker_image,
            self.is_killed,
            self.has_contents,
            self.cpuset,
            self.gpuset,
            self.max_memory,
            self.disk_utilization,
            self.exitcode,
            self.failure_message,
            self.kill_message,
            self.finished,
            self.finalized,
        )

    def update(self, worker):
        return self

    @property
    def server_state(self):
        raise NotImplementedError

    @property
    def is_active(self):
        raise NotImplementedError


class Preparing(RunState):
    @property
    def server_state(self):
        return State.PREPARING

    @property
    def is_active(self):
        return True

    def update(self, worker):
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
        if self.is_killed:
            return self.transition(CleaningUp)

        dependencies_ready = True
        status_messages = []

        if not worker.shared_file_system:
            # No need to download dependencies if we're in the shared FS since they're already in our FS
            for dep_key, dep in self.bundle.dependencies.items():
                dependency_state = worker.dependency_manager.get(self.bundle.uuid, dep_key)
                if dependency_state.stage == DependencyStage.DOWNLOADING:
                    status_messages.append(
                        'Downloading dependency %s: %s done (archived size)'
                        % (dep.child_path, size_str(dependency_state.size_bytes))
                    )
                    dependencies_ready = False
                elif dependency_state.stage == DependencyStage.FAILED:
                    # Failed to download dependency; -> CLEANING_UP
                    self.failure_message = "Failed to download dependency %s: %s" % (
                        dep.child_path,
                        dependency_state.message,
                    )
                    return self.transition(CleaningUp)

        # get the docker image
        docker_image = self.resources.docker_image
        image_state = worker.image_manager.get(docker_image)
        if image_state.stage == DependencyStage.DOWNLOADING:
            status_messages.append(
                'Pulling docker image: ' + (image_state.message or docker_image or "")
            )
            dependencies_ready = False
        elif image_state.stage == DependencyStage.FAILED:
            # Failed to pull image; -> CLEANING_UP
            self.failure_message = 'Failed to download Docker image: %s' % image_state.message
            logger.error(self.failure_message)
            return self.transition(CleaningUp)

        # stop proceeding if dependency and image downloads aren't all done
        if not dependencies_ready:
            status_message = status_messages.pop()
            if status_messages:
                status_message += "(and downloading %d other dependencies and docker images)" % len(
                    status_messages
                )
            self.run_status = status_message
            return self

        # All dependencies ready! Set up directories, symlinks and container. Start container.
        # 1) Set up a directory to store the bundle.
        if worker.shared_file_system:
            if not os.path.exists(self.local_bundle_path):
                if self.bundle_dir_wait_num_tries == 0:
                    self.failure_message = (
                        "Bundle directory cannot be found on the shared filesystem. "
                        "Please ensure the shared fileystem between the server and "
                        "your worker is mounted properly or contact your administrators."
                    )
                    logger.error(self.failure_message)
                    return self.transition(CleaningUp)
                self.run_status = "Waiting for bundle directory to be created by the server"
                self.bundle_dir_wait_num_tries -= 1
                return self
        else:
            remove_path(self.local_bundle_path)
            os.mkdir(self.local_bundle_path)

        # 2) Set up symlinks
        docker_dependencies = []
        docker_dependencies_path = (
            '/' + self.bundle.uuid + ('_dependencies' if not worker.shared_file_system else '')
        )
        for dep_key, dep in self.bundle.dependencies.items():
            full_child_path = os.path.normpath(os.path.join(self.local_bundle_path, dep.child_path))
            if not full_child_path.startswith(self.local_bundle_path):
                # Dependencies should end up in their bundles (ie prevent using relative paths like ..
                # to get out of their parent bundles
                self.failure_message = 'Invalid key for dependency: %s' % (dep.child_path)
                logger.error(self.failure_message)
                return self.transition(CleaningUp)
            docker_dependency_path = os.path.join(docker_dependencies_path, dep.child_path)
            if worker.shared_file_system:
                # On a shared FS, we know where the dep is stored and can get the contents directly
                dependency_path = os.path.realpath(os.path.join(dep.location, dep.parent_path))
            else:
                # On a dependency_manager setup ask the manager where the dependency is
                dependency_path = os.path.join(
                    worker.dependency_manager.dependencies_dir,
                    worker.dependency_manager.get(self.bundle.uuid, dep_key).path,
                )
                os.symlink(docker_dependency_path, full_child_path)
            # These are turned into docker volume bindings like:
            #   dependency_path:docker_dependency_path:ro
            docker_dependencies.append((dependency_path, docker_dependency_path))

        # 3) Set up container
        if self.resources.network:
            docker_network = worker.docker_network_external.name
        else:
            docker_network = worker.docker_network_internal.name

        try:
            cpuset, gpuset = worker.assign_cpu_and_gpu_sets(
                self.resources.cpus, self.resources.gpus
            )
        except Exception as e:
            self.failure_message = "Cannot assign enough resources: %s" % traceback.format_exc()
            logger.error(self.failure_message)
            return self.transition(CleaningUp)

        # 4) Start container
        try:
            container = docker_utils.start_bundle_container(
                self.local_bundle_path,
                self.bundle.uuid,
                docker_dependencies,
                self.bundle.command,
                self.resources.docker_image,
                network=docker_network,
                cpuset=cpuset,
                gpuset=gpuset,
                memory_bytes=self.resources.memory,
                runtime=worker.docker_runtime,
            )
            worker.worker_docker_network.connect(container)
        except Exception as e:
            message = 'Cannot start Docker container: {}'.format(e)
            logger.error(message)
            logger.error(traceback.format_exc())
            raise

        self.run_status = 'Running job in Docker container'
        self.container_id = container.id
        self.container = container
        self.docker_image = image_state.digest
        self.has_contents = True
        self.cpuset = cpuset
        self.gpuset = gpuset
        return self.transition(Running)


class Running(RunState):
    @property
    def server_state(self):
        return State.RUNNING

    @property
    def is_active(self):
        return True

    def update(self, worker):
        """
        1- Check run status of the docker container
        2- If run is killed, kill the container
        3- If run is finished, move to CLEANING_UP state
        """

        def check_and_report_finished():
            try:
                self.finished, self.exitcode, self.failure_msg = docker_utils.check_finished(
                    self.container
                )
            except docker_utils.DockerException:
                logger.error(traceback.format_exc())
                self.finished, self.exitcode, self.failure_msg = False, None, None

        def check_resource_utilization():
            kill_messages = []

            run_stats = docker_utils.get_container_stats(self.container)

            self.max_memory = max(self.max_memory, run_stats.get('memory', 0))
            self.disk_utilization = worker.disk_utilization_threads[self.bundle.uuid][
                'disk_utilization'
            ]

            container_time_total = docker_utils.get_container_running_time(self.container)
            self.container_time_total = container_time_total
            self.container_time_user = run_stats.get(
                'container_time_user', self.container_time_user
            )
            self.container_time_system = (
                run_stats.get('container_time_system', self.container_time_system),
            )

            if self.resources.time and container_time_total > self.resources.time:
                kill_messages.append(
                    'Time limit exceeded. (Container uptime %s > time limit %s)'
                    % (duration_str(container_time_total), duration_str(self.resources.time))
                )

            if self.max_memory > self.resources.memory or self.exitcode == '137':
                kill_messages.append('Memory limit %s exceeded.' % size_str(self.resources.memory))

            if self.resources.disk and self.disk_utilization > self.resources.disk:
                kill_messages.append('Disk limit %sb exceeded.' % size_str(self.resources.disk))

            if kill_messages:
                self.kill_message = ' '.join(kill_messages)
                self.is_killed = True

        def check_disk_utilization():
            running = True
            while running:
                start_time = time.time()
                try:
                    worker.disk_utilization_threads[self.bundle.uuid][
                        'disk_utilization'
                    ] = get_path_size(self.local_bundle_path)
                    running = worker.disk_utilization_threads[self.bundle.uuid]['running']
                except Exception:
                    logger.error(traceback.format_exc())
                end_time = time.time()

                # To ensure that we don't hammer the disk for this computation when
                # there are lots of files, we run it at most 10% of the time.
                time.sleep(max((end_time - start_time) * 10, 1.0))

        worker.disk_utilization_threads.add_if_new(
            self.bundle.uuid, threading.Thread(target=check_disk_utilization, args=[])
        )
        check_and_report_finished()
        check_resource_utilization()

        if self.is_killed:
            if docker_utils.container_exists(self.container):
                try:
                    self.container.kill()
                except docker.errors.APIError:
                    finished, _, _ = docker_utils.check_finished(self.container)
                    if not finished:
                        logger.error(traceback.format_exc())
            worker.disk_utilization_threads[self.bundle.uuid]['running'] = False
            worker.disk_utilization_threads.remove(self.bundle.uuid)
            return self.transition(CleaningUp)
        if self.finished:
            logger.debug(
                'Finished run with UUID %s, exitcode %s, failure_message %s',
                self.bundle.uuid,
                self.exitcode,
                self.failure_message,
            )
            worker.disk_utilization_threads[self.bundle.uuid]['running'] = False
            worker.disk_utilization_threads.remove(self.bundle.uuid)
            self.run_status = "Uploading results"
            return self.transition(CleaningUp)
        else:
            return self


class CleaningUp(RunState):
    @property
    def server_state(self):
        return State.RUNNING

    @property
    def is_active(self):
        return False

    def update(self, worker):
        """
        1- delete the container if still existent
        2- clean up the dependencies from bundle directory
        3- release the dependencies in dependency manager
        4- If bundle has contents to upload (i.e. was RUNNING at some point),
            move to UPLOADING_RESULTS state
           Otherwise move to FINALIZING state
        """
        if self.container_id is not None:
            while docker_utils.container_exists(self.container):
                try:
                    finished, _, _ = docker_utils.check_finished(self.container)
                    if finished:
                        self.container.remove(force=True)
                        self.container = None
                        self.container_id = None
                        break
                    else:
                        try:
                            self.container.kill()
                        except docker.errors.APIError:
                            logger.error(traceback.format_exc())
                            time.sleep(1)
                except docker.errors.APIError:
                    logger.error(traceback.format_exc())
                    time.sleep(1)

        for dep_key, dep in self.bundle.dependencies.items():
            if not worker.shared_file_system:  # No dependencies if shared fs worker
                worker.dependency_manager.release(self.bundle.uuid, dep_key)

            child_path = os.path.join(self.local_bundle_path, dep.child_path)
            try:
                remove_path(child_path)
            except Exception:
                logger.error(traceback.format_exc())

        if not worker.shared_file_system and self.has_contents:
            # No need to upload results since results are directly written to bundle store
            self.run_status = 'Uploading results'
            self.container = None
            return self.transition(UploadingResults)
        else:
            if not self.failure_message and self.is_killed:
                self.failure_message = self.kill_message
            self.run_status = "Finalizing bundle"
            return self.transition(Finalizing)


class UploadingResults(RunState):
    @property
    def server_state(self):
        return State.RUNNING

    @property
    def is_active(self):
        return False

    def update(self, worker):
        """
        If bundle not already uploading:
            Use the RunManager API to upload contents at local_bundle_path to the server
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
                logger.debug('Uploading results for run with UUID %s', self.bundle.uuid)

                def progress_callback(bytes_uploaded):
                    run_status = 'Uploading results: %s done (archived size)' % size_str(
                        bytes_uploaded
                    )
                    worker.uploading_threads[self.bundle.uuid]['run_status'] = run_status
                    return True

                worker.upload_bundle_contents(
                    self.bundle.uuid, self.local_bundle_path, progress_callback
                )
                worker.uploading_threads[self.bundle.uuid]['success'] = True
            except Exception as e:
                worker.uploading_threads[self.bundle.uuid]['run_status'] = (
                    "Error while uploading: %s" % e
                )
                logger.error(traceback.format_exc())

        worker.uploading_threads.add_if_new(
            self.bundle.uuid, threading.Thread(target=upload_results, args=[])
        )

        if worker.uploading_threads[self.bundle.uuid].is_alive():
            self.run_status = worker.uploading_threads[self.bundle.uuid]['run_status']
            return self
        elif not worker.uploading_threads[self.bundle.uuid]['success']:
            # upload failed
            failure_message = self.failure_message
            if failure_message:
                self.failure_message = (
                    failure_message
                    + '. '
                    + worker.uploading_threads[self.bundle.uuid]['run_status']
                )
            else:
                self.failure_message = worker.uploading_threads[self.bundle.uuid]['run_status']

        worker.uploading_threads.remove(self.bundle.uuid)
        if not self.failure_message and self.is_killed:
            self.failure_message = self.kill_message
        self.run_status = "Finalizing bundle"
        return self.transition(Finalizing)


class Finalizing(RunState):
    @property
    def server_state(self):
        return State.FINALIZING

    @property
    def is_active(self):
        return False

    def update(self, worker):
        """
        If a full worker cycle has passed since we got into FINALIZING we already reported to
        server so can move on to FINISHED. Can also remove local_bundle_path now
        """
        if self.finalized:
            if not worker.shared_file_system:
                remove_path(self.local_bundle_path)  # don't remove bundle if shared FS
            self.run_status = "Finished"
            return self.transition(Finished)
        else:
            return self


class Finished(RunState):
    @property
    def is_active(self):
        return False

    @property
    def server_state(self):
        if self.is_killed:
            return State.KILLED
        elif self.exitcode != 0:
            return State.FAILED
        else:
            return State.READY
