from contextlib import closing
import httplib
import logging
import os
import socket
import threading
import time
import traceback
import multiprocessing
import re
from subprocess import check_output

from run_manager import RunManagerBase, RunBase, FilesystemRunMixin
from bundle_service_client import BundleServiceException
from docker_client import DockerException
from download_util import BUNDLE_NO_LONGER_RUNNING_MESSAGE, get_target_info, get_target_path, PathException
from file_util import get_path_size, gzip_file, gzip_string, read_file_section, summarize_file, tar_gzip_directory, remove_path
from formatting import duration_str, size_str


logger = logging.getLogger(__name__)


class DockerRunManager(RunManagerBase):
    def __init__(self, docker, bundle_service, image_manager, worker):
        self._docker = docker
        self._bundle_service = bundle_service
        self._image_manager = image_manager
        self._worker = worker

    @property
    def cpus(self):
        return multiprocessing.cpu_count()

    @property
    def memory_bytes(self):
        try:
            return os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
        except ValueError:
            # Fallback to sysctl when os.sysconf('SC_PHYS_PAGES') fails on OS X
            return int(check_output(['sysctl', '-n', 'hw.memsize']).strip())

    @property
    def gpus(self):
        if not self._docker._use_nvidia_docker:
            return 0

        container_id = self._docker.run_nvidia_smi('-L', 'nvidia/cuda:8.0-runtime')
        out, err = self._docker.get_logs(container_id)
        count = len(re.findall('^GPU \d', out))
        self._docker.delete_container(container_id)
        return count

    def create_run(self, bundle, bundle_path, resources):
        run = Run(self._bundle_service, self._docker, self._image_manager, self._worker, bundle, bundle_path, resources)
        return run

    def serialize(self, run):
        """ Output a dictionary able to be serialized into json """
        assert isinstance(run, Run), "Could not serialize run which was from different run manager."
        run_info = {
            'bundle': run._bundle,
            'bundle_path': run._bundle_path,
            'resources': run._resources,
            'container_id': run._container_id,
            'start_time': run._start_time,
        }
        return run_info

    def deserialize(self, data):
        """ Create a new Run object and populate it based on given run_info dictionary """
        bundle = data['bundle']
        bundle_path = data['bundle_path']
        resources = data['resources']
        run = Run(self._bundle_service, self._docker, self._image_manager, self._worker,
                  bundle, bundle_path, resources)
        run._container_id = data['container_id']
        run._start_time = data['start_time']
        return run

    def worker_did_start(self):
        self._image_manager.start_cleanup_thread()

    def worker_will_stop(self):
        self._image_manager.stop_cleanup_thread()


class Run(FilesystemRunMixin, RunBase):
    """
    This class manages a single run, including

        1) Reporting to the bundle service that the run has started.
        2) Starting the Docker container.
        3) Reporting running container resource utilization to the bundle
           service, and killing the container if it uses too many resources.
        4) Periodically informing the server that the job is still running (think of this as a heartbeat,
           if the server does not receive one for more than WORKER_TIMEOUT_SECONDS, the job is moved to
           the WORKER_OFFLINE state)
        5) Handling any messages related to the run.
        6) Reporting to the bundle service that the run has finished.
    """

    def __init__(self, bundle_service, docker, image_manager, worker, bundle, bundle_path, resources):
        super(Run, self).__init__()
        self._bundle_service = bundle_service
        self._docker = docker
        self._image_manager = image_manager
        self._worker = worker
        self._bundle = bundle
        self._bundle_path = os.path.realpath(bundle_path)
        self._dep_paths = set([dep['child_path'] for dep in self._bundle['dependencies']])
        self._resources = resources
        self._uuid = bundle['uuid']
        self._container_id = None
        self._start_time = None # start time of container

        self._disk_utilization_lock = threading.Lock()
        self._disk_utilization = 0

        self._max_memory = 0

        self._kill_lock = threading.Lock()
        self._killed = False
        self._kill_message = None
        self._docker_container_killed = False

        self._finished_lock = threading.Lock()
        self._finished = False

    @property
    def bundle(self):
        return self._bundle

    @property
    def resources(self):
        return self._resources

    @property
    def bundle_path(self):
        return self._bundle_path

    @property
    def is_shared_file_system(self):
        return self._worker.shared_file_system

    def start(self):
        """
        Starts running the bundle. First, it checks in with the bundle service
        and sees if the bundle is still assigned to this worker. If not, returns
        False. Otherwise, starts the run in a new thread and returns True.
        """
        # Report that the bundle is running. We note the start time here for
        # accurate accounting of time used, since the clock on the bundle
        # service and on the worker could be different.
        self._start_time = time.time()
        start_message = {
            'hostname': socket.gethostname(),
            'start_time': int(self._start_time),
        }
        if not self._bundle_service.start_bundle(self._worker.id, self._uuid,
                                                 start_message):
            return False

        if self._worker.shared_file_system:
            # On a shared file system we create the path in the bundle manager
            # to avoid NFS directory cache issues. Here, we wait for the cache
            # on this machine to expire and for the path to appear.
            while not os.path.exists(self._bundle_path):
                time.sleep(0.5)
        else:
            # Set up a directory to store the bundle.
            remove_path(self._bundle_path)
            os.mkdir(self._bundle_path)

        # Start a thread for this run.
        threading.Thread(target=Run._start, args=[self]).start()

        return True

    def resume(self):
        """
        Report that the bundle is running. We note the start time here for
        accurate accounting of time used, since the clock on the bundle
        service and on the worker could be different.
        """
        start_message = {
            'hostname': socket.gethostname(),
            'start_time': int(self._start_time),
        }

        if not self._bundle_service.resume_bundle(self._worker.id, self._uuid,
                                                 start_message):
            return False

        # Start a thread for this run.
        def resume_run(self):
            Run._safe_update_run_status(self, 'Running')
            Run._monitor(self)

        threading.Thread(target=resume_run, args=[self]).start()
        return True


    def _safe_update_docker_image(self, docker_image):
        """ Update the docker_image metadata field for the run bundle """
        try:
            update = {
                'docker_image': docker_image
            }
            self._bundle_service.update_bundle_metadata(self._worker.id, self._uuid, update)
        except BundleServiceException:
            traceback.print_exc()

    def _safe_update_run_status(self, status):
        try:
            update = {
                'run_status': status,
                'last_updated': int(time.time()),
                'time': time.time() - self._start_time,
            }
            self._bundle_service.update_bundle_metadata(self._worker.id, self._uuid, update)
        except BundleServiceException:
            traceback.print_exc()

    def _throttled_updater(self):
        PROGRESS_UPDATE_FREQ_SECS = 2.0
        last_update_time = [0]
        def update(status):
            if (time.time() - last_update_time[0] >= PROGRESS_UPDATE_FREQ_SECS):
                last_update_time[0] = time.time()
                self._safe_update_run_status(status)
        return update

    def _check_killed(self):
        if self._is_killed():
            raise Exception(self._get_kill_message())

    def _start(self):
        """
        Starts the Docker container and then passes execution on to the _monitor
        function.
        """
        logger.debug('Starting run with UUID %s', self._uuid)
        try:
            # Used to ensure that we can kill the run while it's downloading
            # dependencies or the Docker image.

            dependencies = self.setup_dependencies()

            def do_start():
                self._safe_update_run_status('Starting Docker container')
                # The docker client only wants the pair of paths
                docker_dependencies = [(dep[0], dep[1]) for dep in dependencies]
                return self._docker.start_container(
                    self._bundle_path, self._uuid, self._bundle['command'],
                    self._resources['docker_image'],
                    self._resources['request_network'],
                    docker_dependencies)

            # Pull the docker image regardless of whether or not we already have it
            # This will make sure we pull updated versions of the image
            updater = self._throttled_updater()

            def update_status_and_check_killed(status):
                updater('Pulling docker image: ' + status)
                self._check_killed()
            self._docker.download_image(self._resources['docker_image'],
                                        update_status_and_check_killed)
            self._container_id = do_start()

            digest = self._docker.get_image_repo_digest(self._resources['docker_image'])
            self._safe_update_docker_image(digest)
            self._image_manager.touch_image(digest)

        except Exception as e:
            logger.exception('Failed while starting run')
            self._finish(failure_message=str(e))
            self._worker.finish_run(self._uuid)
            return

        self._safe_update_run_status('Running')
        self._monitor()

    def download_dependency(self, uuid, path):
        updater = self._throttled_updater()

        def update_status_and_check_killed(bytes_downloaded):
            updater('Downloading dependency %s/%s: %s done (archived size)' %
                    (uuid, path, size_str(bytes_downloaded)))
            self._check_killed()

        dependency_path = self._worker.add_dependency(uuid, path, self._uuid, update_status_and_check_killed)
        return dependency_path

    def _monitor(self):
        # We measure the disk utilization in another thread, since that could be
        # slow if there are lots of files.
        threading.Thread(target=Run._compute_disk_utilization, args=[self]).start()

        REPORT_FREQ_SECS = 5.0
        last_report_time = 0
        while True:
            self._handle_kill()
            if self._check_and_report_finished():
                break

            if time.time() - last_report_time >= REPORT_FREQ_SECS:
                report = True
                last_report_time = time.time()
            else:
                report = False
            self._check_and_report_resource_utilization(report)

            try:
                self.resume()
            except BundleServiceException:
                pass

            # TODO(klopyrev): Upload the contents of the running bundle to the
            #                 bundle service every few hours, so that they are
            #                 available in case the worker dies.

            time.sleep(0.5)

        self._worker.finish_run(self._uuid)

    def _check_and_report_resource_utilization(self, report):
        new_metadata = {}

        # Get wall clock time.
        new_metadata['time'] = time.time() - self._start_time
        if (self._resources['request_time'] and
            new_metadata['time'] > self._resources['request_time']):
            self.kill('Time limit %s exceeded.' % duration_str(self._resources['request_time']))

        # Get memory, time_user and time_system.
        new_metadata.update(self._docker.get_container_stats(self._container_id))
        if 'memory' in new_metadata and new_metadata['memory'] > self._max_memory:
            self._max_memory = new_metadata['memory']
        new_metadata['memory_max'] = self._max_memory
        if (self._resources['request_memory'] and
            'memory' in new_metadata and
            new_metadata['memory'] > self._resources['request_memory']):
            self.kill('Memory limit %sb exceeded.' % size_str(self._resources['request_memory']))

        # Get disk utilization.
        with self._disk_utilization_lock:
            new_metadata['data_size'] = self._disk_utilization
        if (self._resources['request_disk'] and
            new_metadata['data_size'] > self._resources['request_disk']):
            self.kill('Disk limit %sb exceeded.' % size_str(self._resources['request_disk']))

        new_metadata['last_updated'] = int(time.time())

        if report:
            logger.debug('Reporting resource utilization for run with UUID %s', self._uuid)
            try:
                self._bundle_service.update_bundle_metadata(self._worker.id, self._uuid, new_metadata)
            except BundleServiceException:
                traceback.print_exc()

    def _compute_disk_utilization(self):
        while not self._is_finished():
            start_time = time.time()
            try:
                disk_utilization = get_path_size(self._bundle_path, self._dep_paths)
                with self._disk_utilization_lock:
                    self._disk_utilization = disk_utilization
            except Exception:
                traceback.print_exc()
            end_time = time.time()

            # To ensure that we don't hammer the disk for this computation when
            # there are lots of files, we run it at most 10% of the time.
            time.sleep(max((end_time - start_time) * 10, 1.0))

    def kill(self):
        with self._kill_lock:
            self._killed = True
            self._kill_message = 'Kill requested.'

    def _is_killed(self):
        with self._kill_lock:
            return self._killed

    def _get_kill_message(self):
        with self._kill_lock:
            return self._kill_message

    def _handle_kill(self):
        if self._is_killed() and not self._docker_container_killed:
            try:
                self._docker.kill_container(self._container_id)
                self._docker_container_killed = True
            except DockerException:
                traceback.print_exc()

    def _check_and_report_finished(self):
        try:
            finished, exitcode, failure_message = (
                self._docker.check_finished(self._container_id))
        except DockerException:
            traceback.print_exc()
            return False

        if finished:
            self._finish(exitcode, failure_message)
        return finished

    def _finish(self, exitcode=None, failure_message=None):
        logger.debug('Finished run with UUID %s, exitcode %s, failure_message %s',
                     self._uuid, exitcode, failure_message)
        self._set_finished()
        try:
            # Delete the container.
            if self._container_id is not None:
                while True:
                    try:
                        self._docker.delete_container(self._container_id)
                        break
                    except DockerException:
                        traceback.print_exc()
                        time.sleep(1)

            # Clean-up dependencies.
            for dep in self._bundle['dependencies']:
                if not self._worker.shared_file_system:
                    self._worker.remove_dependency(
                        dep['parent_uuid'], dep['parent_path'], self._uuid)
                # Clean-up the symlinks we created.
                child_path = os.path.join(self._bundle_path, dep['child_path'])
                remove_path(child_path)

            if not self._worker.shared_file_system:
                logger.debug('Uploading results for run with UUID %s', self._uuid)
                updater = self._throttled_updater()
                def update_status(bytes_uploaded):
                    updater('Uploading results: %s done (archived size)' %
                        size_str(bytes_uploaded))
                self._execute_bundle_service_command_with_retry(
                    lambda: self._bundle_service.update_bundle_contents(
                        self._worker.id, self._uuid, self._bundle_path,
                        update_status))

            logger.debug('Finalizing run with UUID %s', self._uuid)
            self._safe_update_run_status('Finished')  # Also, reports the finish time.
            if failure_message is None and self._is_killed():
                failure_message = self._get_kill_message()
            finalize_message = {
                'exitcode': exitcode,
                'failure_message': failure_message,
            }
            self._execute_bundle_service_command_with_retry(
                lambda: self._bundle_service.finalize_bundle(
                    self._worker.id, self._uuid, finalize_message))
        except Exception:
            traceback.print_exc()

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

    def _is_finished(self):
        with self._finished_lock:
            return self._finished

    def _set_finished(self):
        with self._finished_lock:
            self._finished = True

    @property
    def requested_memory_bytes(self):
        """
        If request_memory is defined, then return that.
        Otherwise, this run's memory usage does not get checked, so return inf.
        """
        return self._resources['request_memory'] or float('inf')
