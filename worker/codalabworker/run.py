from contextlib import closing
import httplib
import logging
import os
import socket
import threading
import time
import traceback

from bundle_service_client import BundleServiceException
from docker_client import DockerException
from download_util import BUNDLE_NO_LONGER_RUNNING_MESSAGE, get_target_info, get_target_path, PathException
from file_util import get_path_size, gzip_file, gzip_string, read_file_section, summarize_file, tar_gzip_directory, remove_path
from formatting import duration_str, size_str


logger = logging.getLogger(__name__)


class Run(object):
    """
    This class manages a single run, including

        1) Reporting to the bundle service that the run has started.
        2) Starting the Docker container.
        3) Reporting running container resource utilization to the bundle
           service, and killing the container if it uses too many resources.
        4) Handling any messages related to the run.
        5) Reporting to the bundle service that the run has finished.
    """
    def __init__(self, bundle_service, docker, image_manager, worker,
                 bundle, bundle_path, resources):
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

        self._disk_utilization_lock = threading.Lock()
        self._disk_utilization = 0

        self._max_memory = 0

        self._kill_lock = threading.Lock()
        self._killed = False
        self._kill_message = None
        self._docker_container_killed = False

        self._finished_lock = threading.Lock()
        self._finished = False

    def run(self):
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

    def _start(self):
        """
        Starts the Docker container and then passes execution on to the _monitor
        function.
        """
        logger.debug('Starting run with UUID %s', self._uuid)
        try:
            # Used to ensure that we can kill the run while it's downloading
            # dependencies or the Docker image.
            def check_killed():
                if self._is_killed():
                    raise Exception(self._get_kill_message())

            dependencies = []
            docker_dependencies_path = '/' + self._uuid + '_dependencies'
            for dep in self._bundle['dependencies']:
                child_path = os.path.normpath(
                    os.path.join(self._bundle_path, dep['child_path']))
                if not child_path.startswith(self._bundle_path):
                    raise Exception('Invalid key for dependency: %s' % (
                        dep['child_path']))

                if self._worker.shared_file_system:
                    parent_bundle_path = dep['location']

                    # Check that the dependency is valid (i.e. points inside the
                    # bundle and isn't a broken symlink).
                    parent_bundle_path = os.path.realpath(parent_bundle_path)
                    dependency_path = os.path.realpath(
                        os.path.join(parent_bundle_path, dep['parent_path']))
                    if (not dependency_path.startswith(parent_bundle_path) or
                        not os.path.exists(dependency_path)):
                        raise Exception('Invalid dependency %s/%s' % (
                            dep['parent_uuid'], dep['parent_path']))
                else:
                    updater = self._throttled_updater()
                    def update_status_and_check_killed(bytes_downloaded):
                        updater('Downloading dependency %s: %s done (archived size)' % (
                            dep['child_path'], size_str(bytes_downloaded)))
                        check_killed()
                    dependency_path = self._worker.add_dependency(
                        dep['parent_uuid'], dep['parent_path'], self._uuid,
                        update_status_and_check_killed)

                docker_dependency_path = os.path.join(
                    docker_dependencies_path, dep['child_path'])
                os.symlink(docker_dependency_path, child_path)
                dependencies.append((dependency_path, docker_dependency_path))

            def do_start():
                self._safe_update_run_status('Starting Docker container')
                return self._docker.start_container(
                    self._bundle_path, self._uuid, self._bundle['command'],
                    self._resources['docker_image'],
                    self._resources['request_network'],
                    dependencies)

            try:
                self._container_id = do_start()
            except DockerException as e:
                # The download image call is slow, even if the image is already
                # available. Thus, we only make it if we know the image is not
                # available. Start-up is much faster that way.
                if 'No such image' in e.message:
                    updater = self._throttled_updater()
                    def update_status_and_check_killed(status):
                        updater('Downloading Docker image: ' + status)
                        check_killed()
                    self._docker.download_image(self._resources['docker_image'],
                                                update_status_and_check_killed)
                    self._container_id = do_start()
                else:
                    raise

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

    @staticmethod
    def read_run_missing(bundle_service, worker, socket_id):
        message = {
            'error_code': httplib.INTERNAL_SERVER_ERROR,
            'error_message': BUNDLE_NO_LONGER_RUNNING_MESSAGE,
        }
        bundle_service.reply(worker.id, socket_id, message)

    def read(self, socket_id, path, read_args):
        def reply_error(code, message):
            message = {
                'error_code': code,
                'error_message': message,
            }
            self._bundle_service.reply(self._worker.id, socket_id, message)

        try:
            read_type = read_args['type']
            if read_type == 'get_target_info':
                # At the top-level directory, we should ignore dependencies.
                if path and os.path.normpath(path) in self._dep_paths:
                    target_info = None
                else:
                    try:
                        target_info = get_target_info(
                            self._bundle_path, self._uuid, path, read_args['depth'])
                    except PathException as e:
                        reply_error(httplib.BAD_REQUEST, e.message)
                        return

                    if not path and read_args['depth'] > 0:
                        target_info['contents'] = [
                            child for child in target_info['contents']
                            if child['name'] not in self._dep_paths]

                self._bundle_service.reply(self._worker.id, socket_id,
                                           {'target_info': target_info})
            else:
                try:
                    final_path = get_target_path(self._bundle_path, self._uuid, path)
                except PathException as e:
                    reply_error(httplib.BAD_REQUEST, e.message)
                    return

                if read_type == 'stream_directory':
                    if path:
                        exclude_names = []
                    else:
                        exclude_names = self._dep_paths
                    with closing(tar_gzip_directory(final_path, exclude_names=exclude_names)) as fileobj:
                        self._bundle_service.reply_data(self._worker.id, socket_id, {}, fileobj)
                elif read_type == 'stream_file':
                    with closing(gzip_file(final_path)) as fileobj:
                        self._bundle_service.reply_data(self._worker.id, socket_id, {}, fileobj)
                elif read_type == 'read_file_section':
                    string = gzip_string(read_file_section(
                        final_path, read_args['offset'], read_args['length']))
                    self._bundle_service.reply_data(self._worker.id, socket_id, {}, string)
                elif read_type == 'summarize_file':
                    string = gzip_string(summarize_file(
                        final_path, read_args['num_head_lines'],
                        read_args['num_tail_lines'], read_args['max_line_length'],
                        read_args['truncation_text']))
                    self._bundle_service.reply_data(self._worker.id, socket_id, {}, string)
        except BundleServiceException:
            traceback.print_exc()
        except Exception as e:
            traceback.print_exc()
            reply_error(httplib.INTERNAL_SERVER_ERROR, e.message)

    def write(self, subpath, string):
        # Make sure you're not trying to write over a dependency.
        if os.path.normpath(subpath) in self._dep_paths:
            return

        # Do the write.
        with open(os.path.join(self._bundle_path, subpath), 'w') as f:
            f.write(string)

    def kill(self, message):
        with self._kill_lock:
            self._killed = True
            self._kill_message = message

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
