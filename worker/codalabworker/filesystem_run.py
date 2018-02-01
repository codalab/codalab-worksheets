from contextlib import closing
import httplib
import os
import traceback
import threading
import logging

import time
from bundle_service_client import BundleServiceException
from download_util import get_target_info, get_target_path, PathException
from file_util import gzip_file, gzip_string, read_file_section, summarize_file, tar_gzip_directory, get_path_size
from formatting import *
from fsm import State, ThreadedFiniteStateMachine


class FilesystemRunMixin(object):
    """
    Mixin which implements some methods for runs which store their data on the worker machines filesystem.
    """
    def __init__(self):
        self._dependencies = None

    @property
    def is_shared_file_system(self):
        raise NotImplementedError

    @property
    def docker_working_directory(self):
        return os.path.join('/', self.bundle['uuid'])

    def setup_dependencies(self):
        """
        Set up and return the dependencies for this run bundle.

        For example, a run bundle with uuid 0x111111 with dependencies foo:0xaaaa 0xbbbbb
        would return [(BUNDLE_DIR/0xaaaa, /0x111111/foo, 0xaaaa), (BUNDLE_DIR/0xbbbbb, /0x111111/0xbbbbb, 0xbbbbb)]

        :return: A list of dependencies which are tuples of the form (local_path, docker_path, bundle_uuid)
        """
        # If dependencies have already been setup, just return them
        if self._dependencies is not None:
            return self._dependencies

        dependencies = []
        # Mount the dependencies directly into the working directory
        for dep in self.bundle['dependencies']:
            child_path = os.path.normpath(os.path.join(self.bundle_path, dep['child_path']))
            if not child_path.startswith(self.bundle_path):
                raise Exception('Invalid key for dependency: %s' % dep['child_path'])

            dependency_path = self._get_or_download_dependency(dep)

            docker_dependency_path = os.path.join(self.docker_working_directory, dep['child_path'])
            dependency_name = dep['child_path']
            dependencies.append((dependency_path, docker_dependency_path, dependency_name))

        self._dependencies = dependencies
        return self._dependencies

    def cleanup_dependencies(self):
        logging.debug('Cleaning up dependencies for run %s' % (self.bundle['uuid']))

        for dep in self.bundle['dependencies']:
            dependency_mount_folder = '%s/%s' % (self.docker_working_directory, dep['child_path'])
            try:
                os.rmdir(dependency_mount_folder)
            except OSError:
                logging.exception("Failed to remove dependency folder %s", dependency_mount_folder)

            # Remove any dependencies added if not shared filesystem
            if not self.is_shared_file_system:
                self._worker.remove_dependency(dep['parent_uuid'], dep['parent_path'], self.bundle['uuid'])

            # Since we mount the dependencies directly, it creates extra folders which we need to cleanup
            dependency_mount_folder = '%s/%s' % (self.docker_working_directory, dep['child_path'])
            try:
                os.rmdir(dependency_mount_folder)
            except OSError:
                logging.exception("Failed to remove dependency folder %s", dependency_mount_folder)

    def _get_or_download_dependency(self, dep):
        """
        Gets the path to the dependency, possibly downloading it if need be.
        """
        if self.is_shared_file_system:
            parent_bundle_path = os.path.realpath(dep['location'])
            dep_path = os.path.realpath(os.path.join(parent_bundle_path, dep['parent_path']))
            if not (dep_path.startswith(parent_bundle_path) and os.path.exists(dep_path)):
                raise Exception('Invalid dep %s/%s' % (dep['parent_uuid'], dep['parent_path']))
            return dep_path
        else:
            return self.download_dependency(dep['parent_uuid'], dep['parent_path'])

    def download_dependency(self, uuid, path):
        """
        Download the specified uuid/path and return the path it was downloaded to.
        :param uuid: bundle uuid to download from
        :param path: the path into the bundle to download
        :return: the path the data was downloaded to
        """
        raise NotImplementedError

    def read(self, path, read_args, socket):
        # Reads may take a long time, so do the read in a separate thread.
        threading.Thread(target=read_from_filesystem, args=(self, path, read_args, socket)).start()

    def write(self, subpath, string):
        # Make sure you're not trying to write over a dependency.
        if os.path.normpath(subpath) in self.dependency_paths:
            return False

        # Do the write.
        with open(os.path.join(self.bundle_path, subpath), 'w') as f:
            f.write(string)
        return True

    def pre_start(self):
        self.setup_dependencies()

    def post_stop(self):
        self.cleanup_dependencies()

        # Upload the data if needed
        if not self._worker.shared_file_system:
            uuid = self.bundle['uuid']
            logging.debug('Uploading results for run with UUID %s', uuid)

            def update_status(bytes_uploaded):
                logging.debug('Uploading results: %s done (archived size)' % size_str(bytes_uploaded))

            self._bundle_service.update_bundle_contents(self._worker.id, uuid, self._bundle_path, update_status)


def read_from_filesystem(run, path, read_args, socket):
    def reply_error(code, message):
        message = {
            'error_code': code,
            'error_message': message,
        }
        socket.reply(message)

    try:
        read_type = read_args['type']
        if read_type == 'get_target_info':
            # At the top-level directory, we should ignore dependencies.
            if path and os.path.normpath(path) in run.dependency_paths:
                target_info = None
            else:
                try:
                    target_info = get_target_info(
                        run.bundle_path, run.bundle['uuid'], path, read_args['depth'])
                except PathException as e:
                    reply_error(httplib.BAD_REQUEST, e.message)
                    return

                if not path and read_args['depth'] > 0:
                    target_info['contents'] = [
                        child for child in target_info['contents']
                        if child['name'] not in run.dependency_paths]

            socket.reply({'target_info': target_info})
        else:
            try:
                final_path = get_target_path(run.bundle_path, run.bundle['uuid'], path)
            except PathException as e:
                reply_error(httplib.BAD_REQUEST, e.message)
                return

            if read_type == 'stream_directory':
                if path:
                    exclude_names = []
                else:
                    exclude_names = run.dependency_paths
                with closing(tar_gzip_directory(final_path, exclude_names=exclude_names)) as fileobj:
                    socket.reply_data({}, fileobj)
            elif read_type == 'stream_file':
                with closing(gzip_file(final_path)) as fileobj:
                    socket.reply_data({}, fileobj)
            elif read_type == 'read_file_section':
                string = gzip_string(read_file_section(
                    final_path, read_args['offset'], read_args['length']))
                socket.reply_data({}, string)
            elif read_type == 'summarize_file':
                string = gzip_string(summarize_file(
                    final_path, read_args['num_head_lines'],
                    read_args['num_tail_lines'], read_args['max_line_length'],
                    read_args['truncation_text']))
                socket.reply_data({}, string)
    except BundleServiceException:
        traceback.print_exc()
    except Exception as e:
        traceback.print_exc()
        reply_error(httplib.INTERNAL_SERVER_ERROR, e.message)


class FilesystemBundleMonitor(object):
    """
    Helper to monitor the filesystem where a run bundle is being written.
    """
    def __init__(self, bundle_path, dependencies):
        state = FilesystemBundleMonitor.Monitor(bundle_path, dependencies)
        self._fsm = ThreadedFiniteStateMachine(state, daemonic=True)

    @property
    def disk_utilization(self):
        return self._fsm._state._disk_utilization

    def start(self):
        return self._fsm.start()

    @property
    def is_alive(self):
        return self._fsm.is_alive

    def stop(self):
        return self._fsm.stop()

    class Monitor(State):
        def __init__(self, bundle_path, dependencies):
            self._bundle_path = bundle_path
            # We exclude the paths which are the dependency folders
            self._exclude_paths = [path for _, _, path in dependencies]
            self._end_time = 0
            self._start_time = 0
            self._disk_utilization = 0

        def update(self):
            start_time = time.time()
            try:
                disk_utilization = get_path_size(self._bundle_path, self._exclude_paths)
                # Setting the instance variable is atomic, so no need to lock
                self._disk_utilization = disk_utilization
            except Exception:
                logging.exception('Problem calculating disk utilization for path %s.', self._bundle_path)
            end_time = time.time()
            self._start_time = start_time
            self._end_time = end_time
            return self

        @property
        def update_period(self):
            # To ensure that we don't hammer the disk for this computation when
            # there are lots of files, we run it at most 10% of the time.
            return max((self._end_time - self._start_time) * 10.0, 1.0)
