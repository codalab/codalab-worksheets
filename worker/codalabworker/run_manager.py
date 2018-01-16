from contextlib import closing
import httplib
import os
import traceback
import threading

from bundle_service_client import BundleServiceException
from download_util import get_target_info, get_target_path, PathException
from file_util import gzip_file, gzip_string, read_file_section, summarize_file, tar_gzip_directory


# TODO Should this be run factory instead if all it does is creation?
class RunManagerBase(object):
    """
    Base class for classes which manages individual runs on a worker.
    Each worker has a single run manager which it uses to create, start, resume, and monitor runs.

    Different implementation of this class will execute the runs in different ways.
    For example, one run manager may submit to the local docker socket while another submits to a managed cloud compute
    service.
    """

    @property
    def cpus(self):
        """
        :return: The total available cpus for this RunManager.
        """
        raise NotImplementedError

    @property
    def memory(self):
        """
        :return: The total available memory, in megabytes, for this RunManager.
        """
        raise NotImplementedError

    @property
    def gpus(self):
        """
        :return: The total available cpus for this RunManager.
        """
        raise NotImplementedError

    def create_run(self, bundle, bundle_path, resources):
        """
        Creates a new run which when started will execute the provided run bundle
        :param bundle: the run bundle to execute
        :param bundle_path: path on the filesystem where the bundles data is stored
        :param resources: the resources requested for this run bundle, e.g. cpu, memory, docker image
        :return: a new Run which will execute the provided run bundle
        """
        raise NotImplementedError

    def serialize(self, run):
        """
        Serialize the run in order to persist its state.
        This is used so that workers can pickup where they left off if they are killed.
        :param run: the run to serialize
        :return: a dict of the serialized data for the run
        """
        raise NotImplementedError

    def deserialize(self, run_data):
        """
        Deserialize run data into a run instance.
        It is expected that the data is from a call to RunManager.serialize(run)
        :param run_data: the serialized run data
        :return: a new run instance which was represented by the data
        """
        raise NotImplementedError


class RunBase(object):
    """
    Base class for classes which represent an executable run bundle.
    These are returned from a RunManager and common methods for manipulating the run.
    """

    @property
    def bundle(self):
        raise NotImplementedError

    @property
    def resources(self):
        raise NotImplementedError

    @property
    def dependency_paths(self):
        """
        :return: A list of filesystem paths to all dependencies.
        """
        return set([dep['child_path'] for dep in self.bundle['dependencies']])

    @property
    def bundle_path(self):
        """
        :return: The filesystem path to the bundle.
        """
        raise NotImplementedError

    @property
    def requested_memory_bytes(self):
        """
        If request_memory is defined, then return that.
        Otherwise, this run's memory usage does not get checked, so return inf.
        """
        return self.resources.get('request_memory') or float('inf')

    def start(self):
        """
        Start this run asynchronously.
        :return: True if the run was started, False otherwise.
        """
        raise NotImplementedError

    def resume(self):
        """
        Resume this run asynchronously.
        This is used primarily after a worker has deserialized a saved run and then wants to continue it.
        :return: True if the run could be resumed, False otherwise
        """
        raise NotImplementedError

    def kill(self):
        """
        Kill this run if it is started.
        :return: True if the run was killed, False otherwise
        """
        raise NotImplementedError

    def read(self, path, read_args, socket):
        """
        Read the data at the path and send it back over the socket.
        More than likely this is done asynchronously.
        :param path: The path to the data to be read. Refers to a path from this runs bundle.
        :param read_args: A dict with parameters about how to read the data.
        :param socket: A SocketConnection to send the read data to.
        :return: True if success, False otherwise
        """
        raise NotImplementedError

    def write(self, subpath, data):
        """
        Write the data to the specified subpath of this runs bundle.
        :param subpath: Path to write the data at.
        :param data: The data to be written.
        :return: True if success, False otherwise.
        """
        raise NotImplementedError


class FilesystemRunMixin(object):
    """
    Mixin which implements some methods for runs which store their data on the worker machines filesystem.
    """
    def __init__(self):
        self._dependencies = None

    @property
    def is_shared_file_system(self):
        raise NotImplementedError

    def setup_dependencies(self):
        """
        Set up and return the dependencies for this run bundle.
        :return: A list of dependencies which are tuples of the form (local_path, docker_path, bundle_uuid)
        """
        # If dependencies have already been setup, just return them
        if self._dependencies is not None:
            return self._dependencies

        dependencies = []
        docker_dependencies_directory = os.path.join('/', self.bundle['uuid'] + '_dependencies')
        for dep in self.bundle['dependencies']:
            child_path = os.path.normpath(os.path.join(self.bundle_path, dep['child_path']))
            if not child_path.startswith(self.bundle_path):
                raise Exception('Invalid key for dependency: %s' % dep['child_path'])

            if self.is_shared_file_system:
                parent_bundle_path = dep['location']

                # Check that the dependency is valid (i.e. points inside the
                # bundle and isn't a broken symlink).
                parent_bundle_path = os.path.realpath(parent_bundle_path)
                dependency_path = os.path.realpath(os.path.join(parent_bundle_path, dep['parent_path']))
                if not (dependency_path.startswith(parent_bundle_path) and os.path.exists(dependency_path)):
                    raise Exception('Invalid dependency %s/%s' % (dep['parent_uuid'], dep['parent_path']))
            else:
                raise Exception('Only shared file system is supported for now')

            docker_dependency_path = os.path.join(docker_dependencies_directory, dep['child_path'])
            os.symlink(docker_dependency_path, child_path)
            dependencies.append((dependency_path, docker_dependency_path, dep['parent_uuid']))

        self._dependencies = dependencies
        return self._dependencies

    def read(self, path, read_args, socket):
        # Reads may take a long time, so do the read in a separate thread.
        threading.Thread(target=read_from_filesystem, args=(self, path, read_args, socket)).start()
        return True

    def write(self, subpath, string):
        # Make sure you're not trying to write over a dependency.
        if os.path.normpath(subpath) in self.dependency_paths:
            return False

        # Do the write.
        with open(os.path.join(self.bundle_path, subpath), 'w') as f:
            f.write(string)
        return True


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
