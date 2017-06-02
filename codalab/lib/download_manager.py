from contextlib import closing

from codalab.common import http_error_to_exception, precondition, State, UsageError, NotFoundError
from codalabworker import download_util
from codalabworker import file_util


def retry_if_no_longer_running(f):
    """
    Decorator that retries a download if the bundle finishes running in the
    middle of the download, after the download message is sent but before it is
    handled.
    """
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            if e.message == download_util.BUNDLE_NO_LONGER_RUNNING_MESSAGE:
                # Retry just once, since by now the state should already be set
                # to READY / FAILED, unless there's some internal error.
                return f(*args, **kwargs)
            else:
                raise
    return wrapper


class DownloadManager(object):
    """
    Used for downloading the contents of bundles. The main purpose of this class
    is to fetch bundle data, whether it is available locally or needs to be
    downloaded from the worker.

    Note, this class does not check permissions in any way. The caller is
    responsible for doing all required permissions checks.
    """

    def __init__(self, bundle_model, worker_model, bundle_store):
        self._bundle_model = bundle_model
        self._worker_model = worker_model
        self._bundle_store = bundle_store

    @retry_if_no_longer_running
    def get_target_info(self, uuid, path, depth):
        """
        Returns information about an individual target inside the bundle, or
        None if the target or bundle doesn't exist.

        For information about the format of the return value, see
        worker.download_util.get_target_info.
        """
        try:
            bundle_state = self._bundle_model.get_bundle_state(uuid)
        except NotFoundError:
            bundle_state = None

        # Return None if invalid bundle reference
        if bundle_state is None:
            return None
        elif bundle_state != State.RUNNING:
            bundle_path = self._bundle_store.get_bundle_location(uuid)
            try:
                return download_util.get_target_info(bundle_path, uuid, path, depth)
            except download_util.PathException as e:
                raise UsageError(e.message)
        else:
            # get_target_info calls are sent to the worker even on a shared file
            # system since 1) due to NFS caching the worker has more up to date
            # information on directory contents, and 2) the logic of hiding
            # the dependency paths doesn't need to be re-implemented here.
            worker = self._worker_model.get_bundle_worker(uuid)
            response_socket_id = self._worker_model.allocate_socket(worker['user_id'], worker['worker_id'])
            try:
                read_args = {
                    'type': 'get_target_info',
                    'depth': depth,
                }
                self._send_read_message(worker, response_socket_id, uuid, path, read_args)
                with closing(self._worker_model.start_listening(response_socket_id)) as sock:
                    result = self._worker_model.get_json_message(sock, 60)
                precondition(result is not None, 'Unable to reach worker')
                if 'error_code' in result:
                    raise http_error_to_exception(result['error_code'], result['error_message'])
                return result['target_info']
            finally:
                self._worker_model.deallocate_socket(response_socket_id)

    @retry_if_no_longer_running
    def stream_tarred_gzipped_directory(self, uuid, path):
        """
        Returns a file-like object containing a tarred and gzipped archive
        of the given directory.
        """
        if self._is_available_locally(uuid):
            directory_path = self._get_target_path(uuid, path)
            return file_util.tar_gzip_directory(directory_path)
        else:
            worker = self._worker_model.get_bundle_worker(uuid)
            response_socket_id = self._worker_model.allocate_socket(worker['user_id'], worker['worker_id'])
            try:
                read_args = {
                    'type': 'stream_directory',
                }
                self._send_read_message(worker, response_socket_id, uuid, path, read_args)
                fileobj = self._get_read_response_stream(response_socket_id)
                return Deallocating(fileobj, self._worker_model, response_socket_id)
            except:
                self._worker_model.deallocate_socket(response_socket_id)
                raise

    @retry_if_no_longer_running
    def stream_file(self, uuid, path, gzipped):
        """
        Returns a file-like object reading the given file. This file is gzipped
        if gzipped is True.
        """
        if self._is_available_locally(uuid):
            file_path = self._get_target_path(uuid, path)
            if gzipped:
                return file_util.gzip_file(file_path)
            else:
                return open(file_path)
        else:
            worker = self._worker_model.get_bundle_worker(uuid)
            response_socket_id = self._worker_model.allocate_socket(worker['user_id'], worker['worker_id'])
            try:
                read_args = {
                    'type': 'stream_file',
                }
                self._send_read_message(worker, response_socket_id, uuid, path, read_args)
                fileobj = self._get_read_response_stream(response_socket_id)
                if not gzipped:
                    fileobj = file_util.un_gzip_stream(fileobj)
                return Deallocating(fileobj, self._worker_model, response_socket_id)
            except:
                self._worker_model.deallocate_socket(response_socket_id)
                raise

    @retry_if_no_longer_running
    def read_file_section(self, uuid, path, offset, length, gzipped):
        """
        Reads length bytes of the file at the given path in the bundle.
        The result is gzipped if gzipped is True.
        """
        if self._is_available_locally(uuid):
            file_path = self._get_target_path(uuid, path)
            string = file_util.read_file_section(file_path, offset, length)
            if gzipped:
                string = file_util.gzip_string(string)
            return string
        else:
            worker = self._worker_model.get_bundle_worker(uuid)
            response_socket_id = self._worker_model.allocate_socket(worker['user_id'], worker['worker_id'])
            try:
                read_args = {
                    'type': 'read_file_section',
                    'offset': offset,
                    'length': length,
                }
                self._send_read_message(worker, response_socket_id, uuid, path, read_args)
                string = self._get_read_response_string(response_socket_id)
            finally:
                self._worker_model.deallocate_socket(response_socket_id)

            if not gzipped:
                string = file_util.un_gzip_string(string)
            return string

    @retry_if_no_longer_running
    def summarize_file(self, uuid, path, num_head_lines, num_tail_lines, max_line_length, truncation_text, gzipped):
        """
        Summarizes the file at the given path in the bundle, returning a string
        containing the given numbers of lines from beginning and end of the file.
        If the file needs to be truncated, places truncation_text at the
        truncation point.
        This string is gzipped if gzipped is True.
        """
        if self._is_available_locally(uuid):
            file_path = self._get_target_path(uuid, path)
            string = file_util.summarize_file(file_path, num_head_lines, num_tail_lines, max_line_length, truncation_text)
            if gzipped:
                string = file_util.gzip_string(string)
            return string
        else:
            worker = self._worker_model.get_bundle_worker(uuid)
            response_socket_id = self._worker_model.allocate_socket(worker['user_id'], worker['worker_id'])
            try:
                read_args = {
                    'type': 'summarize_file',
                    'num_head_lines': num_head_lines,
                    'num_tail_lines': num_tail_lines,
                    'max_line_length': max_line_length,
                    'truncation_text': truncation_text,
                }
                self._send_read_message(worker, response_socket_id, uuid, path, read_args)
                string = self._get_read_response_string(response_socket_id)
            finally:
                self._worker_model.deallocate_socket(response_socket_id)

            if not gzipped:
                string = file_util.un_gzip_string(string)
            return string

    def _is_available_locally(self, uuid):
        if self._bundle_model.get_bundle_state(uuid) == State.RUNNING:
            if self._worker_model.shared_file_system:
                worker = self._worker_model.get_bundle_worker(uuid)
                return worker['user_id'] == self._bundle_model.root_user_id
            else:
                return False

        return True

    def _get_target_path(self, uuid, path):
        bundle_path = self._bundle_store.get_bundle_location(uuid)
        try:
            return download_util.get_target_path(bundle_path, uuid, path)
        except download_util.PathException as e:
            raise UsageError(e.message)

    def _send_read_message(self, worker, response_socket_id, uuid, path, read_args):
        message = {
            'type': 'read',
            'socket_id': response_socket_id,
            'uuid': uuid,
            'path': path,
            'read_args': read_args,
        }
        precondition(
            self._worker_model.send_json_message(worker['socket_id'], message, 60),
            'Unable to reach worker')

    def _get_read_response_stream(self, response_socket_id):
        with closing(self._worker_model.start_listening(response_socket_id)) as sock:
            header_message = self._worker_model.get_json_message(sock, 60)
            precondition(header_message is not None, 'Unable to reach worker')
            if 'error_code' in header_message:
                raise http_error_to_exception(header_message['error_code'], header_message['error_message'])

            fileobj = self._worker_model.get_stream(sock, 60)
            precondition(fileobj is not None, 'Unable to reach worker')
            return fileobj

    def _get_read_response_string(self, response_socket_id):
        with closing(self._get_read_response_stream(response_socket_id)) as fileobj:
            return fileobj.read()


class Deallocating(object):
    """
    Deallocates the socket when closed.
    """
    def __init__(self, fileobj, worker_model, socket_id):
        self._fileobj = fileobj
        self._worker_model = worker_model
        self._socket_id = socket_id

    def __getattr__(self, attr):
        return getattr(self._fileobj, attr)

    def close(self):
        self._fileobj.close()
        self._worker_model.deallocate_socket(self._socket_id)

