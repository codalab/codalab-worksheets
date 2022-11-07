import logging
import os
from contextlib import closing

from codalab.common import (
    http_error_to_exception,
    precondition,
    UsageError,
    NotFoundError,
    parse_linked_bundle_url,
)
from codalab.worker import download_util
from codalab.worker.bundle_state import State
from codalab.worker.un_gzip_stream import un_gzip_stream

logger = logging.getLogger(__name__)


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
            if str(e) == download_util.BUNDLE_NO_LONGER_RUNNING_MESSAGE:
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
        from codalab.worker import file_util

        self._bundle_model = bundle_model
        self._worker_model = worker_model
        self._bundle_store = bundle_store
        self.file_util = file_util

    @retry_if_no_longer_running
    def get_target_info(self, target, depth):
        """
        Returns information about an individual target inside the bundle,
        If the path is not found within the bundle files, checks whether the path
        points to one of the dependencies of the bundle, and if so, recursively
        tries to get the information for the path within that dependency bundle.

        :param target: a download_util.BundleTarget containing the bundle UUID and subpath

        Raises NotFoundError if the bundle or the path within the bundle is not found

        For information about the format of the return value, see
        worker.download_util.get_target_info.
        """
        try:
            return self._get_target_info_within_bundle(target, depth)
        except NotFoundError as err:
            # if path not in bundle, check if it matches one of its dependencies
            child_path_to_dep = {
                dep.child_path: dep
                for dep in self._bundle_model.get_bundle_dependencies(target.bundle_uuid)
            }
            matching_dep = child_path_to_dep.get(target.subpath.split(os.path.sep)[0])
            if matching_dep:
                # The path actually belongs to a dependency of this bundle
                # Get the subpath of the dependency, and the subpath requested in this call and join them
                # ie if dependency is key:dep-bundle/dep-subpath and the requested path is bundle/key/path-subpath
                # call get_target_info((dep-bundle, dep-subpath/path-subpath))
                parent_path = matching_dep.parent_path
                parent_subpath = target.subpath.split(os.path.sep)[1:]
                if parent_path:
                    new_path = os.path.sep.join([parent_path] + parent_subpath)
                else:
                    new_path = os.path.sep.join(parent_subpath)
                return self.get_target_info(
                    download_util.BundleTarget(matching_dep.parent_uuid, new_path), depth
                )
            raise err
        except Exception as ex:
            raise NotFoundError(str(ex))

    def _get_target_info_within_bundle(self, target, depth):
        """
        Helper for get_target_info that only checks for the target info within that bundle
        without considering the path might be pointing to one of the dependencies.
        Raises NotFoundError if the path is not found.
        """
        bundle_state = self._bundle_model.get_bundle_state(target.bundle_uuid)
        bundle_link_url = self._bundle_model.get_bundle_metadata(
            [target.bundle_uuid], "link_url"
        ).get(target.bundle_uuid)
        if bundle_link_url:
            bundle_link_url = self._transform_link_path(bundle_link_url)
        # Raises NotFoundException if uuid is invalid
        if bundle_state == State.PREPARING:
            raise NotFoundError(
                "Bundle {} hasn't started running yet, files not available".format(
                    target.bundle_uuid
                )
            )
        elif bundle_state != State.RUNNING:
            bundle_path = bundle_link_url or self._bundle_store.get_bundle_location(
                target.bundle_uuid
            )
            try:
                return download_util.get_target_info(bundle_path, target, depth)
            except download_util.PathException as err:
                raise NotFoundError(str(err))
        else:
            # get_target_info calls are sent to the worker even on a shared file
            # system since 1) due to NFS caching the worker has more up to date
            # information on directory contents, and 2) the logic of hiding
            # the dependency paths doesn't need to be re-implemented here.
            worker = self._bundle_model.get_bundle_worker(target.bundle_uuid)
            response_socket_id = self._worker_model.allocate_socket(
                worker['user_id'], worker['worker_id']
            )
            try:
                read_args = {'type': 'get_target_info', 'depth': depth}
                self._send_read_message(worker, response_socket_id, target, read_args)
                with closing(self._worker_model.start_listening(response_socket_id)) as sock:
                    result = self._worker_model.get_json_message(sock, 60)
                if result is None:  # dead workers are a fact of life now
                    logging.info('Unable to reach worker, bundle state {}'.format(bundle_state))
                    raise NotFoundError(
                        'Unable to reach worker of running bundle with bundle state {}'.format(
                            bundle_state
                        )
                    )
                elif 'error_code' in result:
                    raise http_error_to_exception(result['error_code'], result['error_message'])
                target_info = result['target_info']
                # Deserialize dict response sent over JSON
                target_info['resolved_target'] = download_util.BundleTarget.from_dict(
                    target_info['resolved_target']
                )
                return target_info
            finally:
                self._worker_model.deallocate_socket(response_socket_id)

    @retry_if_no_longer_running
    def stream_tarred_gzipped_directory(self, target):
        """
        Returns a file-like object containing a tarred and gzipped archive
        of the given directory.
        """
        bundle_state = self._bundle_model.get_bundle_state(target.bundle_uuid)
        # Raises NotFoundException if uuid is invalid

        if bundle_state == State.PREPARING:
            raise NotFoundError(
                "Bundle {} hasn't started running yet, files not available".format(
                    target.bundle_uuid
                )
            )
        elif bundle_state != State.RUNNING:
            directory_path = self._get_target_path(target)
            with self.file_util.OpenFile(directory_path, gzipped=True) as f:
                return f
        else:
            # stream_tarred_gzipped_directory calls are sent to the worker even
            # on a shared filesystem since
            # 1) due to NFS caching the worker has more up to date
            #   information on directory contents
            # 2) the logic of hiding
            #   the dependency paths doesn't need to be re-implemented here.
            worker = self._bundle_model.get_bundle_worker(target.bundle_uuid)
            response_socket_id = self._worker_model.allocate_socket(
                worker['user_id'], worker['worker_id']
            )
            try:
                read_args = {'type': 'stream_directory'}
                self._send_read_message(worker, response_socket_id, target, read_args)
                fileobj = self._get_read_response_stream(response_socket_id)
                return Deallocating(fileobj, self._worker_model, response_socket_id)
            except Exception:
                self._worker_model.deallocate_socket(response_socket_id)
                raise

    @retry_if_no_longer_running
    def stream_file(self, target, gzipped):
        """
        Returns a file-like object reading the given file. This file is gzipped
        if gzipped is True.
        """
        if self._is_available_locally(target):
            file_path = self._get_target_path(target)
            if gzipped:
                return self.file_util.gzip_file(file_path)
            else:
                with self.file_util.OpenFile(file_path, gzipped=False) as f:
                    return f
        else:
            worker = self._bundle_model.get_bundle_worker(target.bundle_uuid)
            response_socket_id = self._worker_model.allocate_socket(
                worker['user_id'], worker['worker_id']
            )
            try:
                read_args = {'type': 'stream_file'}
                self._send_read_message(worker, response_socket_id, target, read_args)
                fileobj = self._get_read_response_stream(response_socket_id)
                if not gzipped:
                    fileobj = un_gzip_stream(fileobj)
                return Deallocating(fileobj, self._worker_model, response_socket_id)
            except Exception:
                self._worker_model.deallocate_socket(response_socket_id)
                raise

    @retry_if_no_longer_running
    def read_file_section(self, target, offset, length, gzipped):
        """
        Reads length bytes of the file at the given path in the bundle.
        The result is gzipped if gzipped is True.
        """
        if self._is_available_locally(target):
            file_path = self._get_target_path(target)
            bytestring = self.file_util.read_file_section(file_path, offset, length)
            if gzipped:
                bytestring = self.file_util.gzip_bytestring(bytestring)
            return bytestring
        else:
            worker = self._bundle_model.get_bundle_worker(target.bundle_uuid)
            response_socket_id = self._worker_model.allocate_socket(
                worker['user_id'], worker['worker_id']
            )
            try:
                read_args = {'type': 'read_file_section', 'offset': offset, 'length': length}
                self._send_read_message(worker, response_socket_id, target, read_args)
                bytestring = self._get_read_response(response_socket_id)
            finally:
                self._worker_model.deallocate_socket(response_socket_id)

            # Note: all data from the worker is gzipped (see `local_reader.py`).
            if not gzipped:
                bytestring = self.file_util.un_gzip_bytestring(bytestring)
            return bytestring

    @retry_if_no_longer_running
    def summarize_file(
        self, target, num_head_lines, num_tail_lines, max_line_length, truncation_text, gzipped
    ):
        """
        Summarizes the file at the given path in the bundle, returning bytes
        containing the given numbers of lines from beginning and end of the file.
        If the file needs to be truncated, places truncation_text at the
        truncation point.
        The return value is gzipped if gzipped is True.
        """
        if self._is_available_locally(target):
            file_path = self._get_target_path(target)
            # Note: summarize_file returns string, but make it bytes for consistency.
            bytestring = self.file_util.summarize_file(
                file_path, num_head_lines, num_tail_lines, max_line_length, truncation_text
            ).encode()
            if gzipped:
                bytestring = self.file_util.gzip_bytestring(bytestring)
            return bytestring
        else:
            worker = self._bundle_model.get_bundle_worker(target.bundle_uuid)
            response_socket_id = self._worker_model.allocate_socket(
                worker['user_id'], worker['worker_id']
            )
            try:
                read_args = {
                    'type': 'summarize_file',
                    'num_head_lines': num_head_lines,
                    'num_tail_lines': num_tail_lines,
                    'max_line_length': max_line_length,
                    'truncation_text': truncation_text,
                }
                self._send_read_message(worker, response_socket_id, target, read_args)
                bytestring = self._get_read_response(response_socket_id)
            finally:
                self._worker_model.deallocate_socket(response_socket_id)

            # Note: all data from the worker is gzipped (see `local_reader.py`).
            if not gzipped:
                bytestring = self.file_util.un_gzip_bytestring(bytestring)
            return bytestring

    def netcat(self, uuid, port, message):
        """
        Sends a raw bytestring into the specified port of a running bundle, then return the response.
        """
        worker = self._bundle_model.get_bundle_worker(uuid)
        response_socket_id = self._worker_model.allocate_socket(
            worker['user_id'], worker['worker_id']
        )
        try:
            self._send_netcat_message(worker, response_socket_id, uuid, port, message)
            bytestring = self._get_read_response(response_socket_id)
        finally:
            self._worker_model.deallocate_socket(response_socket_id)

        return bytestring

    def _is_available_locally(self, target):
        """Returns whether the target is accessible from the current machine. Returns True
        if the target is on an accessible disk or if the target is on Azure Blob Storage.
        """
        file_path = self._get_target_path(target)
        if parse_linked_bundle_url(file_path).uses_beam:
            # Return True if the URL is in Azure Blob Storage.
            return True
        if self._bundle_model.get_bundle_state(target.bundle_uuid) in [
            State.RUNNING,
            State.PREPARING,
        ]:
            return self._bundle_model.get_bundle_worker(target.bundle_uuid)['shared_file_system']
        return True

    def _transform_link_path(self, path):
        """Transforms a link file path to its properly mounted path.
        Every file path is mounted to a path with "/opt/codalab-worksheets-link-mounts"
        prepended to it.
        """
        return f"/opt/codalab-worksheets-link-mounts{path}"

    def _get_target_path(self, target):
        bundle_link_url = self._bundle_model.get_bundle_metadata(
            [target.bundle_uuid], "link_url"
        ).get(target.bundle_uuid)
        if bundle_link_url:
            # If bundle_link_url points to a locally mounted volume, call _transform_link_path
            # to get the actual path where it can be accessed.
            bundle_link_url = self._transform_link_path(bundle_link_url)
        bundle_path = bundle_link_url or self._bundle_store.get_bundle_location(target.bundle_uuid)
        try:
            path = download_util.get_target_path(bundle_path, target)
            return path
        except download_util.PathException as e:
            raise UsageError(str(e))

    def get_target_bypass_url(self, target, **kwargs):
        """
        Get SAS url with read permission. Used for bypass server downloading from Azure blob storage.
        """
        return parse_linked_bundle_url(self._get_target_path(target)).bundle_path_bypass_url(
            permission='r', **kwargs
        )

    def _send_read_message(self, worker, response_socket_id, target, read_args):
        message = {
            'type': 'read',
            'socket_id': response_socket_id,
            'uuid': target.bundle_uuid,
            'path': target.subpath,
            'read_args': read_args,
        }
        if not self._worker_model.send_json_message(
            worker['socket_id'], message, 60
        ):  # dead workers are a fact of life now
            logging.info('Unable to reach worker')

    def _send_netcat_message(self, worker, response_socket_id, uuid, port, message):
        message = {
            'type': 'netcat',
            'socket_id': response_socket_id,
            'uuid': uuid,
            'port': port,
            'message': message,
        }
        if not self._worker_model.send_json_message(
            worker['socket_id'], message, 60
        ):  # dead workers are a fact of life now
            logging.info('Unable to reach worker')

    def _get_read_response_stream(self, response_socket_id):
        with closing(self._worker_model.start_listening(response_socket_id)) as sock:
            header_message = self._worker_model.get_json_message(sock, 60)
            precondition(header_message is not None, 'Unable to reach worker')
            if 'error_code' in header_message:
                raise http_error_to_exception(
                    header_message['error_code'], header_message['error_message']
                )

            fileobj = self._worker_model.get_stream(sock, 60)
            precondition(fileobj is not None, 'Unable to reach worker')
            return fileobj

    def _get_read_response(self, response_socket_id):
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
