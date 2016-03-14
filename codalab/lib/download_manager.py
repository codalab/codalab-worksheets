import os

from codalab.common import UsageError
from codalab.lib import path_util
from worker.download_util import get_and_check_target_path, get_target_path
from worker import file_util


class DownloadManager(object):
    """
    Used for downloading the contents of bundles. The main purpose of this class
    is to fetch bundle data, whether it is available locally or needs to be
    downloaded from the worker.

    Note, this class does not check permissions in any way. The caller is
    responsible for doing all required permissions checks.

    TODO(klopyrev): Worker code in a future pull request.
    """

    def __init__(self, bundle_store):
        self._bundle_store = bundle_store

    def get_target_info(self, uuid, path, depth):
        """
        Returns information about an individual target inside the bundle, or
        None if the target doesn't exist.
        """
        bundle_path = self._bundle_store.get_bundle_location(uuid)
        final_path = get_target_path(bundle_path, path)
        # TODO: This doesn't really check for .., but this code will be
        # deprecated in favor of a version that uses the contents index soon.
        if not os.path.islink(final_path) and not os.path.exists(final_path):
            return None
        return path_util.get_info(final_path, depth)

    def stream_tarred_gzipped_directory(self, uuid, path):
        """
        Returns a file-like object containing a tarred and gzipped archive
        of the given directory.
        """
        directory_path = self._get_and_check_target_path(uuid, path)
        return file_util.tar_gzip_directory(directory_path)

    def stream_file(self, uuid, path, gzipped):
        """
        Returns a file-like object reading the given file. This file is gzipped
        if gzipped is True.
        """
        file_path = self._get_and_check_target_path(uuid, path)
        if gzipped:
            return file_util.gzip_file(file_path)
        else:
            return open(file_path)

    def read_file_section(self, uuid, path, offset, length, gzipped):
        """
        Reads length bytes of the file at the given path in the bundle.
        The result is gzipped if gzipped is True.
        """
        file_path = self._get_and_check_target_path(uuid, path)
        string = file_util.read_file_section(file_path, offset, length)
        if gzipped:
            string = file_util.gzip_string(string)
        return string

    def summarize_file(self, uuid, path, num_head_lines, num_tail_lines, max_line_length, truncation_text, gzipped):
        """
        Summarizes the file at the given path in the bundle, returning a string
        containing the given numbers of lines from beginning and end of the file.
        If the file needs to be truncated, places truncation_text at the
        truncation point.
        This string is gzipped if gzipped is True.
        """
        file_path = self._get_and_check_target_path(uuid, path)
        string = file_util.summarize_file(file_path, num_head_lines, num_tail_lines, max_line_length, truncation_text)
        if gzipped:
            string = file_util.gzip_string(string)
        return string

    def _get_and_check_target_path(self, uuid, path):
        bundle_path = self._bundle_store.get_bundle_location(uuid)
        target_path, error_message = get_and_check_target_path(bundle_path, uuid, path)
        if error_message is not None:
            raise UsageError(error_message)
        return target_path
