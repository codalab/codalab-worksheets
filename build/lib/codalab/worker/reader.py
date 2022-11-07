from contextlib import closing
import http.client
import os
import threading

import codalab.worker.download_util as download_util
from codalab.worker.download_util import get_target_path, PathException, BundleTarget
from codalab.worker.file_util import (
    gzip_file,
    gzip_bytestring,
    read_file_section,
    summarize_file,
    tar_gzip_directory,
)


class Reader(object):
    def __init__(self):
        self.read_handlers = {
            'get_target_info': self.get_target_info,
            'stream_directory': self.stream_directory,
            'stream_file': self.stream_file,
            'read_file_section': self.read_file_section,
            'summarize_file': self.summarize_file,
        }
        self.read_threads = []  # Threads

    def read(self, run_state, path, read_args, reply):
        read_type = read_args['type']
        handler = self.read_handlers.get(read_type, None)
        if handler:
            handler(run_state, path, read_args, reply)
        else:
            err = (http.client.BAD_REQUEST, "Unsupported read_type for read: %s" % read_type)
            reply(err)

    def stop(self):
        for thread in self.read_threads:
            thread.join()

    def _threaded_read(self, run_state, path, stream_fn, reply_fn):
        """
        Given a run state, a path, a stream function and a reply function,
            - Computes the real filesystem path to the path in the bundle
            - In case of error, invokes reply_fn with an http error
            - Otherwise starts a thread calling stream_fn on the computed final path
        """
        try:
            final_path = get_target_path(
                run_state.bundle_path, BundleTarget(run_state.bundle.uuid, path)
            )
        except PathException as e:
            reply_fn((http.client.NOT_FOUND, str(e)), None, None)
        read_thread = threading.Thread(target=stream_fn, args=[final_path])
        read_thread.start()
        self.read_threads.append(read_thread)

    def get_target_info(self, run_state, path, args, reply_fn):
        """
        Return target_info of path in bundle as a message on the reply_fn
        """
        target_info = None
        dep_paths = set([dep.child_path for dep in run_state.bundle.dependencies])

        # if path is a dependency raise an error
        if path and os.path.normpath(path) in dep_paths:
            err = (
                http.client.NOT_FOUND,
                '{} not found in bundle {}'.format(path, run_state.bundle.uuid),
            )
            reply_fn(err, None, None)
            return
        else:
            try:
                target_info = download_util.get_target_info(
                    run_state.bundle_path, BundleTarget(run_state.bundle.uuid, path), args['depth']
                )
            except PathException as e:
                err = (http.client.NOT_FOUND, str(e))
                reply_fn(err, None, None)
                return

        if not path and args['depth'] > 0:
            target_info['contents'] = [
                child for child in target_info['contents'] if child['name'] not in dep_paths
            ]
        # Object is not JSON serializable so submit its dict in API response
        # The client is responsible for deserializing it
        target_info['resolved_target'] = target_info['resolved_target'].__dict__
        reply_fn(None, {'target_info': target_info}, None)

    def stream_directory(self, run_state, path, args, reply_fn):
        """
        Stream the directory at path using a separate thread
        """
        dep_paths = set([dep.child_path for dep in run_state.bundle.dependencies])
        exclude_names = [] if path else dep_paths

        def stream_thread(final_path):
            with closing(tar_gzip_directory(final_path, exclude_names=exclude_names)) as fileobj:
                reply_fn(None, {}, fileobj)

        self._threaded_read(run_state, path, stream_thread, reply_fn)

    def stream_file(self, run_state, path, args, reply_fn):
        """
        Stream the file  at path using a separate thread
        """

        def stream_file(final_path):
            with closing(gzip_file(final_path)) as fileobj:
                reply_fn(None, {}, fileobj)

        self._threaded_read(run_state, path, stream_file, reply_fn)

    def read_file_section(self, run_state, path, args, reply_fn):
        """
        Read the section of file at path of length args['length'] starting at
        args['offset'] (bytes) using a separate thread
        """

        def read_file_section_thread(final_path):
            bytestring = gzip_bytestring(
                read_file_section(final_path, args['offset'], args['length'])
            )
            reply_fn(None, {}, bytestring)

        self._threaded_read(run_state, path, read_file_section_thread, reply_fn)

    def summarize_file(self, run_state, path, args, reply_fn):
        """
        Summarize the file including args['num_head_lines'] and
        args['num_tail_lines'] but limited with args['max_line_length'] using
        args['truncation_text'] on a separate thread
        """

        def summarize_file_thread(final_path):
            bytestring = gzip_bytestring(
                summarize_file(
                    final_path,
                    args['num_head_lines'],
                    args['num_tail_lines'],
                    args['max_line_length'],
                    args['truncation_text'],
                ).encode()
            )
            reply_fn(None, {}, bytestring)

        self._threaded_read(run_state, path, summarize_file_thread, reply_fn)
