from contextlib import closing
import httplib
import os
import threading

from codalabworker.run_manager import Reader
from codalabworker.download_util import (
        BUNDLE_NO_LONGER_RUNNING_MESSAGE,
        get_target_info,
        get_target_path,
        PathException
)
from codalabworker.file_util import (
    un_tar_directory,
    get_path_size,
    gzip_file,
    gzip_string,
    read_file_section,
    summarize_file,
    tar_gzip_directory,
    remove_path,
)

class LocalReader(Reader):
    def _threaded_read(self, run_state, path, stream_fn, reply_fn):
        try:
            final_path = get_target_path(run_state.bundle_path, run_state.bundle['uuid'], path)
        except PathException as e:
            reply_fn((httplib.BAD_REQUEST, e.message), None, None)
        threading.Thread(target=stream_fn, args=[final_path]).start()

    def get_target_info(self, run_state, path, dep_paths, args, reply_fn):
        bundle_uuid = run_state.bundle['uuid']
        # At the top-level directory, we should ignore dependencies.
        if path and os.path.normpath(path) in dep_paths:
            target_info = None
        else:
            try:
                target_info = get_target_info(
                    run_state.bundle_path, bundle_uuid, path, args['depth'])
            except PathException as e:
                err = (httplib.BAD_REQUEST, e.message)
                reply_fn(err, None, None)

            if target_info is not None and not path and args['depth'] > 0:
                target_info['contents'] = [
                    child for child in target_info['contents']
                    if child['name'] not in dep_paths]

        reply_fn(None, {'target_info': target_info}, None)

    def stream_directory(self, run_state, path, dep_paths, args, reply_fn):
        exclude_names = [] if path else dep_paths
        def stream_thread(final_path):
            with closing(tar_gzip_directory(final_path, exclude_names=exclude_names)) as fileobj:
                reply_fn(None, {}, fileobj)
        self._threaded_read(run_state, path, stream_thread, reply_fn)

    def stream_file(self, run_state, path, dep_paths, args, reply_fn):
        def stream_file(final_path):
            with closing(gzip_file(final_path)) as fileobj:
                reply_fn(None, {}, fileobj)
        self._threaded_read(run_state, path, stream_file, reply_fn)

    def read_file_section(self, run_state, path, dep_paths, args, reply_fn):
        def read_file_section_thread(final_path):
            string = gzip_string(read_file_section(
                final_path, args['offset'], args['length']))
            reply_fn(None, {}, string)
        self._threaded_read(run_state, path, read_file_section_thread, reply_fn)

    def summarize_file(self, run_state, path, dep_paths, args, reply_fn):
        def summarize_file_thread(final_path):
            string = gzip_string(summarize_file(
                final_path, args['num_head_lines'],
                args['num_tail_lines'], args['max_line_length'],
                args['truncation_text']))
            reply_fn(None, {}, string)
        self._threaded_read(run_state, path, summarize_file_thread, reply_fn)
