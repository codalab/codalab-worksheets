import os
import unittest
from mock import Mock
import fake_filesystem_unittest

from codalabworker.local_run.local_reader import LocalReader

class LocalReaderTest(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()
        self.reader = LocalReader()

    def tearDown(self):
        pass

    def test_get_target_info(self):
        """
        get_target_info(run_state, path, dep_paths, args, reply_fn):
            - run_state: uuid, bundle_path
            - path: path in bundle to look up
            - dep_paths: figure out what these are
            - args: Anything besides depth?
            - reply_fn: This should be called with (err, message, data)
        """
        pass

    def test_stream_directory(self):
        """
        stream_directory(run_state, path, dep_paths, args, reply_fn):
            - run_state: uuid, bundle_path
            - path: path in bundle to look up
            - dep_paths: figure out what these are
            - args: Not used
            - reply_fn: This should be called with (err, message, data)
        """
        pass

    def test_stream_file(self):
        """
        stream_file(run_state, path, dep_paths, args, reply_fn):
            - run_state: uuid, bundle_path
            - path: path in bundle to look up
            - dep_paths: figure out what these are
            - args: Not used
            - reply_fn: This should be called with (err, message, data)
        Only difference from above is gzip function call
        """
        pass

    def test_read_file_section(self):
        """
        read_file_section(run_state, path, dep_paths, args, reply_fn):
            - run_state: uuid, bundle_path
            - path: path in bundle to look up
            - dep_paths: unused
            - args: offset, length
            - reply_fn: This should be called with (err, message, data)
        """
        pass

    def test_summarize_file(self):
        """
        summarize_file(run_state, path, dep_paths, args, reply_fn):
            - run_state: uuid, bundle_path
            - path: path in bundle to look up
            - dep_paths: ununsed
            - args: num_head_lines, num_tail_lines, max_line_length, truncation_text
            - reply_fn: This should be called with (err, message, data)
        """
        pass
