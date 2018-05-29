import os
import unittest
from mock import Mock
import fake_filesystem_unittest

from codalabworker.fsm import JsonStateCommitter

class JsonStateCommitterTest(fake_filesystem_unittest.TestCase):
    def setUp(self):
        self.setUpPyfakefs()
        self.test_dir = '/test'
        self.fs.create_dir(self.test_dir)
        self.state_file = 'test-state.json'
        self.state_path = os.path.join(self.test_dir, self.state_file)
        self.committer = JsonStateCommitter(self.state_path)

    def tearDown(self):
        os.remove(self.state_path)

    def test_path_parsing(self):
        """ Simple test to ensure we don't mess up the state file path"""
        self.assertEqual(self.committer._state_file, self.state_path)

    def test_commit(self):
        """Make sure state is committed correctly"""
        test_state = {'state': 'value'}
        test_state_json_str = '{\"state\": \"value\"}'
        self.committer.commit(test_state)
        with open(self.state_path) as f:
            self.assertEqual(test_state_json_str, f.read())

    def test_load(self):
        """ Make sure load loads the state file if it exists """
        test_state = {'state': 'value'}
        test_state_json_str = '{\"state\": \"value\"}'
        self.fs.create_file(self.state_path, contents=test_state_json_str)
        loaded_state = self.committer.load()
        self.assertDictEqual(test_state, loaded_state)

    def test_default(self):
        """ Make sure load with a default works if state file doesn't exist """
        default_state = {'state': 'value'}
        loaded_state = self.committer.load(default=default_state)
        self.assertDictEqual(default_state, loaded_state)
