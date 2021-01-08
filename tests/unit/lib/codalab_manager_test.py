import os
import unittest
from pathlib import Path

from codalab.lib.codalab_manager import CodaLabManager


class CodalabManagerTest(unittest.TestCase):
    def setUp(self):
        os.environ['CODALAB_HOME'] = str(Path.home())

    def tearDown(self):
        def remove_file_if_exists(file):
            file_path = os.path.join(str(Path.home()), file)
            if os.path.exists(file_path):
                os.remove(file_path)

        # Remove the state and config json files created by the CodaLabManagers
        remove_file_if_exists('state.json')
        remove_file_if_exists('config.json')
        del os.environ['CODALAB_HOME']

    def test_temp_codalab_manager(self):
        manager: CodaLabManager = CodaLabManager(temporary=True)
        self.assertEqual(manager.state, {'auth': {}, 'sessions': {}})
        manager.save_state()
        self.assertFalse(
            os.path.exists(manager.state_path),
            msg='Assert that the current state is not written out to state_path for a temporary CodaLabManager',
        )
