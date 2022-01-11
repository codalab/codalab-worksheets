import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Dict

from codalab.lib.codalab_manager import CodaLabManager


class CodalabManagerTest(unittest.TestCase):

    codalab_home = None

    def setUp(self):
        if 'CODALAB_HOME' in os.environ:
            print(os.environ['CODALAB_HOME'])
            self.codalab_home = os.environ['CODALAB_HOME']
        os.environ['CODALAB_HOME'] = str(Path.home())

    def tearDown(self):
        def remove_file_if_exists(file):
            file_path = os.path.join(str(Path.home()), file)
            if os.path.exists(file_path):
                os.remove(file_path)

        # Remove the state and config json files created by the CodaLabManagers
        remove_file_if_exists('state.json')
        remove_file_if_exists('config.json')

        if self.codalab_home:
            os.environ['CODALAB_HOME'] = self.codalab_home
        else:
            del os.environ['CODALAB_HOME']

    def test_temp_codalab_manager(self):
        manager: CodaLabManager = CodaLabManager(temporary=True)
        self.assertEqual(manager.state, {'auth': {}, 'sessions': {}})
        manager.save_state()
        self.assertFalse(
            os.path.exists(manager.state_path),
            msg='Assert that the current state is not written out to state_path for a temporary CodaLabManager',
        )

    def test_temp_codalab_manager_initialize_state(self):
        initial_state: Dict = {
            'auth': {"https://worksheets.codalab.org": {"token_info": {"access_token": "secret"}}},
            'sessions': {},
        }

        cache_file = tempfile.NamedTemporaryFile(delete=False)
        with open(cache_file.name, "w") as f:
            json.dump(initial_state, f)
        os.environ["CODALAB_STATE"] = cache_file.name

        manager: CodaLabManager = CodaLabManager(temporary=True)

        self.assertEqual(manager.state, initial_state)
        os.remove(cache_file.name)
