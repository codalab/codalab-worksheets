import os
import unittest

from codalab.lib.codalab_manager import CodaLabManager


class CodalabManagerTest(unittest.TestCase):
    def setUp(self):
        os.environ['CODALAB_HOME'] = '.'

    def tearDown(self):
        try:
            # Remove the state and config json files created by the CodaLabManagers
            os.remove('state.json')
            os.remove('config.json')
        except:
            pass

        del os.environ['CODALAB_HOME']

    def test_get_state_for_temp_codalab_manager(self):
        manager: CodaLabManager = CodaLabManager(temporary=True)
        self.assertEqual(manager.get_state(), {'auth': {}, 'sessions': {}})
        manager.save_state()
        self.assertFalse(
            os.path.exists(manager.state_path),
            msg='Assert that the current state is not written out to state_path for a temporary CodaLabManager',
        )

    def test_state_persistence(self):
        manager1: CodaLabManager = CodaLabManager(temporary=False)
        manager1.state['auth'] = {'https://worksheets.codalab.org': 'old_token'}
        manager1.save_state()
        os.environ['CODALAB_SESSION'] = 'session1'
        manager1.set_current_worksheet_uuid('https://worksheets.codalab.org', '0x1')
        self.assertEqual(
            manager1.get_state(),
            {
                'auth': {'https://worksheets.codalab.org': 'old_token'},
                'sessions': {
                    'session1': {
                        'address': 'https://worksheets.codalab.org',
                        'worksheet_uuid': '0x1',
                    }
                },
            },
        )

        manager2: CodaLabManager = CodaLabManager(temporary=False)
        manager2.state['auth'] = {'https://worksheets.codalab.org': 'new_token'}
        manager2.save_state()
        os.environ['CODALAB_SESSION'] = 'session2'
        manager2.set_current_worksheet_uuid('https://worksheets.codalab.org', '0x2')

        # Assert that the new token is respected and both sessions are kept
        self.assertEqual(
            manager2.get_state(),
            {
                'auth': {'https://worksheets.codalab.org': 'new_token'},
                'sessions': {
                    'session1': {
                        'address': 'https://worksheets.codalab.org',
                        'worksheet_uuid': '0x1',
                    },
                    'session2': {
                        'address': 'https://worksheets.codalab.org',
                        'worksheet_uuid': '0x2',
                    },
                },
            },
        )
        self.assertEqual(
            manager1.get_state(),
            manager2.get_state(),
            msg='Assert that both CodaLabManagers have the same state',
        )

        manager1.logout('https://worksheets.codalab.org')
        # Assert that the token is cleared after logging out, but sessions are kept
        self.assertEqual(
            manager2.get_state(),
            {
                'auth': {},
                'sessions': {
                    'session1': {
                        'address': 'https://worksheets.codalab.org',
                        'worksheet_uuid': '0x1',
                    },
                    'session2': {
                        'address': 'https://worksheets.codalab.org',
                        'worksheet_uuid': '0x2',
                    },
                },
            },
        )
        self.assertEqual(
            manager1.get_state(),
            manager2.get_state(),
            msg='Assert that both CodaLabManagers still have the same state',
        )
