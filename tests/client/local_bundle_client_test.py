'''
Local bundle client tests.
'''
import os
import unittest

from codalab.common import UsageError
from codalab.client.local_bundle_client import LocalBundleClient
from codalab.lib import path_util, spec_util
from codalab.lib.bundle_store import MultiDiskBundleStore
from codalab.model.sqlite_model import SQLiteModel
from codalab.server.auth import MockAuthHandler, User

class GroupsAndPermsTest(unittest.TestCase):
    '''
    Tests for groups and permissions
    '''

    @classmethod
    def setUpClass(cls):
        cls.test_root = path_util.normalize("~/.codalab_tests")
        path_util.make_directory(cls.test_root)
        cls.bundle_store = MultiDiskBundleStore(cls.test_root)
        cls.model = SQLiteModel("sqlite:///{}".format(os.path.join(cls.test_root, 'bundle.db')), {})
        cls.model.root_user_id = '0'
        users = [User('root', '0'), User('user1', '1'), User('user2', '2'), User('user4', '4')]
        cls.auth_handler = MockAuthHandler(users)
        cls.client = LocalBundleClient('local', cls.bundle_store, cls.model, None, None, cls.auth_handler, verbose=1)

    @classmethod
    def tearDownClass(cls):
        cls.model.engine.close()
        path_util.remove(cls.test_root)

    def set_current_user(self, username, password):
        token_info = self.client.login('credentials', username, password)
        self.auth_handler.validate_token(token_info['access_token'])

    def test_new_group(self):
        defaultCount = 1  # Include public group
        self.set_current_user('root', '')
        # Verify assumption that there are no groups for user 'root'.
        groups = self.client.list_groups()
        self.assertEqual(defaultCount + 0, len(groups))
        # Create group 'grp1'
        g1 = self.client.new_group('grp1')
        self.assertIsNotNone(g1)
        self.assertIn('name', g1)
        self.assertEqual('grp1', g1['name'])
        self.assertIn('uuid', g1)
        # Verify output of list command.
        groups = self.client.list_groups()
        self.assertEqual(defaultCount + 1, len(groups))
        g1prime = [g for g in groups if g['name'] != 'public'][0]
        self.assertIn('name', g1prime)
        self.assertEqual(g1['name'], g1prime['name'])
        self.assertIn('uuid', g1prime)
        self.assertEqual(g1['uuid'], g1prime['uuid'])
        self.assertIn('role', g1prime)
        self.assertEqual('owner', g1prime['role'])
        # Delete group 'grp1'
        g1deleted = self.client.rm_group('grp1')
        self.assertIn('name', g1deleted)
        self.assertEqual(g1['name'], g1deleted['name'])
        self.assertIn('uuid', g1deleted)
        self.assertEqual(g1['uuid'], g1deleted['uuid'])
        groups = self.client.list_groups()
        self.assertEqual(defaultCount + 0, len(groups))

    def test_new_group_bad_name(self):
        self.set_current_user('root', '')
        with self.assertRaises(UsageError):
            self.client.new_group('9grp1')

    def test_delete_nonexistent_group(self):
        self.set_current_user('root', '')
        uuid = spec_util.generate_uuid()
        with self.assertRaises(UsageError):
            self.client.rm_group(uuid)

    def test_delete_group(self):
        self.set_current_user('root', '')
        grp_name = 'g_delete_group'
        g1 = self.client.new_group(grp_name)
        # Can't create two groups of the same name
        with self.assertRaises(UsageError):
            self.client.new_group(grp_name)
        # Delete by UUID
        g1deleted = self.client.rm_group(g1['uuid'])
        self.assertEqual(g1['uuid'], g1deleted['uuid'])
        # Create it now
        g2 = self.client.new_group(grp_name)
        # Delete by name
        g2deleted = self.client.rm_group(grp_name)
        self.assertEqual(g2['uuid'], g2deleted['uuid'])
        # Can't delete by name when no group has that name
        with self.assertRaises(UsageError):
            self.client.rm_group(grp_name)

    def test_membership(self):
        def _assert_group_count_for(username, num_group):
            self.set_current_user(username, '')
            groups = self.client.list_groups()
            num_group += 1  # For public group, who everyone is part of
            if num_group != len(groups):
                print 'GROUPS:', groups
            self.assertEqual(num_group, len(groups))
        _assert_group_count_for('root', 0)
        _assert_group_count_for('user1', 0)
        _assert_group_count_for('user2', 0)
        # root creates a group and adds user1 as an admin
        self.set_current_user('root', '')
        grp_name = 'g_membership'
        g1 = self.client.new_group(grp_name)
        _assert_group_count_for('root', 1)
        _assert_group_count_for('user1', 0)
        _assert_group_count_for('user2', 0)
        self.set_current_user('root', '')
        self.client.add_user('user1', grp_name, True)
        _assert_group_count_for('root', 1)
        _assert_group_count_for('user1', 1)
        _assert_group_count_for('user2', 0)
        # user1 adds user2 as a regular member
        self.set_current_user('user1', '')
        self.client.add_user('user2', g1['uuid'], False)
        _assert_group_count_for('root', 1)
        _assert_group_count_for('user1', 1)
        _assert_group_count_for('user2', 1)
        # show group info (as user2)
        self.set_current_user('user2', '')
        g1_info = self.client.group_info(grp_name)
        self.assertEqual(g1['uuid'], g1_info['uuid'])
        self.assertIn('members', g1_info)
        self.assertEqual(3, len(g1_info['members']))
        members = {member['user_name']: member['role'] for member in g1_info['members']}
        self.assertIn('root', members)
        self.assertEqual('owner', members['root'])
        self.assertIn('user1', members)
        self.assertEqual('admin', members['user1'])
        self.assertIn('user2', members)
        self.assertEqual('member', members['user2'])
        # user2 (not admin) is not allowed to add user4 to the group
        self.set_current_user('user2', '')
        with self.assertRaises(UsageError):
            self.client.add_user('user4', g1['uuid'], False)
        # user2 can't delete the group
        self.set_current_user('user2', '')
        with self.assertRaises(UsageError):
            self.client.rm_group(grp_name)
        # root removes user2
        self.set_current_user('root', '')
        self.client.rm_user('user2', grp_name)
        _assert_group_count_for('root', 1)
        _assert_group_count_for('user1', 1)
        _assert_group_count_for('user2', 0)
        # root deletes the group
        self.set_current_user('root', '')
        self.client.rm_group(grp_name)
        _assert_group_count_for('root', 0)
        _assert_group_count_for('user1', 0)
        _assert_group_count_for('user2', 0)
