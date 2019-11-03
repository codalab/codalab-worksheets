import unittest
from mock import Mock

from codalab.lib import bundle_util


class BundleUtilTest(unittest.TestCase):
    def test_get_ancestors_string_representation(self):
        def side_effect(*args):
            if args[1] == '0x12345':
                return {
                    'uuid': '0x12345',
                    'metadata': {'name': 'bundle1'},
                    'dependencies': [{'parent_uuid': '0x23456'}],
                }
            elif args[1] == '0x23456':
                return {
                    'uuid': '0x23456',
                    'metadata': {'name': 'bundle2'},
                    'dependencies': [{'parent_uuid': '0x34567'}],
                }
            elif args[1] == '0x34567':
                return {'uuid': '0x34567', 'metadata': {'name': 'bundle3'}, 'dependencies': []}

        mock_client = Mock()
        mock_client.fetch.side_effect = side_effect
        actual = bundle_util.get_ancestors_string_representation(mock_client, '0x12345')
        self.assertEqual(actual, '- bundle1(0x12345)\n  - bundle2(0x23456)\n    - bundle3(0x34567)')
