import unittest

from codalab.worker.bundle_state import State, BundleInfo, Dependency


class BundleStateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.bundle_info_fields = {
            'uuid': '0xuuid',
            'bundle_type': 'bundle_type',
            'owner_id': 'owner_id',
            'command': 'command',
            'data_hash': 'data_hash',
            'state': State.RUNNING,
            'frozen': None,
            'is_anonymous': False,
            'metadata': {'test_field': 'test_value'},
            'args': 'args',
            'dependencies': [
                {
                    'child_uuid': 'child_uuid_0',
                    'child_path': 'child_path_0',
                    'parent_uuid': 'parent_uuid_0',
                    'parent_path': 'parent_path_0',
                    'parent_name': 'parent_name_0',
                },
                {
                    'child_uuid': 'child_uuid_1',
                    'child_path': 'child_path_1',
                    'parent_uuid': 'parent_uuid_1',
                    'parent_path': 'parent_path_1',
                    'parent_name': 'parent_name_1',
                },
            ],
            'location': 'location',
        }

    def tearDown(self) -> None:
        del self.bundle_info_fields

    def test_bundle_info_serialization(self):
        info = BundleInfo(
            uuid=self.bundle_info_fields['uuid'],
            bundle_type=self.bundle_info_fields['bundle_type'],
            owner_id=self.bundle_info_fields['owner_id'],
            command=self.bundle_info_fields['command'],
            data_hash=self.bundle_info_fields['data_hash'],
            state=self.bundle_info_fields['state'],
            frozen=self.bundle_info_fields['frozen'],
            is_anonymous=self.bundle_info_fields['is_anonymous'],
            metadata=self.bundle_info_fields['metadata'],
            args=self.bundle_info_fields['args'],
            dependencies=self.bundle_info_fields['dependencies'],
            location=self.bundle_info_fields['location'],
        )

        self.assertEqual(
            info.dependencies,
            [
                Dependency(
                    parent_name='parent_name_0',
                    parent_path='parent_path_0',
                    parent_uuid='parent_uuid_0',
                    child_path='child_path_0',
                    child_uuid='child_uuid_0',
                    location=None,
                ),
                Dependency(
                    parent_name='parent_name_1',
                    parent_path='parent_path_1',
                    parent_uuid='parent_uuid_1',
                    child_path='child_path_1',
                    child_uuid='child_uuid_1',
                    location=None,
                ),
            ],
        )
        self.assertEqual(
            info.as_dict['dependencies'],
            [
                {
                    'parent_name': 'parent_name_0',
                    'parent_path': 'parent_path_0',
                    'parent_uuid': 'parent_uuid_0',
                    'child_path': 'child_path_0',
                    'child_uuid': 'child_uuid_0',
                    'location': None,
                },
                {
                    'parent_name': 'parent_name_1',
                    'parent_path': 'parent_path_1',
                    'parent_uuid': 'parent_uuid_1',
                    'child_path': 'child_path_1',
                    'child_uuid': 'child_uuid_1',
                    'location': None,
                },
            ],
        )
        self.assertEqual(info.as_dict, BundleInfo.from_dict(info.as_dict).as_dict)
