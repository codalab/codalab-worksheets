import unittest

from codalab.worker.bundle_state import State, BundleInfo


class BundleStateTest(unittest.TestCase):
    def test_bundle_info_serialization(self):
        bundle_info_fields = {
            'uuid': '0xuuid',
            'bundle_type': 'bundle_type',
            'owner_id': 'owner_id',
            'command': 'command',
            'data_hash': 'data_hash',
            'state': State.RUNNING,
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
        info = BundleInfo(
            uuid=bundle_info_fields['uuid'],
            bundle_type=bundle_info_fields['bundle_type'],
            owner_id=bundle_info_fields['owner_id'],
            command=bundle_info_fields['command'],
            data_hash=bundle_info_fields['data_hash'],
            state=bundle_info_fields['state'],
            is_anonymous=bundle_info_fields['is_anonymous'],
            metadata=bundle_info_fields['metadata'],
            args=bundle_info_fields['args'],
            dependencies=bundle_info_fields['dependencies'],
            location=bundle_info_fields['location'],
        )
        self.assertEqual(info.to_dict(), BundleInfo.from_dict(info.to_dict()).to_dict())
