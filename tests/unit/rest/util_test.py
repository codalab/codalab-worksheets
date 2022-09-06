import unittest
from unittest.mock import Mock

from codalab.model.tables import GROUP_OBJECT_PERMISSION_ALL, GROUP_OBJECT_PERMISSION_NONE
from codalab.rest import util


class UtilTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        util.request.user = Mock(user_id='test_user')

    def _mock_model_call(self, expected_uuids, to_return, uuid_arg_position=0):
        def side_effect(*args, **kwargs):
            actual_uuids = kwargs.get('uuid') if 'uuid' in kwargs else set(args[uuid_arg_position])
            if actual_uuids == expected_uuids:
                return to_return
            else:
                self.fail(
                    'BundleModel function called with the wrong arguments. Passed in %s but expected %s.'
                    % (actual_uuids, expected_uuids)
                )

        return side_effect

    def _mock_bundle(self, uuid):
        metadata_mock = Mock()
        metadata_mock.to_dict.side_effect = lambda: dict()
        return Mock(
            uuid=uuid,
            bundle_type='run',
            owner_id=None,
            command=None,
            data_hash=None,
            state='created',
            frozen=None,
            is_anonymous=False,
            metadata=metadata_mock,
            dependencies=[],
        )

    def test_get_bundle_infos_single_worksheet(self):
        bundle_uuids = {'0x123', '0x234', '0x345'}
        worksheet_uuids = {'0x111', '0x222', '0x333', '0x444'}
        model = Mock()
        model.batch_get_bundles.side_effect = self._mock_model_call(
            bundle_uuids, [self._mock_bundle(uuid) for uuid in bundle_uuids]
        )
        bundle_permissions = {
            '0x123': GROUP_OBJECT_PERMISSION_ALL,
            '0x234': GROUP_OBJECT_PERMISSION_ALL,
            '0x345': GROUP_OBJECT_PERMISSION_ALL,
        }
        model.get_user_bundle_permissions.side_effect = self._mock_model_call(
            bundle_uuids, bundle_permissions, 1
        )
        bundle_worksheet_uuids = {
            '0x123': ['0x111', '0x444'],
            '0x234': ['0x222'],
            '0x345': ['0x333'],
        }
        model.get_host_worksheet_uuids.side_effect = self._mock_model_call(
            bundle_uuids, bundle_worksheet_uuids
        )
        worksheet_permissions = {
            '0x111': GROUP_OBJECT_PERMISSION_ALL,
            '0x222': GROUP_OBJECT_PERMISSION_ALL,
            '0x333': GROUP_OBJECT_PERMISSION_NONE,
            '0x444': GROUP_OBJECT_PERMISSION_NONE,
        }
        model.get_user_worksheet_permissions.side_effect = self._mock_model_call(
            worksheet_uuids, worksheet_permissions, 1
        )
        worksheets = [Mock(uuid='0x111'), Mock(uuid='0x222')]
        worksheets[0].configure_mock(name='ws1')
        worksheets[1].configure_mock(name='ws2')
        model.batch_get_worksheets.side_effect = self._mock_model_call(
            {'0x111', '0x222'}, worksheets
        )
        infos = util.get_bundle_infos(
            bundle_uuids,
            get_children=False,
            get_single_host_worksheet=True,
            get_host_worksheets=False,
            get_permissions=False,
            ignore_not_found=True,
            model=model,
        )

        # Bundles 0x123 and 0x234 had a worksheet with correct permissions. 0x345 had a worksheet that can't be read.
        self.assertDictEqual(infos['0x123']['host_worksheet'], {'uuid': '0x111', 'name': 'ws1'})
        self.assertDictEqual(infos['0x234']['host_worksheet'], {'uuid': '0x222', 'name': 'ws2'})
        self.assertTrue('host_worksheet' not in infos['0x345'])
