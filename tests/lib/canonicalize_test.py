import mock
import os
import unittest

from codalab.common import (
  State,
  UsageError,
)
from codalab.lib import (
  canonicalize,
  spec_util,
)


class CanonicalizeTest(unittest.TestCase):
  def test_get_bundle_uuid(self):
    tester = self

    worksheet_uuid = '0x12345'
    missing_name = 'missing_name'
    unique_name = 'unique_name'
    ambiguous_name = 'ambiguous_name'
    bundle_counts = {
      missing_name: 0,
      unique_name: 1,
      ambiguous_name: 2,
    }
    test_uuids = ['test_uuid_0', 'test_uuid_1', 'test_uuid_2']
    test_uuid = test_uuids[0]

    class MockBundleModel(object):
      def get_bundle_uuids(self, conditions, max_results, count=False):
        tester.assertEqual(set(conditions), set(['name', 'user_id', 'worksheet_uuid']))
        name = conditions['name'].replace('%', '')
        tester.assertIn(name, bundle_counts)
        count = bundle_counts[name]
        return [
          test_uuids[i]
          for i in range(count)
        ]
    model = MockBundleModel()
    user = None

    # Test that get_bundle_uuid is idempotent on generated uuids.
    uuid = spec_util.generate_uuid()
    self.assertEqual(uuid, canonicalize.get_bundle_uuid(model, user, worksheet_uuid, uuid))
    # Test that get_bundle_uuid returns the uuid of a uniquely named bundle.
    self.assertEqual(test_uuid, canonicalize.get_bundle_uuid(model, user, worksheet_uuid, unique_name))
    # Test that get_bundle_uuid raises UsageErrors on missing names.
    self.assertRaises(
      UsageError,
      lambda: canonicalize.get_bundle_uuid(model, user, worksheet_uuid, missing_name),
    )
    # Test that get_bundle_uuid raises UsageError on specs that can be neither a
    # name or a uuid.
    self.assertRaises(
      UsageError,
      lambda: canonicalize.get_bundle_uuid(model, user, worksheet_uuid, 'names have no exclamations!'),
    )
