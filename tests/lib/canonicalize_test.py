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
    user_id = None

    # Test that get_bundle_uuid is idempotent on generated uuids.
    uuid = spec_util.generate_uuid()
    self.assertEqual(uuid, canonicalize.get_bundle_uuid(model, user_id, worksheet_uuid, uuid))
    # Test that get_bundle_uuid returns the uuid of a uniquely named bundle.
    self.assertEqual(test_uuid, canonicalize.get_bundle_uuid(model, user_id, worksheet_uuid, unique_name))
    # Test that get_bundle_uuid raises UsageErrors on missing names.
    self.assertRaises(
      UsageError,
      lambda: canonicalize.get_bundle_uuid(model, user_id, worksheet_uuid, missing_name),
    )
    # Test that get_bundle_uuid raises UsageError on specs that can be neither a
    # name or a uuid.
    self.assertRaises(
      UsageError,
      lambda: canonicalize.get_bundle_uuid(model, user_id, worksheet_uuid, 'names have no exclamations!'),
    )

  def test_get_target_path(self):
    tester = self
    test_bundle_spec = 'test_bundle_spec'
    test_uuid = 'test_uuid'
    test_data_hash = 'test_data_hash'
    test_location = 'test_location'
    test_path = 'test_path'
    target = (test_uuid, test_path)

    class MockBundleModel(object):
      def get_bundle(self, uuid):
        tester.assertEqual(uuid, test_uuid)
        return self._bundle
    test_model = MockBundleModel()

    class MockBundleStore(object):
      def get_temp_location(self, identifier):
        return os.path.join('temp', identifier)
      def get_location(self, data_hash):
        tester.assertEqual(data_hash, test_data_hash)
        return test_location
    bundle_store = MockBundleStore()

    def get_bundle_uuid(model, bundle_spec):
      self.assertEqual(model, test_model)
      self.assertEqual(bundle_spec, test_bundle_spec)
      return test_uuid

    with mock.patch('codalab.lib.canonicalize.get_bundle_uuid', get_bundle_uuid):
      test_model._bundle = type('MockBundle', (object,), {
        'state': State.CREATED,
        'data_hash': None,
      })
      test_model._bundle.data_hash = test_data_hash
      result = canonicalize.get_target_path(bundle_store, test_model, target)
      self.assertEqual(result, os.path.join(test_location, test_path))
