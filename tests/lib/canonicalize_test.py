import mock
import os
import unittest

from codalab.common import (
  State,
  UsageError,
)
from codalab.lib import canonicalize
from codalab.objects.bundle import Bundle
 

class CanonicalizeTest(unittest.TestCase):
  def test_get_spec_uuid(self):
    tester = self

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
      def search_bundles(self, **kwargs):
        tester.assertEqual(set(kwargs), set(['name']))
        tester.assertIn(kwargs['name'], bundle_counts)
        count = bundle_counts[kwargs['name']]
        return [
          type('MockBundle', (object,), {'uuid': test_uuids[i]})
          for i in range(count)
        ]
    model = MockBundleModel()

    # Test that get_spec_uuid is idempotent on generated uuids.
    uuid = Bundle.generate_uuid()
    self.assertEqual(uuid, canonicalize.get_spec_uuid(model, uuid))
    # Test that get_spec_uuid returns the uuid of a uniquely named bundle.
    self.assertEqual(test_uuid, canonicalize.get_spec_uuid(model, unique_name))
    # Test that get_spec_uuid raises UsageErrors on missing or ambigious names.
    self.assertRaises(
      UsageError,
      lambda: canonicalize.get_spec_uuid(model, missing_name),
    )
    self.assertRaises(
      UsageError,
      lambda: canonicalize.get_spec_uuid(model, ambiguous_name),
    )
    # Test that get_spec_uuid raises UsageError on specs that can be neither a
    # name or a uuid.
    self.assertRaises(
      UsageError,
      lambda: canonicalize.get_spec_uuid(model, 'names have no exclamations!'),
    )

  def test_get_target_path(self):
    tester = self
    test_bundle_spec = 'test_bundle_spec'
    test_uuid = 'test_uuid'
    test_data_hash = 'test_data_hash'
    test_location = 'test_location'
    test_path = 'test_path'
    target = (test_bundle_spec, test_path)

    class MockBundleModel(object):
      def get_bundle(self, uuid):
        tester.assertEqual(uuid, test_uuid)
        return self._bundle
    test_model = MockBundleModel()

    class MockBundleStore(object):
      def get_location(self, data_hash):
        tester.assertEqual(data_hash, test_data_hash)
        return test_location
    bundle_store = MockBundleStore()

    def get_spec_uuid(model, bundle_spec):
      self.assertEqual(model, test_model)
      self.assertEqual(bundle_spec, test_bundle_spec)
      return test_uuid

    with mock.patch('codalab.lib.canonicalize.get_spec_uuid', get_spec_uuid):
      test_model._bundle = type('MockBundle', (object,), {
        'state': State.CREATED,
        'data_hash': test_data_hash,
      })
      self.assertRaises(UsageError, lambda: canonicalize.get_target_path(
        bundle_store,
        test_model,
        target,
      ))
      test_model._bundle.state = State.READY
      result = canonicalize.get_target_path(bundle_store, test_model, target)
      self.assertEqual(result, os.path.join(test_location, test_path))
