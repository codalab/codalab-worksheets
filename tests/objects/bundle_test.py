import json
import unittest

from codalab.model.tables import bundle as cl_bundle
from codalab.objects.bundle import Bundle
from codalab.objects.metadata_spec import MetadataSpec


class MockBundle(Bundle):
  BUNDLE_TYPE = 'mock'
  METADATA_SPECS = (
    MetadataSpec('str_metadata', basestring, 'test str metadata'),
    MetadataSpec('int_metadata', int, 'test int metadata'),
    MetadataSpec('list_metadata', list, 'test list metadata'),
  )

  @classmethod
  def construct(cls, **kwargs):
    final_kwargs = dict(kwargs, bundle_type=MockBundle.BUNDLE_TYPE)
    return cls(final_kwargs)


class BundleTest(unittest.TestCase):
  COLUMNS = tuple(col.name for col in cl_bundle.c if col.name != 'id')

  str_metadata = 'my_str'
  int_metadata = 17
  list_metadata = ['value_1', 'value_2']

  bundle_type = MockBundle.BUNDLE_TYPE
  command = 'my_command'
  data_hash = 'my_data_hash'
  state = 'my_state'
  #worker_command = None

  def construct_mock_bundle(self):
    metadata = {
      'str_metadata': self.str_metadata,
      'int_metadata': self.int_metadata,
      'list_metadata': self.list_metadata,
    }
    return MockBundle.construct(
      command=self.command,
      data_hash=self.data_hash,
      state=self.state,
      metadata=metadata,
      dependencies=[],
      owner_id=0,
      is_anonymous=False,
      #worker_command=self.worker_command,
    )

  def check_bundle(self, bundle, uuid=None):
    for spec in MockBundle.METADATA_SPECS:
      expected_value = getattr(self, spec.key)
      if spec.type == list:
        expected_value = list(expected_value)
      self.assertEqual(getattr(bundle.metadata, spec.key), expected_value)
    for column in self.COLUMNS:
      if column == 'uuid':
        expected_value = uuid or getattr(bundle, column)
      else:
        expected_value = getattr(bundle, column)
      self.assertEqual(getattr(bundle, column), expected_value)

  def test_columns(self):
    '''
    Test that Bundle.COLUMNS includes precisely the non-id columns of cl_bundle,
    in the same order.
    '''
    self.assertEqual(Bundle.COLUMNS, self.COLUMNS)

  def test_init(self):
    '''
    Test that initializing a Bundle works and that its fields are correct.
    '''
    bundle = self.construct_mock_bundle()
    bundle.validate()
    self.check_bundle(bundle)

  def test_to_dict(self):
    '''
    Test that serializing and deserializing a bundle recovers the original.
    '''
    bundle = self.construct_mock_bundle()
    serialized_bundle = bundle.to_dict()
    # Serialize through JSON to check that the serialized bundle can be
    # transferred over the wire or depressed into a database.
    json_bundle = json.loads(json.dumps(serialized_bundle))
    deserialized_bundle = MockBundle(json_bundle)
    self.check_bundle(deserialized_bundle, uuid=bundle.uuid)
