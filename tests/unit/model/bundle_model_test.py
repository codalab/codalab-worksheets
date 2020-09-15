import mock
from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector
import unittest

from codalab.model.bundle_model import BundleModel, db_metadata


def metadata_to_dicts(uuid, metadata):
    return [
        {'bundle_uuid': uuid, 'metadata_key': key, 'metadata_value': value}
        for (key, value) in metadata.items()
    ]


def canonicalize(metadata_dicts):
    '''
  Convert a list of metadata dicts (which may be computed in-memory by
  calling metadata_to_dicts, or which may come from SQLAlchemy) into a
  canonical form for comparison.
  '''
    # Strip out any 'id' columns coming from the database.
    return sorted(
        sorted((k, v) for (k, v) in dict(metadata_dict).items() if k != 'id')
        for metadata_dict in metadata_dicts
    )


class MockDependency(object):
    _fields = {
        'child_uuid': 'my_uuid',
        'child_path': 'my_child_path',
        'parent_uuid': 'my_parent_uuid',
        'parent_path': 'my_parent_path',
    }

    def __init__(self, row=None):
        if row:
            for (field, value) in self._fields.items():
                self._tester.assertEqual(row[field], value)
        for (field, value) in self._fields.items():
            setattr(self, field, value)

    def to_dict(self):
        return dict(self._fields)


class MockBundle(object):
    _fields = {
        'uuid': 'my_uuid',
        'bundle_type': 'my_bundle_type',
        'data_hash': 'my_data_hash',
        'state': 'my_state',
        'metadata': {'key_1': 'value_1', 'key_2': 'value_2'},
        'dependencies': [MockDependency().to_dict()],
    }

    def __init__(self, row=None):
        if row:
            for (field, value) in self._fields.items():
                if field == 'metadata':
                    actual_value = canonicalize(row[field])
                    expected_value = canonicalize(
                        metadata_to_dicts(self._fields['uuid'], self._fields['metadata'])
                    )
                    self._tester.assertEqual(actual_value, expected_value)
                elif field == 'dependencies':
                    [MockDependency(dep) for dep in row[field]]
                else:
                    self._tester.assertEqual(row[field], value)
        for (field, value) in self._fields.items():
            setattr(self, field, value)
        self._validate_called = False

    def validate(self):
        self._validate_called = True

    def to_dict(self, strict=None):
        result = dict(self._fields)
        result['metadata'] = metadata_to_dicts(result['uuid'], result['metadata'])
        return result
