import mock
import os
from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector
import unittest

from codalab.model.bundle_model import BundleModel, db_metadata
from codalab.lib.spec_util import generate_uuid


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

    def __init__(self, uuid=None, row=None):
        self._fields = {
            'uuid': uuid,
            'bundle_type': 'my_bundle_type',
            'data_hash': 'my_data_hash',
            'state': 'my_state',
            'metadata': [
                {'bundle_uuid': uuid, 'metadata_key': 'key', 'metadata_value': 'value'},
                {'bundle_uuid': uuid, 'metadata_key': 'key', 'metadata_value': '你好世界😊'}
            ],
            'dependencies': [MockDependency().to_dict()],
        }
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
        # result['metadata'] = metadata_to_dicts(result['uuid'], result['metadata'])
        return result


class BundleModelTestBase:
    maxDiff = None
    def setUp(self):
        MockBundle._tester = self
        MockDependency._tester = self
        self.engine = create_engine(self.engine_conn_string, **self.engine_conn_kwargs)
        self.model = BundleModel(self.engine, {})
        # We'll test the result of this schema creation step in test_create_tables.
        self.model.create_tables()

    def tearDown(self):
        self.model = None
        self.engine = None

    def test_create_tables(self):
        inspector = Inspector.from_engine(self.engine)
        tables = set(inspector.get_table_names())
        for table in db_metadata.tables:
            self.assertIn(table, tables)

    def test_save_and_get_bundle(self):
        bundle = MockBundle(generate_uuid())
        self.model.save_bundle(bundle)
        self.assertTrue(bundle._validate_called)

        get_bundle_subclass_path = 'codalab.model.bundle_model.get_bundle_subclass'
        with mock.patch(get_bundle_subclass_path, lambda bundle_type: MockBundle):
            retrieved_bundle = self.model.get_bundle(bundle.uuid)
        self.assertTrue(isinstance(retrieved_bundle, MockBundle))
        bundle_metadata = bundle.to_dict()['metadata']
        retrieved_bundle_metadata = retrieved_bundle.to_dict()['metadata']
        self.assertEqual([(item["metadata_key"], item["metadata_value"]) for item in retrieved_bundle_metadata], [(item["metadata_key"], item["metadata_value"]) for item in bundle_metadata])

class BundleModelSqlLiteTest(BundleModelTestBase, unittest.TestCase):
    engine_conn_string = 'sqlite://'
    engine_conn_kwargs = dict(strategy='threadlocal', encoding='utf-8')

class BundleModelMySqlTest(BundleModelTestBase, unittest.TestCase):
    print(os.environ)
    engine_conn_string = 'mysql://%s:%s@mysql:3306/codalab_bundles?charset=utf8mb4' % (
        # os.getenv('CODALAB_MYSQL_USER'),
        # os.getenv('CODALAB_MYSQL_PWD'),
        'codalab',
        'codalab'
    )
    engine_conn_kwargs = dict(strategy='threadlocal',
            pool_size=20,
            max_overflow=100,
            pool_recycle=3600,
            encoding='utf-8',)