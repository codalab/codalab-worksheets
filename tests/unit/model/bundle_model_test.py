import unittest
from tests.unit.server.bundle_manager import TestBase
from codalab.worker.bundle_state import State
from codalab.model.bundle_model import is_academic_email


class BundleModelTest(TestBase, unittest.TestCase):
    def test_ready_bundle_should_not_transition_worker_offline(self):
        """transition_bundle_worker_offline should not transition a READY bundle to worker_offline."""
        bundle = self.create_run_bundle(State.READY)
        self.save_bundle(bundle)
        result = self.bundle_manager._model.transition_bundle_worker_offline(bundle)
        self.assertEqual(result, False)
        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.READY)

    def test_finalizing_bundle_should_not_transition_worker_offline(self):
        """transition_bundle_worker_offline should transition a FINALIZING bundle to worker_offline."""
        bundle = self.create_run_bundle(State.FINALIZING)
        self.save_bundle(bundle)
        result = self.bundle_manager._model.transition_bundle_worker_offline(bundle)
        self.assertEqual(result, True)
        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.WORKER_OFFLINE)

    def test_is_academic_email(self):
        """Unit test to check is_academic_email function."""
        test_cases = {
            "abc@stanford.edu": True,
            "abc@xyz.edu.cn": True,
            "abc@xyz.edu.sg": True,
            "abc.edu.cn@xyz.mail": False,
            "abc@xyz.edu.xyz": False,
        }
        for key, value in test_cases.items():
            self.assertEqual(is_academic_email(key), value)


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
