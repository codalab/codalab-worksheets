import unittest


class RunManagerBaseTestMixin:
    """
    Provides some common test cases for run managers
    """
    def create_run_manager(self):
        raise NotImplementedError

    def create_run(self):
        # TODO Mock these out
        bundle = {
            'uuid': 'fake_test_bundle'
        }
        bundle_path = '/tmp/foo/ldsjfjlsdfj/bar'
        resources = {}

        manager = self.create_run_manager()
        return manager, manager.create_run(bundle, bundle_path, resources)

    def test_create(self):
        self.create_run()

    def test_serde(self):
        manager, run = self.create_run()
        data = manager.serialize(run)
        serde_run = manager.deserialize(data)
        self.assertIsInstance(serde_run, run.__class__, "serde run should be the same class as original")

