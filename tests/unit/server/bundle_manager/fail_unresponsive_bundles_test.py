from codalab.worker.bundle_state import State
from freezegun import freeze_time
from tests.unit.server.bundle_manager import BaseBundleManagerTest


class BundleManagerFailUnresponsiveBundlesTest(BaseBundleManagerTest):
    def test_no_bundles(self):
        """With no bundles available, nothing should happen."""
        self.bundle_manager._fail_unresponsive_bundles()

    @freeze_time("2012-01-14", as_kwarg='frozen_time')
    def test_fail_bundle(self, frozen_time):
        """Bundles stuck in uploading state for too long should be failed."""
        bundle = self.create_run_bundle(State.UPLOADING)
        self.save_bundle(bundle)

        frozen_time.move_to("2020-02-12")
        self.bundle_manager._fail_unresponsive_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.FAILED)
        self.assertIn(
            "Bundle has been stuck in uploading state for more than 60 days",
            bundle.metadata.failure_message,
        )
