from codalab.worker.bundle_state import State
from codalab.bundles.run_bundle import RunBundle
from codalab.lib.spec_util import generate_uuid
from freezegun import freeze_time
from tests.unit.server.bundle_manager import BASE_METADATA, BaseBundleManagerTest


class BundleManagerScheduleRunBundlesTest(BaseBundleManagerTest):
    def test_no_bundles(self):
        """With no bundles available, nothing should happen."""
        self.bundle_manager._schedule_run_bundles()

    def test_no_workers(self):
        """When no workers are available, no bundles should be scheduled."""
        bundle = self.create_run_bundle()
        self.save_bundle(bundle)

        self.bundle_manager._schedule_run_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.CREATED)

    def test_stage_single_bundle(self):
        """When a worker with the right specs is available, a bundle should be staged."""
        bundle = self.create_run_bundle(
            state=State.STAGED,
            metadata=dict(request_memory="0", request_time="", request_cpus=1, request_gpus=0),
        )
        self.save_bundle(bundle)

        self.mock_worker_checkin(cpus=1, user_id=self.user_id)
        self.bundle_manager._schedule_run_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STARTING)

    @freeze_time("2020-02-01", as_kwarg='frozen_time')
    def test_cleanup_dead_workers(self, frozen_time):
        """If workers don't check in for a long enough time period, they should be removed."""
        self.mock_worker_checkin(cpus=1, user_id=self.user_id)

        self.assertEqual(len(self.bundle_manager._worker_model.get_workers()), 1)

        frozen_time.move_to("2020-02-12")
        self.bundle_manager._schedule_run_bundles()

        self.assertEqual(len(self.bundle_manager._worker_model.get_workers()), 0)

    def test_restage_stuck_starting_bundles(self):
        """No workers are currently running a bundle, it should be restaged."""
        bundle = self.create_run_bundle(State.STARTING)
        self.save_bundle(bundle)
        self.bundle_manager._schedule_run_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STAGED)

    def test_bring_offline_stuck_running_bundles(self):
        """If no workers exist to claim a bundle, it should go to the WORKER_OFFLINE state."""
        bundle = self.create_run_bundle(State.RUNNING)
        self.save_bundle(bundle)
        self.bundle_manager._schedule_run_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.WORKER_OFFLINE)

    def test_finalizing_bundle_goes_offline_if_no_worker_claims(self):
        """If no worker claims a FINALIZING bundle, it should go to the WORKER_OFFLINE_STATE."""
        bundle = self.create_run_bundle(State.FINALIZING)
        self.save_bundle(bundle)
        self.bundle_manager._schedule_run_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.WORKER_OFFLINE)

    def test_finalizing_bundle_gets_finished(self):
        """If a worker checks in with a "finalizing" message, the bundle should transition
        to the FINALIZING and then FINISHED state."""
        bundle = self.create_run_bundle(State.STAGED)
        self.save_bundle(bundle)
        worker_id = self.mock_worker_checkin(cpus=1, user_id=self.user_id)

        # Bundle is assigned to worker
        self.bundle_manager._schedule_run_bundles()
        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STARTING)

        # Worker sends back a "finalizing" message
        bundle.state = State.FINALIZING
        self.mock_bundle_checkin(bundle, worker_id)

        # Bundle is finished
        self.bundle_manager._schedule_run_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.READY)
