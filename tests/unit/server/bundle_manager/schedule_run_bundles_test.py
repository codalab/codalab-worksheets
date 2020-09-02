from codalab.worker.bundle_state import State
from codalab.bundles.run_bundle import RunBundle
from codalab.lib.spec_util import generate_uuid
from freezegun import freeze_time
from tests.unit.server.bundle_manager import BASE_METADATA, BaseBundleManagerTest


class BundleManagerScheduleRunBundlesTest(BaseBundleManagerTest):
    def test_no_bundles(self):
        self.bundle_manager._schedule_run_bundles()

    def test_no_workers(self):
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id='id1',
            uuid=generate_uuid(),
            state=State.CREATED,
        )
        self.bundle_manager._model.save_bundle(bundle)

        self.bundle_manager._schedule_run_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.CREATED)

    def test_stage_single_bundle(self):
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=dict(
                BASE_METADATA, request_memory="0", request_time="", request_cpus=1, request_gpus=0
            ),
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.STAGED,
        )
        self.bundle_manager._model.save_bundle(bundle)

        self.mock_worker_checkin(cpus=1)
        self.bundle_manager._schedule_run_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STARTING)

    # TODO: switch to the newest version of freezegun with the patch in https://github.com/spulec/freezegun/pull/353,
    # so that we can use as_kwarg and thus maintain the order of parameters as (self, frozen_time).
    @freeze_time("2020-02-01", as_arg=True)
    def test_cleanup_dead_workers(frozen_time, self):
        # Workers should be removed after they don't check in for a long enough time period.
        self.mock_worker_checkin(cpus=1)

        self.assertEqual(len(self.bundle_manager._worker_model.get_workers()), 1)

        frozen_time.move_to("2020-02-12")
        self.bundle_manager._schedule_run_bundles()

        self.assertEqual(len(self.bundle_manager._worker_model.get_workers()), 0)

    def test_restage_stuck_starting_bundles(self):
        # No workers are currently running this bundle, so it should be restaged.
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.STARTING,
        )
        self.bundle_manager._model.save_bundle(bundle)

        self.bundle_manager._schedule_run_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STAGED)

    def test_bring_offline_stuck_running_bundles(self):
        # No workers exist to claim this bundle, so it should go to the WORKER_OFFLINE state.
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.RUNNING,
        )
        self.bundle_manager._model.save_bundle(bundle)

        self.bundle_manager._schedule_run_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.WORKER_OFFLINE)

    def test_finalizing_bundle_goes_offline_if_no_worker_claims(self):
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.FINALIZING,
        )
        self.bundle_manager._model.save_bundle(bundle)

        self.bundle_manager._schedule_run_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.WORKER_OFFLINE)

    def test_finalizing_bundle_gets_finished(self):
        bundle = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.STAGED,
        )
        self.bundle_manager._model.save_bundle(bundle)
        worker_id = self.mock_worker_checkin(cpus=1)

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
