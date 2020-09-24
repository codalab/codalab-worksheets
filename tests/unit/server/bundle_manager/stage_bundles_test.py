from codalab.worker.bundle_state import Dependency, State
from codalab.objects.dependency import Dependency
from codalab.bundles.run_bundle import RunBundle
from codalab.lib.spec_util import generate_uuid
from tests.unit.server.bundle_manager import BASE_METADATA, BaseBundleManagerTest


class BundleManagerStageBundlesTest(BaseBundleManagerTest):
    def test_no_bundles(self):
        self.bundle_manager._stage_bundles()

    def test_single_bundle(self):
        bundle = self.create_run_bundle()
        self.save_bundle(bundle)

        self.bundle_manager._stage_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STAGED)

    def test_with_dependency(self):
        bundle, parent = self.create_bundle_single_dep()
        self.save_bundle(parent)
        self.save_bundle(bundle)

        self.bundle_manager._stage_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STAGED)

    def test_do_not_stage_with_failed_dependency(self):
        for state in (State.FAILED, State.KILLED):
            with self.subTest(state=state):
                bundle, parent = self.create_bundle_single_dep(parent_state=state, bundle_state=State.CREATED)
                self.save_bundle(bundle)
                self.save_bundle(parent)

                self.bundle_manager._stage_bundles()

                bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
                self.assertEqual(bundle.state, State.FAILED)
                self.assertIn(
                    "Please use the --allow-failed-dependencies flag",
                    bundle.metadata.failure_message,
                )

    def test_allow_failed_dependencies(self):
        for state in (State.FAILED, State.KILLED):
            with self.subTest(state=state):
                bundle, parent = self.create_bundle_single_dep()
                bundle.state = State.CREATED
                bundle.metadata.allow_failed_dependencies = True
                self.save_bundle(bundle)
                self.save_bundle(parent)

                self.bundle_manager._stage_bundles()

                bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
                self.assertEqual(bundle.state, State.STAGED)

    def test_missing_parent(self):
        bundle = self.create_run_bundle()
        bundle.dependencies = [
            Dependency(
                {
                    "parent_uuid": generate_uuid(),
                    "parent_path": "",
                    "child_uuid": bundle.uuid,
                    "child_path": "src",
                }
            )
        ]
        self.save_bundle(bundle)

        self.bundle_manager._stage_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.FAILED)
        self.assertIn("Missing parent bundles", bundle.metadata.failure_message)

    def test_no_permission_parents(self):
        bundle, parent = self.create_bundle_single_dep()
        parent.owner_id = generate_uuid()
        self.save_bundle(parent)
        self.save_bundle(bundle)

        self.bundle_manager._stage_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.FAILED)
        self.assertIn("does not have sufficient permissions", bundle.metadata.failure_message)
