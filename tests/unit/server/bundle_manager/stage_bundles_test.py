from codalab.worker.bundle_state import State
from codalab.objects.dependency import Dependency
from codalab.lib.spec_util import generate_uuid
from tests.unit.server.bundle_manager import BaseBundleManagerTest


class BundleManagerStageBundlesTest(BaseBundleManagerTest):
    def test_no_bundles(self):
        """With no bundles available, nothing should happen."""
        self.bundle_manager._stage_bundles()

    def test_single_bundle(self):
        """A single bundle should be staged."""
        bundle = self.create_run_bundle()
        self.save_bundle(bundle)

        self.bundle_manager._stage_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STAGED)

    def test_with_dependency(self):
        """A single bundle with a dependency should be staged."""
        bundle, parent = self.create_bundle_single_dep()
        self.save_bundle(parent)
        self.save_bundle(bundle)

        self.bundle_manager._stage_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.STAGED)

    def test_do_not_stage_with_failed_dependency(self):
        """A bundle with a failed dependency should not be staged."""
        for state in (State.FAILED, State.KILLED):
            with self.subTest(state=state):
                bundle, parent = self.create_bundle_single_dep(
                    parent_state=state, bundle_state=State.CREATED
                )
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
        """A bundle with a failed dependency, with the --allow-failed-dependencies
        flag, should be staged."""
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
        """A bundle with a dependency that refers to a nonexistent parent should not
        be staged."""
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
        """A bundle with parents that the user doesn't have permission to should
        not be staged."""
        bundle, parent = self.create_bundle_single_dep()
        parent.owner_id = generate_uuid()
        self.save_bundle(parent)
        self.save_bundle(bundle)

        self.bundle_manager._stage_bundles()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.FAILED)
        self.assertIn("does not have sufficient permissions", bundle.metadata.failure_message)
