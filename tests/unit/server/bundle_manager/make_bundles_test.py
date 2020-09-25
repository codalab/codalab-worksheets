from codalab.worker.bundle_state import State
from codalab.objects.dependency import Dependency
from codalab.bundles.run_bundle import RunBundle
from codalab.bundles.make_bundle import MakeBundle
from codalab.bundles.dataset_bundle import DatasetBundle
from codalab.lib.spec_util import generate_uuid
import os
import tempfile
from tests.unit.server.bundle_manager import (
    BaseBundleManagerTest,
    BASE_METADATA,
    BASE_METADATA_MAKE_BUNDLE,
    BASE_METADATA_DATASET_BUNDLE,
)


class BundleManagerMakeBundlesTest(BaseBundleManagerTest):
    def test_no_bundles(self):
        """With no bundles available, nothing should happen."""
        self.bundle_manager._make_bundles()
        self.assertFalse(self.bundle_manager._is_making_bundles())

    def test_restage_stuck_bundle(self):
        """Bundles stuck in "MAKING" should be restaged and go back to the "MAKING" state."""
        bundle = self.create_make_bundle(state=State.MAKING)
        self.save_bundle(bundle)
        self.bundle_manager._make_bundles()

        self.assertTrue(self.bundle_manager._is_making_bundles())
        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.MAKING)

    def test_bundle_no_dependencies(self):
        """A MakeBundle with no dependencies should be made."""
        bundle = self.create_make_bundle(state=State.STAGED)
        self.save_bundle(bundle)
        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.READY)

    def test_single_dependency(self):
        """A MakeBundle with a single dependency should be made."""
        bundle, parent = self.create_bundle_single_dep(
            bundle_type=MakeBundle, bundle_state=State.STAGED
        )
        self.save_bundle(parent)
        self.save_bundle(bundle)

        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)

        self.assertEqual(bundle.state, State.READY)

        with self.read_bundle(bundle, "src") as f:
            self.assertEqual(f.read(), "hello world")

    def test_multiple_dependencies(self):
        """A MakeBundle with two dependencies should be made."""
        bundle, parent1, parent2 = self.create_bundle_two_deps()
        self.save_bundle(bundle)
        self.save_bundle(parent1)
        self.save_bundle(parent2)

        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)

        self.assertEqual(bundle.state, State.READY)

        with self.read_bundle(bundle, "src1") as f:
            self.assertEqual(f.read(), "hello world 1")
        with self.read_bundle(bundle, "src2") as f:
            self.assertEqual(f.read(), "hello world 2")

    def test_fail_invalid_dependency_path(self):
        """A MakeBundle with an invalid dependency specified should fail."""
        bundle = self.create_make_bundle(state=State.STAGED)
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

        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)

        self.assertEqual(bundle.state, State.FAILED)
        self.assertIn("Invalid dependency", bundle.metadata.failure_message)

    def test_linked_dependency(self):
        """A MakeBundle with a linked dependency should be made."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"hello world")
            tempfile_name = f.name
        parent = DatasetBundle.construct(
            metadata=dict(BASE_METADATA_DATASET_BUNDLE, link_url=tempfile_name),
            owner_id=self.user_id,
            uuid=generate_uuid(),
        )
        bundle = self.create_make_bundle(state=State.STAGED)
        bundle.dependencies = [
            Dependency(
                {
                    "parent_uuid": parent.uuid,
                    "parent_path": "",
                    "child_uuid": bundle.uuid,
                    "child_path": "src",
                }
            )
        ]
        self.save_bundle(parent)
        self.save_bundle(bundle)

        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.READY)

        with self.read_bundle(bundle, "src") as f:
            self.assertEqual(f.read(), "hello world")
        os.remove(tempfile_name)
