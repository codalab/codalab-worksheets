import tests.unit.azure_blob_mock  # noqa: F401

import os
import tempfile

from io import BytesIO

from unittest.mock import Mock

from codalab.bundles.dataset_bundle import DatasetBundle
from codalab.bundles.make_bundle import MakeBundle
from codalab.lib.spec_util import generate_uuid
from codalab.objects.dependency import Dependency
from codalab.worker.bundle_state import State
from tests.unit.server.bundle_manager import (
    BaseBundleManagerTest,
    BASE_METADATA_DATASET_BUNDLE,
    FILE_CONTENTS_1,
    FILE_CONTENTS_2,
)


class BundleManagerMakeBundlesTest(BaseBundleManagerTest):
    def test_no_bundles(self):
        """With no bundles available, nothing should happen."""
        self.bundle_manager._make_bundles()
        self.assertFalse(self.bundle_manager._is_making_bundles())

    def make_bundles_and_wait(self):
        """Helper function to run _make_bundles() and wait for the bundles to be
        fully made."""
        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

    def test_restage_stuck_bundle(self):
        """Bundles stuck in "MAKING" should be restaged and go back to the "MAKING" state."""
        bundle = self.create_make_bundle(state=State.MAKING)
        self.save_bundle(bundle)

        # Fixes a race case in which _make_bundle is called before this function can check the bundle state,
        # and instead sets the bundle state to "READY". We only want to test the restaging behavior in this test,
        # so we set _make_bundle to do nothing.
        self.bundle_manager._make_bundle = Mock()

        self.bundle_manager._make_bundles()

        self.assertTrue(self.bundle_manager._is_making_bundles())
        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.MAKING)

    def test_bundle_no_dependencies(self):
        """A MakeBundle with no dependencies should be made."""
        bundle = self.create_make_bundle(state=State.STAGED)
        self.save_bundle(bundle)
        self.make_bundles_and_wait()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.READY)

    def test_single_dependency(self):
        """A MakeBundle with a single dependency should be made."""
        bundle, parent = self.create_bundle_single_dep(
            bundle_type=MakeBundle, bundle_state=State.STAGED
        )

        self.make_bundles_and_wait()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)

        self.assertEqual(bundle.state, State.READY)

        self.assertEqual(self.read_bundle(bundle, "src"), FILE_CONTENTS_1)

    def test_multiple_dependencies(self):
        """A MakeBundle with two dependencies should be made."""
        bundle, parent1, parent2 = self.create_bundle_two_deps()

        self.make_bundles_and_wait()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)

        self.assertEqual(bundle.state, State.READY)

        self.assertEqual(self.read_bundle(bundle, "src1"), FILE_CONTENTS_1)
        self.assertEqual(self.read_bundle(bundle, "src2"), FILE_CONTENTS_2)

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

        self.make_bundles_and_wait()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)

        self.assertEqual(bundle.state, State.FAILED)
        self.assertIn("Invalid dependency", bundle.metadata.failure_message)

    def test_linked_dependency(self):
        """A MakeBundle with a linked dependency should be made."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(FILE_CONTENTS_1.encode())
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

        self.make_bundles_and_wait()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.READY)

        self.assertEqual(self.read_bundle(bundle, "src"), FILE_CONTENTS_1)
        os.remove(tempfile_name)

    def test_blob_storage_dependency(self):
        """A MakeBundle with a dependency stored on Blob Storage should be made."""
        parent = DatasetBundle.construct(
            metadata=BASE_METADATA_DATASET_BUNDLE, owner_id=self.user_id, uuid=generate_uuid(),
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

        self.upload_manager.upload_to_bundle_store(
            parent,
            ("contents", BytesIO(FILE_CONTENTS_1.encode())),
            git=False,
            unpack=True,
            use_azure_blob_beta=True,
        )

        self.make_bundles_and_wait()

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.READY)

        self.assertEqual(self.read_bundle(bundle, "src"), FILE_CONTENTS_1)
