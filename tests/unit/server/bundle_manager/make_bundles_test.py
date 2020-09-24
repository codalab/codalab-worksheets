from codalab.worker.bundle_state import Dependency, State
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
        self.bundle_manager._make_bundles()
        self.assertFalse(self.bundle_manager._is_making_bundles())

    def test_restage_stuck_bundle(self):
        bundle = self.create_make_bundle(state=State.MAKING)

        self.bundle_manager._make_bundles()

        self.assertTrue(self.bundle_manager._is_making_bundles())
        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.MAKING)

    def test_bundle_no_dependencies(self):
        bundle = self.create_make_bundle(state=State.STAGED)

        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.READY)

    def test_single_dependency(self):
        bundle, _ = self.create_bundle_single_dep()

        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)

        self.assertEqual(bundle.state, State.READY)

        with open(
            os.path.join(
                self.codalab_manager.bundle_store().get_bundle_location(bundle.uuid), "src"
            ),
            "r",
        ) as f:
            self.assertEqual(f.read(), "hello world")

    def test_multiple_dependencies(self):
        bundle, _, __ = self.create_bundle_two_deps()

        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)

        self.assertEqual(bundle.state, State.READY)

        with open(
            os.path.join(
                self.codalab_manager.bundle_store().get_bundle_location(bundle.uuid), "src1"
            ),
            "r",
        ) as f:
            self.assertEqual(f.read(), "hello world 1")
        with open(
            os.path.join(
                self.codalab_manager.bundle_store().get_bundle_location(bundle.uuid), "src2"
            ),
            "r",
        ) as f:
            self.assertEqual(f.read(), "hello world 2")

    def test_fail_invalid_dependency_path(self):
        parent = RunBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.READY,
        )
        bundle = MakeBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA_MAKE_BUNDLE,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.STAGED,
        )
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
        self.bundle_manager._model.save_bundle(parent)
        self.bundle_manager._model.save_bundle(bundle)

        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)

        self.assertEqual(bundle.state, State.FAILED)
        self.assertIn("Invalid dependency", bundle.metadata.failure_message)

    def test_linked_dependency(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"hello world")
            tempfile_name = f.name
        parent = DatasetBundle.construct(
            metadata=dict(BASE_METADATA_DATASET_BUNDLE, link_url=tempfile_name),
            owner_id=self.user_id,
            uuid=generate_uuid(),
        )
        bundle = MakeBundle.construct(
            targets=[],
            command='',
            metadata=BASE_METADATA_MAKE_BUNDLE,
            owner_id=self.user_id,
            uuid=generate_uuid(),
            state=State.STAGED,
        )
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
        self.bundle_manager._model.save_bundle(parent)
        self.bundle_manager._model.save_bundle(bundle)

        threads = self.bundle_manager._make_bundles()
        self.assertTrue(self.bundle_manager._is_making_bundles())
        for t in threads:
            t.join()
        self.assertFalse(self.bundle_manager._is_making_bundles())

        bundle = self.bundle_manager._model.get_bundle(bundle.uuid)
        self.assertEqual(bundle.state, State.READY)

        with open(
            os.path.join(
                self.codalab_manager.bundle_store().get_bundle_location(bundle.uuid), "src"
            ),
            "rb",
        ) as f:
            self.assertEqual(f.read(), b"hello world")
        os.remove(tempfile_name)
