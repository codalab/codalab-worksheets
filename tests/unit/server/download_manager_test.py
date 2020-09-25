from codalab.worker.bundle_state import State
from codalab.bundles.run_bundle import RunBundle
from codalab.lib.spec_util import generate_uuid
from tests.unit.server.bundle_manager import BaseBundleManagerTest
from codalab.worker.download_util import BundleTarget
from codalab.common import NotFoundError

class GetTargetInfoTest(BaseBundleManagerTest):
    def test_not_found(self):
        """Running get_target_info for a nonexistent bundle should raise an error."""
        with self.assertRaises(NotFoundError):
            target = BundleTarget(generate_uuid(), "")
            self.download_manager.get_target_info(target, 0)

    def test_bundle_single_file(self):
        """Running get_target_info for a bundle with a single file."""
        bundle = self.create_run_bundle()
        self.save_bundle(bundle)
        with self.write_bundle(bundle) as f:
          f.write("hello world")
        target = BundleTarget(bundle.uuid, "")
        info = self.download_manager.get_target_info(target, 0)
        self.assertEqual(info["name"], bundle.uuid)
        self.assertEqual(info["size"], 11)
        self.assertEqual(info["perm"], 420)
        self.assertEqual(info["type"], "file")
        self.assertEqual(str(info["resolved_target"]), f"{bundle.uuid}:")

    def test_bundle_folder(self):
        """Running get_target_info for a bundle with a folder."""
        bundle = self.create_run_bundle()
        self.save_bundle(bundle)
        with self.write_bundle(bundle, "item.txt") as f:
          f.write("hello world")
        target = BundleTarget(bundle.uuid, "")
        info = self.download_manager.get_target_info(target, 0)
        self.assertEqual(info["name"], bundle.uuid)
        self.assertEqual(info["size"], 96)
        self.assertEqual(info["perm"], 493)
        self.assertEqual(info["type"], "directory")
        self.assertEqual(str(info["resolved_target"]), f"{bundle.uuid}:")

    def test_bundle_folder_subpath(self):
        """Running get_target_info for a bundle with a folder, with a valid subpath."""
        bundle = self.create_run_bundle()
        self.save_bundle(bundle)
        with self.write_bundle(bundle, "src/item.txt") as f:
          f.write("hello world")
        target = BundleTarget(bundle.uuid, "src")
        info = self.download_manager.get_target_info(target, 0)
        self.assertEqual(info["name"], "src")
        self.assertEqual(info["type"], "directory")
        self.assertEqual(str(info["resolved_target"]), f"{bundle.uuid}:src")

        target = BundleTarget(bundle.uuid, "src/item.txt")
        info = self.download_manager.get_target_info(target, 0)
        self.assertEqual(info["name"], "item.txt")
        self.assertEqual(info["type"], "file")
        self.assertEqual(str(info["resolved_target"]), f"{bundle.uuid}:src/item.txt")
