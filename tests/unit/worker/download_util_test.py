from codalab.worker.download_util import get_target_info, BundleTarget
import apache_beam.io.filesystems
import unittest
import random
import zipfile
from zipfile import ZipFile
from apache_beam.io.filesystems import FileSystems, BlobStorageFileSystem
from codalab.lib.beam.mockblobstoragefilesystem import MockBlobStorageFileSystem


# Monkey-patch so that we use MockBlobStorageFileSystem
# instead of BlobStorageFileSystem
class DummyClass:
    pass


BlobStorageFileSystem.__bases__ = (DummyClass,)
apache_beam.io.filesystems.BlobStorageFileSystem = MockBlobStorageFileSystem


class GetTargetInfoTest(unittest.TestCase):
    def test_single_file(self):
        bundle_uuid = str(random.random())
        bundle_path = f"azfs://storageclwsdev0/bundles/{bundle_uuid}/contents"
        with FileSystems.create(bundle_path) as f:
            f.write(b"a")
        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, None), 0)
        target_info.pop("resolved_target")
        self.assertEqual(target_info, {'name': bundle_uuid, 'type': 'file', 'size': 1, 'perm': 511})

    def test_nested_directories(self):
        bundle_uuid = str(random.random())
        bundle_path = f"azfs://storageclwsdev0/bundles/{bundle_uuid}/contents.zip"
        with FileSystems.create(bundle_path) as f:
            with ZipFile(f, "w") as zf:
                zf.writestr("README.md", "hello world")
                zf.writestr("src/test.sh", "echo hi")
                zf.writestr("dist/a/b/test2.sh", "echo two")
        with FileSystems.open(bundle_path) as f:
            with ZipFile(f, "r") as zf:
                raise Exception([x for x in zf.namelist()])

        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, None), 0)
        target_info.pop("resolved_target")
        self.assertEqual(
            target_info, {'name': bundle_uuid, 'type': 'directory', 'size': 26, 'perm': 511}
        )

        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, None), 1)
        target_info.pop("resolved_target")
        self.assertEqual(
            target_info,
            {
                'name': bundle_uuid,
                'type': 'directory',
                'size': 26,
                'perm': 511,
                'contents': [
                    {'name': 'README.md', 'type': 'file', 'size': 11, 'perm': 511},
                    {'name': 'test.sh', 'type': 'file', 'size': 7, 'perm': 511},
                    {'name': 'test2.sh', 'type': 'file', 'size': 8, 'perm': 511},
                ],
            },
        )

        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, "README.md"), 1)
        target_info.pop("resolved_target")
        self.assertEqual(
            target_info, {'name': 'README.md', 'type': 'file', 'size': 11, 'perm': 511}
        )

        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, "src/test.sh"), 1)
        target_info.pop("resolved_target")
        self.assertEqual(
            target_info, {'name': 'src/test.sh', 'type': 'file', 'size': 7, 'perm': 511}
        )

        target_info = get_target_info(
            bundle_path, BundleTarget(bundle_uuid, "dist/a/b/test2.sh"), 1
        )
        target_info.pop("resolved_target")
        self.assertEqual(
            target_info, {'name': 'dist/a/b/test2.sh', 'type': 'file', 'size': 8, 'perm': 511}
        )

        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, "src"), 1)
        target_info.pop("resolved_target")
        self.assertEqual(
            target_info,
            {
                'name': 'src',
                'type': 'directory',
                'size': 0,
                'perm': 511,
                'contents': [{'name': 'test.sh', 'type': 'file', 'size': 7, 'perm': 511}],
            },
        )

        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, "src/a"), 1)
        target_info.pop("resolved_target")
        print(target_info)
        self.assertEqual(
            target_info,
            {'name': 'src/a', 'type': 'directory', 'size': 0, 'perm': 511, 'contents': []},
        )
