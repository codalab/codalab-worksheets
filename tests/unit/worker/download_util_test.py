import tests.unit.azure_blob_mock  # noqa: F401
from codalab.worker.download_util import get_target_info, BundleTarget
import unittest
import random
from zipfile import ZipFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems


class AzureBlobGetTargetInfoTest(unittest.TestCase):
    def test_single_file(self):
        """Test getting target info of a single file on Azure Blob Storage."""
        bundle_uuid = str(random.random())
        bundle_path = f"azfs://storageclwsdev0/bundles/{bundle_uuid}/contents"
        with FileSystems.create(bundle_path, compression_type=CompressionTypes.UNCOMPRESSED) as f:
            f.write(b"a")
        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, None), 0)
        target_info.pop("resolved_target")
        self.assertEqual(target_info, {'name': bundle_uuid, 'type': 'file', 'size': 1, 'perm': 511})

    def test_nested_directories(self):
        """Test getting target info of different files within a bundle that consists of nested directories, on Azure Blob Storage."""
        bundle_uuid = str(random.random())
        bundle_path = f"azfs://storageclwsdev0/bundles/{bundle_uuid}/contents.zip"
        with FileSystems.create(bundle_path, compression_type=CompressionTypes.UNCOMPRESSED) as f:
            with ZipFile(f, "w") as zf:
                # We need to create separate entries for each directories, as a regular
                # zip file would have.
                zf.writestr("README.md", "hello world")
                zf.writestr("src/", "")
                zf.writestr("src/test.sh", "echo hi")
                zf.writestr("dist/", "")
                zf.writestr("dist/a/", "")
                zf.writestr("dist/a/b/", "")
                zf.writestr("dist/a/b/test2.sh", "echo two")

        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, None), 0)
        target_info.pop("resolved_target")
        self.assertEqual(
            target_info, {'name': bundle_uuid, 'type': 'directory', 'size': 704, 'perm': 511}
        )

        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, None), 1)
        target_info.pop("resolved_target")
        self.assertEqual(
            target_info,
            {
                'name': bundle_uuid,
                'type': 'directory',
                'size': 704,
                'perm': 511,
                'contents': [
                    {'name': 'README.md', 'type': 'file', 'size': 11, 'perm': 511},
                    {'name': 'src', 'type': 'directory', 'size': 0, 'perm': 511},
                    {'name': 'dist', 'type': 'directory', 'size': 0, 'perm': 511},
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
        self.assertEqual(target_info, {'name': 'test.sh', 'type': 'file', 'size': 7, 'perm': 511})

        target_info = get_target_info(
            bundle_path, BundleTarget(bundle_uuid, "dist/a/b/test2.sh"), 1
        )
        target_info.pop("resolved_target")
        self.assertEqual(target_info, {'name': 'test2.sh', 'type': 'file', 'size': 8, 'perm': 511})

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

        # Return all depths
        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, "dist/a"), 999)
        target_info.pop("resolved_target")
        print(target_info)
        self.assertEqual(
            target_info,
            {
                'name': 'a',
                'size': 0,
                'perm': 511,
                'type': 'directory',
                'contents': [
                    {
                        'name': 'b',
                        'size': 0,
                        'perm': 511,
                        'type': 'directory',
                        'contents': [{'name': 'test2.sh', 'size': 8, 'perm': 511, 'type': 'file'}],
                    }
                ],
            },
        )
