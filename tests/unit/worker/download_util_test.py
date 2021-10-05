import tests.unit.azure_blob_mock  # noqa: F401
from codalab.common import parse_linked_bundle_url
from codalab.worker.download_util import (
    get_target_info,
    BundleTarget,
    compute_target_info_blob_descendants_flat,
    PathException,
)
import unittest
import random
import tarfile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from io import BytesIO
import tempfile
from ratarmountcore import SQLiteIndexedTar
import shutil
import gzip


class AzureBlobTestBase:
    """A helper class that contains convenient methods for creating
    files and/or folders."""

    def create_txt_file(self, contents=b"hello world"):
        """Creates a txt file and returns its path."""
        bundle_uuid = str(random.random())
        bundle_path = f"azfs://storageclwsdev0/bundles/{bundle_uuid}/test.txt"
        with FileSystems.create(bundle_path, compression_type=CompressionTypes.UNCOMPRESSED) as f:
            f.write(contents)
        return bundle_uuid, bundle_path

    def create_file(self, contents=b"hello world"):
        """Creates a file on Blob (stored as a .gz with an index.sqlite index file) and returns its path."""
        bundle_uuid = str(random.random())
        bundle_path = f"azfs://storageclwsdev0/bundles/{bundle_uuid}/contents.gz"
        compressed_file = BytesIO(gzip.compress(contents))
        # TODO: Unify this code with code in BlobStorageUploader.write_fileobj().
        with FileSystems.create(bundle_path, compression_type=CompressionTypes.UNCOMPRESSED) as f:
            shutil.copyfileobj(compressed_file, f)
        compressed_file.seek(0)
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as tmp_index_file:
            SQLiteIndexedTar(
                fileObject=compressed_file,
                tarFileName="contents",  # If saving a single file as a .gz archive, this file can be accessed by the "/contents" entry in the index.
                writeIndex=True,
                clearIndexCache=True,
                indexFileName=tmp_index_file.name,
            )
            with FileSystems.create(
                parse_linked_bundle_url(bundle_path).index_path,
                compression_type=CompressionTypes.UNCOMPRESSED,
            ) as out_index_file, open(tmp_index_file.name, "rb") as tif:
                shutil.copyfileobj(tif, out_index_file)
        return bundle_uuid, bundle_path

    def create_directory(self):
        """Creates a directory (stored as a .tar.gz with an index.sqlite index file) and returns its path."""
        bundle_uuid = str(random.random())
        bundle_path = f"azfs://storageclwsdev0/bundles/{bundle_uuid}/contents.tar.gz"

        def writestr(tf, name, contents):
            tinfo = tarfile.TarInfo(name)
            tinfo.size = len(contents)
            tf.addfile(tinfo, BytesIO(contents.encode()))

        def writedir(tf, name):
            tinfo = tarfile.TarInfo(name)
            tinfo.type = tarfile.DIRTYPE
            tf.addfile(tinfo, BytesIO())

        # TODO: Unify this code with code in UploadManager.upload_to_bundle_store().
        with FileSystems.create(
            bundle_path, compression_type=CompressionTypes.UNCOMPRESSED
        ) as out, tempfile.NamedTemporaryFile(
            suffix=".tar.gz"
        ) as tmp_tar_file, tempfile.NamedTemporaryFile(
            suffix=".sqlite"
        ) as tmp_index_file:
            with tarfile.open(name=tmp_tar_file.name, mode="w:gz") as tf:
                # We need to create separate entries for each directory, as a regular
                # .tar.gz file would have.
                writestr(tf, "./README.md", "hello world")
                writedir(tf, "./src")
                writestr(tf, "./src/test.sh", "echo hi")
                writedir(tf, "./dist")
                writedir(tf, "./dist/a")
                writedir(tf, "./dist/a/b")
                writestr(tf, "./dist/a/b/test2.sh", "echo two")
            shutil.copyfileobj(tmp_tar_file, out)
            with open(tmp_tar_file.name, "rb") as ttf:
                SQLiteIndexedTar(
                    fileObject=ttf,
                    tarFileName="contents",
                    writeIndex=True,
                    clearIndexCache=True,
                    indexFileName=tmp_index_file.name,
                )
            with FileSystems.create(
                parse_linked_bundle_url(bundle_path).index_path,
                compression_type=CompressionTypes.UNCOMPRESSED,
            ) as out_index_file, open(tmp_index_file.name, "rb") as tif:
                shutil.copyfileobj(tif, out_index_file)

        return bundle_uuid, bundle_path


class AzureBlobGetTargetInfoTest(AzureBlobTestBase, unittest.TestCase):
    def test_single_txt_file(self):
        """Test getting target info of a single txt file on Azure Blob Storage. As this isn't supported
        (paths should be specified within existing .gz / .tar.gz files), this should throw an exception."""
        bundle_uuid, bundle_path = self.create_txt_file(b"a")
        with self.assertRaises(PathException):
            get_target_info(bundle_path, BundleTarget(bundle_uuid, None), 0)

    def test_single_file(self):
        """Test getting target info of a single file (compressed as .gz) on Azure Blob Storage."""
        bundle_uuid, bundle_path = self.create_file(b"a")
        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, None), 0)
        target_info.pop("resolved_target")
        self.assertEqual(
            target_info, {'name': bundle_uuid, 'type': 'file', 'size': 1, 'perm': 0o755}
        )

    def test_nested_directories(self):
        """Test getting target info of different files within a bundle that consists of nested directories, on Azure Blob Storage."""
        bundle_uuid, bundle_path = self.create_directory()

        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, None), 0)
        target_info.pop("resolved_target")
        self.assertEqual(
            target_info, {'name': bundle_uuid, 'type': 'directory', 'size': 249, 'perm': 0o755}
        )

        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, None), 1)
        target_info.pop("resolved_target")
        self.assertEqual(
            target_info,
            {
                'name': bundle_uuid,
                'type': 'directory',
                'size': 249,
                'perm': 0o755,
                'contents': [
                    {'name': 'README.md', 'type': 'file', 'size': 11, 'perm': 0o644},
                    {'name': 'dist', 'type': 'directory', 'size': 0, 'perm': 0o644},
                    {'name': 'src', 'type': 'directory', 'size': 0, 'perm': 0o644},
                ],
            },
        )

        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, "README.md"), 1)
        target_info.pop("resolved_target")
        self.assertEqual(
            target_info, {'name': 'README.md', 'type': 'file', 'size': 11, 'perm': 0o644}
        )

        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, "src/test.sh"), 1)
        target_info.pop("resolved_target")
        self.assertEqual(target_info, {'name': 'test.sh', 'type': 'file', 'size': 7, 'perm': 0o644})

        target_info = get_target_info(
            bundle_path, BundleTarget(bundle_uuid, "dist/a/b/test2.sh"), 1
        )
        target_info.pop("resolved_target")
        self.assertEqual(
            target_info, {'name': 'test2.sh', 'type': 'file', 'size': 8, 'perm': 0o644}
        )

        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, "src"), 1)
        target_info.pop("resolved_target")
        self.assertEqual(
            target_info,
            {
                'name': 'src',
                'type': 'directory',
                'size': 0,
                'perm': 0o644,
                'contents': [{'name': 'test.sh', 'type': 'file', 'size': 7, 'perm': 0o644}],
            },
        )

        # Return all depths
        target_info = get_target_info(bundle_path, BundleTarget(bundle_uuid, "dist/a"), 999)
        target_info.pop("resolved_target")

        self.assertEqual(
            target_info,
            {
                'name': 'a',
                'size': 0,
                'perm': 0o644,
                'type': 'directory',
                'contents': [
                    {
                        'name': 'b',
                        'size': 0,
                        'perm': 0o644,
                        'type': 'directory',
                        'contents': [
                            {'name': 'test2.sh', 'size': 8, 'perm': 0o644, 'type': 'file'}
                        ],
                    }
                ],
            },
        )

    def test_nested_directories_get_descendants_flat(self):
        """Test the compute_target_info_blob_descendants_flat function with nested directories."""
        bundle_uuid, bundle_path = self.create_directory()

        # Entire directory
        results = compute_target_info_blob_descendants_flat(bundle_path)
        self.assertEqual(
            list(results),
            [
                {'name': '', 'type': 'directory', 'size': 249, 'perm': 0o755, 'contents': None},
                {'name': 'README.md', 'size': 11, 'perm': 0o644, 'type': 'file', 'contents': None,},
                {'name': 'dist', 'size': 0, 'perm': 0o644, 'type': 'directory', 'contents': None,},
                {'name': 'dist/a', 'size': 0, 'perm': 0o644, 'type': 'directory', 'contents': None},
                {
                    'name': 'dist/a/b',
                    'size': 0,
                    'perm': 0o644,
                    'type': 'directory',
                    'contents': None,
                },
                {
                    'name': 'dist/a/b/test2.sh',
                    'size': 8,
                    'perm': 0o644,
                    'type': 'file',
                    'contents': None,
                },
                {'name': 'src', 'size': 0, 'perm': 0o644, 'type': 'directory', 'contents': None,},
                {'name': 'src/test.sh', 'size': 7, 'perm': 0o644, 'type': 'file', 'contents': None},
            ],
        )

        # Subdirectory
        results = compute_target_info_blob_descendants_flat(bundle_path + "/" + "dist")
        self.assertEqual(
            list(results),
            [
                {'name': '', 'type': 'directory', 'size': 0, 'perm': 0o644, 'contents': None},
                {'name': 'a', 'size': 0, 'perm': 0o644, 'type': 'directory', 'contents': None},
                {'name': 'a/b', 'size': 0, 'perm': 0o644, 'type': 'directory', 'contents': None},
                {
                    'name': 'a/b/test2.sh',
                    'size': 8,
                    'perm': 0o644,
                    'type': 'file',
                    'contents': None,
                },
            ],
        )
