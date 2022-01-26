import os
import shutil
import tempfile

from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from typing import Any, Union, Tuple, IO, cast
from ratarmountcore import SQLiteIndexedTar

from codalab.common import UsageError, StorageType, urlopen_with_retry, parse_linked_bundle_url
from codalab.worker.file_util import tar_gzip_directory, GzipStream
from codalab.lib import file_util, path_util, zip_util
from codalab.objects.bundle import Bundle
from codalab.lib.zip_util import ARCHIVE_EXTS_DIR

Source = Union[str, Tuple[str, IO[bytes]]]


class Uploader:
    """Uploader base class. Subclasses should extend this class and implement the
    non-implemented methods that perform the uploads to a bundle store."""

    def __init__(self, bundle_model, bundle_store, destination_bundle_store=None):
        self._bundle_model = bundle_model
        self._bundle_store = bundle_store
        self.destination_bundle_store = destination_bundle_store

    @property
    def storage_type(self):
        """Returns storage type. Must be one of the values of the StorageType enum."""
        raise NotImplementedError

    def write_git_repo(self, source: str, bundle_path: str):
        """Clones the git repository indicated by source and uploads it to the path at bundle_path.
        Args:
            source (str): source to git repository.
            bundle_path (str): Output bundle path.
        """
        raise NotImplementedError

    def write_fileobj(
        self, source_ext: str, source_fileobj: IO[bytes], bundle_path: str, unpack_archive: bool
    ):
        """Writes fileobj indicated, unpacks if specified, and uploads it to the path at bundle_path.
        Args:
            source_ext (str): File extension of the source to write.
            source_fileobj (str): Fileobj of the source to write.
            bundle_path (str): Output bundle path.
            unpack_archive (bool): Whether fileobj is an archive that should be unpacked.
        """
        raise NotImplementedError

    def upload_to_bundle_store(self, bundle: Bundle, source: Source, git: bool, unpack: bool):
        """Uploads the given source to the bundle store.
        Given arguments are the same as UploadManager.upload_to_bundle_store()"""
        try:
            # bundle_path = self._bundle_store.get_bundle_location(bundle.uuid)
            is_url, is_fileobj, filename = self._interpret_source(source)
            if is_url:
                assert isinstance(source, str)
                if git:
                    bundle_path = self._update_and_get_bundle_location(bundle, is_directory=True)
                    self.write_git_repo(source, bundle_path)
                else:
                    # If downloading from a URL, convert the source to a file object.
                    is_fileobj = True
                    source = (filename, urlopen_with_retry(source))
            if is_fileobj:
                source_filename, source_fileobj = cast(Tuple[str, IO[bytes]], source)
                source_ext = zip_util.get_archive_ext(source_filename)
                if unpack and zip_util.path_is_archive(filename):
                    bundle_path = self._update_and_get_bundle_location(
                        bundle, is_directory=source_ext in ARCHIVE_EXTS_DIR
                    )
                    self.write_fileobj(source_ext, source_fileobj, bundle_path, unpack_archive=True)
                else:
                    bundle_path = self._update_and_get_bundle_location(bundle, is_directory=False)
                    self.write_fileobj(
                        source_ext, source_fileobj, bundle_path, unpack_archive=False
                    )

        except UsageError:
            if FileSystems.exists(bundle_path):
                path_util.remove(bundle_path)
            raise

    def _update_and_get_bundle_location(self, bundle: Bundle, is_directory: bool) -> str:
        """Updates the information of the given bundle based on the current storage type
        and is_directory of the upload, then returns the location where the bundle is stored.

        The value of is_directory may affect the bundle location; for example, in Blob Storage,
        directories are stored in .tar.gz files and non-directories are stored as .gz files.

        Args:
            bundle (Bundle): Bundle to update.
            is_directory (bool): Should be set to True if the bundle is a directory and False if it is a single file.
        Returns:
            str: Bundle location.
        """
        if self.destination_bundle_store is not None:
            # In this case, we are using the new BundleStore / BundleLocation model to track the bundle location.
            # Create the appropriate bundle location.
            self._bundle_model.add_bundle_location(
                bundle.uuid, self.destination_bundle_store["uuid"]
            )
            self._bundle_model.update_bundle(
                bundle, {'is_dir': is_directory},
            )
            return self._bundle_store.get_bundle_location(
                bundle.uuid, bundle_store_uuid=self.destination_bundle_store["uuid"]
            )
        else:
            # Else, continue to set the legacy "storage_type" column.
            self._bundle_model.update_bundle(
                bundle, {'storage_type': self.storage_type, 'is_dir': is_directory,},
            )
            return self._bundle_store.get_bundle_location(bundle.uuid)

    def _interpret_source(self, source: Source):
        """Interprets the given source.
        Args:
            source (Source): Source to interpret.
        Returns:
            (is_url, is_fileobj, filename)
        """
        is_url, is_fileobj = False, False
        if isinstance(source, str):
            if path_util.path_is_url(source):
                is_url = True
                source = source.rsplit('?', 1)[0]  # Remove query string from URL, if present
            else:
                raise UsageError("Path must be a URL.")
            filename = os.path.basename(os.path.normpath(source))
        else:
            is_fileobj = True
            filename = source[0]
        return is_url, is_fileobj, filename


class DiskStorageUploader(Uploader):
    """Uploader that uploads to uncompressed files / folders on disk."""

    @property
    def storage_type(self):
        return StorageType.DISK_STORAGE.value

    def write_git_repo(self, source: str, bundle_path: str):
        file_util.git_clone(source, bundle_path)

    def write_fileobj(
        self, source_ext: str, source_fileobj: IO[bytes], bundle_path: str, unpack_archive: bool
    ):
        if unpack_archive:
            zip_util.unpack(source_ext, source_fileobj, bundle_path)
        else:
            with open(bundle_path, 'wb') as out:
                shutil.copyfileobj(cast(IO, source_fileobj), out)


class BlobStorageUploader(Uploader):
    """Uploader that uploads to archive files + index files on Blob Storage."""

    @property
    def storage_type(self):
        return StorageType.AZURE_BLOB_STORAGE.value

    def write_git_repo(self, source: str, bundle_path: str):
        with tempfile.TemporaryDirectory() as tmpdir:
            file_util.git_clone(source, tmpdir)
            # Upload a fileobj with the repo's tarred and gzipped contents.
            self.write_fileobj(
                ".tar.gz", tar_gzip_directory(tmpdir), bundle_path, unpack_archive=True
            )

    def write_fileobj(
        self, source_ext: str, source_fileobj: IO[bytes], bundle_path: str, unpack_archive: bool
    ):
        if unpack_archive:
            output_fileobj = zip_util.unpack_to_archive(source_ext, source_fileobj)
        else:
            output_fileobj = GzipStream(source_fileobj)
        # Write archive file.
        with FileSystems.create(bundle_path, compression_type=CompressionTypes.UNCOMPRESSED) as out:
            shutil.copyfileobj(output_fileobj, out)
        # Write index file to a temporary file, then write that file to Blob Storage.
        with FileSystems.open(
            bundle_path, compression_type=CompressionTypes.UNCOMPRESSED
        ) as ttf, tempfile.NamedTemporaryFile(suffix=".sqlite") as tmp_index_file:
            SQLiteIndexedTar(
                fileObject=ttf,
                tarFileName="contents",  # If saving a single file as a .gz archive, this file can be accessed by the "/contents" entry in the index.
                writeIndex=True,
                clearIndexCache=True,
                indexFilePath=tmp_index_file.name,
            )
            with FileSystems.create(
                parse_linked_bundle_url(bundle_path).index_path,
                compression_type=CompressionTypes.UNCOMPRESSED,
            ) as out_index_file, open(tmp_index_file.name, "rb") as tif:
                shutil.copyfileobj(tif, out_index_file)


class UploadManager(object):
    """
    Contains logic for uploading bundle data to the bundle store and updating
    the associated bundle metadata in the database.
    """

    def __init__(self, bundle_model, bundle_store):
        self._bundle_model = bundle_model
        self._bundle_store = bundle_store

    def upload_to_bundle_store(
        self,
        bundle: Bundle,
        source: Source,
        git: bool,
        unpack: bool,
        use_azure_blob_beta: bool,
        destination_bundle_store=None,
    ):
        """
        Uploads contents for the given bundle to the bundle store.

        |bundle|: specifies the bundle associated with the contents to upload.
        |source|: specifies the location of the contents to upload. Each element is
                   either a URL or a tuple (filename, binary file-like object).
        |git|: for URLs, whether |source| is a git repo to clone.
        |unpack|: whether to unpack |source| if it's an archive.
        |use_azure_blob_beta|: whether to use Azure Blob Storage.
        |destination_bundle_store|: BundleStore to upload to. If specified, uploads to the given BundleStore.

        Exceptions:
        - If |git|, then the bundle contains the result of running 'git clone |source|'
        - If |unpack| is True or a source is an archive (zip, tar.gz, etc.), then unpack the source.
        """
        UploaderCls: Any = DiskStorageUploader
        if destination_bundle_store:
            # Set the uploader class based on which bundle store is specified.
            if destination_bundle_store["storage_type"] in (
                StorageType.AZURE_BLOB_STORAGE.value,
                StorageType.GCS_STORAGE.value,
            ):
                UploaderCls = BlobStorageUploader
        elif use_azure_blob_beta:
            # Legacy "-a" flag without specifying a bundle store.
            UploaderCls = BlobStorageUploader
        return UploaderCls(
            self._bundle_model, self._bundle_store, destination_bundle_store
        ).upload_to_bundle_store(bundle, source, git, unpack)

    def has_contents(self, bundle):
        # TODO: make this non-fs-specific.
        return os.path.exists(self._bundle_store.get_bundle_location(bundle.uuid))

    def cleanup_existing_contents(self, bundle):
        self._bundle_store.cleanup(bundle.uuid, dry_run=False)
        bundle_update = {'data_hash': None, 'metadata': {'data_size': 0}}
        self._bundle_model.update_bundle(bundle, bundle_update)
        self._bundle_model.update_user_disk_used(bundle.owner_id)
