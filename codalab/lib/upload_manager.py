import os
import shutil
import tempfile

from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from typing import Any, Dict, Union, Tuple, IO, cast
from ratarmountcore import SQLiteIndexedTar
from contextlib import closing
from codalab.worker.upload_util import upload_with_chunked_encoding

from codalab.common import (
    StorageURLScheme,
    UsageError,
    StorageType,
    urlopen_with_retry,
    parse_linked_bundle_url,
    httpopen_with_retry,
)
from codalab.worker.file_util import tar_gzip_directory, GzipStream
from codalab.worker.bundle_state import State
from codalab.lib import file_util, path_util, zip_util
from codalab.objects.bundle import Bundle
from codalab.lib.zip_util import ARCHIVE_EXTS_DIR
from codalab.lib.print_util import FileTransferProgress

Source = Union[str, Tuple[str, IO[bytes]]]


class Uploader:
    """Uploader base class. Subclasses should extend this class and implement the
    non-implemented methods that perform the uploads to a bundle store.
    Used when: 1. client -> blob storage (is_client = True in init function)
               2. rest-server -> blob storage (is_client = False in init function)
    """

    def __init__(
        self, bundle_model=None, bundle_store=None, destination_bundle_store=None, is_client=False
    ):
        """
        params:
        bundle_model: Used on rest-server.
        bundle_store: Bundle store model, used on rest-server.
        destination_bundle_store: Indicate destination for bundle storage.
        is_client: Whether this uploader is used on client side. Used on client.
        """
        if not is_client:
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
        self,
        source_ext: str,
        source_fileobj: IO[bytes],
        bundle_path: str,
        unpack_archive: bool,
        bundle_conn_str=None,
        index_conn_str=None,
        progress_callback=None,
    ):
        """Writes fileobj indicated, unpacks if specified, and uploads it to the path at bundle_path.
        Args:
            source_ext (str): File extension of the source to write.
            source_fileobj (str): Fileobj of the source to write.
            bundle_path (str): Output bundle path.
            unpack_archive (bool): Whether fileobj is an archive that should be unpacked.
            bundle_conn_str (str): Connection string for uploading bundle contents.
            index_conn_str (str): Connection string for uploading bundle index file.
            progress_callback (func): Callback function of upload progress.
        """
        raise NotImplementedError

    def upload_to_bundle_store(self, bundle: Bundle, source: Source, git: bool, unpack: bool):
        """Uploads the given source to the bundle store.
        Given arguments are the same as UploadManager.upload_to_bundle_store().
        Used when uploading from rest server."""
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
        self,
        source_ext: str,
        source_fileobj: IO[bytes],
        bundle_path: str,
        unpack_archive: bool,
        bundle_conn_str=None,
        index_conn_str=None,
        progress_callback=None,
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
        self,
        source_ext: str,
        source_fileobj: IO[bytes],
        bundle_path: str,
        unpack_archive: bool,
        bundle_conn_str=None,
        index_conn_str=None,
        progress_callback=None,
    ):
        if unpack_archive:
            output_fileobj = zip_util.unpack_to_archive(source_ext, source_fileobj)
        else:
            output_fileobj = GzipStream(source_fileobj)

        # Write archive file.
        if bundle_conn_str is not None:
            conn_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING', '')
            os.environ['AZURE_STORAGE_CONNECTION_STRING'] = bundle_conn_str
        try:
            bytes_uploaded = 0
            CHUNK_SIZE = 16 * 1024
            with FileSystems.create(
                bundle_path, compression_type=CompressionTypes.UNCOMPRESSED
            ) as out:
                while True:
                    to_send = output_fileobj.read(CHUNK_SIZE)
                    if not to_send:
                        break
                    out.write(to_send)
                    bytes_uploaded += len(to_send)
                    if progress_callback is not None:
                        should_resume = progress_callback(bytes_uploaded)
                        if not should_resume:
                            raise Exception('Upload aborted by client')

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
                if bundle_conn_str is not None:
                    os.environ['AZURE_STORAGE_CONNECTION_STRING'] = index_conn_str
                with FileSystems.create(
                    parse_linked_bundle_url(bundle_path).index_path,
                    compression_type=CompressionTypes.UNCOMPRESSED,
                ) as out_index_file, open(tmp_index_file.name, "rb") as tif:
                    while True:
                        to_send = tif.read(CHUNK_SIZE)
                        if not to_send:
                            break
                        out_index_file.write(to_send)
                        bytes_uploaded += len(to_send)
                        if progress_callback is not None:
                            should_resume = progress_callback(bytes_uploaded)
                            if not should_resume:
                                raise Exception('Upload aborted by client')
        except Exception as err:
            raise err
        finally:  # restore the origin connection string
            if bundle_conn_str is not None:
                os.environ['AZURE_STORAGE_CONNECTION_STRING'] = conn_str if conn_str != '' else None  # type: ignore


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
            bundle_model=self._bundle_model,
            bundle_store=self._bundle_store,
            destination_bundle_store=destination_bundle_store,
            is_client=False,
        ).upload_to_bundle_store(bundle, source, git, unpack)

    def has_contents(self, bundle):
        # TODO: make this non-fs-specific.
        return os.path.exists(self._bundle_store.get_bundle_location(bundle.uuid))

    def cleanup_existing_contents(self, bundle):
        self._bundle_store.cleanup(bundle.uuid, dry_run=False)
        bundle_update = {'data_hash': None, 'metadata': {'data_size': 0}}
        self._bundle_model.update_bundle(bundle, bundle_update)
        self._bundle_model.update_user_disk_used(bundle.owner_id)

    def get_bundle_sas_token(self, path, **kwargs):
        """
        Get SAS token with write permission. Used for bypass server uploading.
        """
        return (
            parse_linked_bundle_url(path)
            .bundle_path_bypass_url(permission='rw', **kwargs)
            .split('?')[-1]  # Get SAS token from SAS url.
        )

    def get_index_sas_token(self, path, **kwargs):
        """
        Get SAS token of the index file with read and write permission. Used for uploading.
        """
        return (
            parse_linked_bundle_url(path)
            .index_path_bypass_url(permission='rw', **kwargs)
            .split('?')[-1]  # Get SAS token from SAS url.
        )

    def get_bundle_signed_url(self, path, **kwargs):
        """
        Get signed url for the bundle path
        """
        return parse_linked_bundle_url(path).bundle_path_bypass_url(**kwargs)

    def get_bundle_index_url(self, path, **kwargs):
        return parse_linked_bundle_url(path).index_path_bypass_url(**kwargs)


class ClientUploadManager(object):
    """
    Upload Manager for CLI client. Handle file upload for CLI client.
    """

    def __init__(self, json_api_client, stdout, stderr):
        self._client = json_api_client
        self.stdout = stdout
        self.stderr = stderr
        self.upload_func = {
            StorageURLScheme.GCS_STORAGE.value: self.upload_GCS_blob_storage,
            StorageURLScheme.AZURE_BLOB_STORAGE.value: self.upload_Azure_blob_storage,
        }

    def get_upload_func(self, bundle_url: str):
        """
        Return different upload fuction for different storage type
        """
        for k, v in self.upload_func.items():
            if bundle_url.startswith(k):
                return v
        return self.upload_Azure_blob_storage

    def upload_to_bundle_store(
        self,
        bundle: Dict,
        packed_source: Dict,
        use_azure_blob_beta: bool,
        destination_bundle_store=None,
    ):
        """
        Bypass server upload. Upload from client directly to different blob storage (Azure, GCS, Disk storage).
        Bypass server uploading is used in following situations:
        # 1. The server set CODALAB_DEFAULT_BUNDLE_STORE_NAME
        # 2. If the user specify `--store` and blob storage is on Azure
        """

        need_bypass = True
        bundle_store_uuid = None
        # 1) Read destination store from --store if user has specified it
        if destination_bundle_store is not None and destination_bundle_store != '':
            storage_info = self._client.fetch_one(
                'bundle_stores',
                params={
                    'name': destination_bundle_store,
                    'include': ['uuid', 'storage_type', 'url'],
                },
            )
            bundle_store_uuid = storage_info['uuid']
            if storage_info['storage_type'] in (StorageType.DISK_STORAGE.value,):
                need_bypass = False  # The user specify --store to upload to disk storage

        # 2) Pack the files to be uploaded
        source_ext = zip_util.get_archive_ext(packed_source['filename'])
        if packed_source['should_unpack'] and zip_util.path_is_archive(packed_source['filename']):
            unpack_before_upload = True
            is_dir = source_ext in zip_util.ARCHIVE_EXTS_DIR
        else:
            unpack_before_upload = False
            is_dir = False

        # 3) Create a bundle location for the bundle
        params = {'need_bypass': need_bypass, 'is_dir': is_dir}
        data = self._client.add_bundle_location(bundle['id'], bundle_store_uuid, params)[0].get(
            'attributes'
        )

        # 4) If bundle location has bundle_conn_str, we should bypass the server when uploading.
        if data.get('bundle_conn_str', None) is not None:
            # Mimic the rest server behavior
            # decided the bundle type (file/directory) and decide whether need to unpack
            bundle_conn_str = data.get('bundle_conn_str')
            index_conn_str = data.get('index_conn_str')
            bundle_url = data.get('bundle_url')
            bundle_read_str = data.get('bundle_read_url', bundle_url)
            try:
                progress = FileTransferProgress('Sent ', f=self.stderr)
                upload_func = self.get_upload_func(bundle_url)
                with closing(packed_source['fileobj']), progress:
                    upload_func(
                        fileobj=packed_source['fileobj'],
                        bundle_url=bundle_url,
                        bundle_conn_str=bundle_conn_str,
                        bundle_read_str=bundle_read_str,
                        index_conn_str=index_conn_str,
                        source_ext=source_ext,
                        should_unpack=unpack_before_upload,
                        progress_callback=progress.update,
                    )
            except Exception as err:
                self._client.update_bundle_state(
                    bundle['id'],
                    params={'success': False, 'error_msg': f'Bypass server upload error. {err}',},
                )
                raise err
            else:
                self._client.update_bundle_state(bundle['id'], params={'success': True})
        else:
            # 5) Otherwise, upload the bundle directly through the server.
            progress = FileTransferProgress('Sent ', packed_source['filesize'], f=self.stderr)
            with closing(packed_source['fileobj']), progress:
                self._client.upload_contents_blob(
                    bundle['id'],
                    fileobj=packed_source['fileobj'],
                    params={
                        'filename': packed_source['filename'],
                        'unpack': packed_source['should_unpack'],
                        'state_on_success': State.READY,
                        'finalize_on_success': True,
                        'use_azure_blob_beta': use_azure_blob_beta,
                        'store': destination_bundle_store or '',
                    },
                    progress_callback=progress.update,
                )

    def upload_Azure_blob_storage(
        self,
        fileobj,
        bundle_url,
        bundle_conn_str,
        bundle_read_str,
        index_conn_str,
        source_ext,
        should_unpack,
        progress_callback=None,
    ):
        """
        Helper function for bypass server upload. Mimic behavior of BlobStorageUploader at client side.
        Change enviroment variable 'AZURE_STORAGE_CONNECTION_STRING' to upload to Azure.

        params:
        fileobj: The file object to upload.
        bundle_url: Url for bundle store, eg "azfs://devstoreaccount1/bundles/{bundle_uuid}/contents.gz".
        bundle_conn_str: Connection string for the contents file.
        bundle_read_str: Signed URL or bundle URL to read the content of bundle.
        index_conn_str: Connection string for the index.sqlite file.
        source_ext: Extension of the file.
        should_unpack: Unpack the file before upload iff True.
        """
        BlobStorageUploader(
            bundle_model=None, bundle_store=None, destination_bundle_store=None, is_client=True
        ).write_fileobj(
            source_ext,
            fileobj,
            bundle_url,
            should_unpack,
            bundle_conn_str,
            index_conn_str,
            progress_callback,
        )

    def upload_GCS_blob_storage(
        self,
        fileobj,
        bundle_url,
        bundle_conn_str,
        bundle_read_str,
        index_conn_str,
        source_ext,
        should_unpack,
        progress_callback=None,
    ):
        from codalab.lib import zip_util

        if should_unpack:
            output_fileobj = zip_util.unpack_to_archive(source_ext, fileobj)
        else:
            output_fileobj = GzipStream(fileobj)

        # Write archive file.
        upload_with_chunked_encoding(
            method='PUT',
            base_url=bundle_conn_str,
            headers={'Content-type': 'application/octet-stream'},
            fileobj=output_fileobj,
            query_params={},
            progress_callback=progress_callback,
        )
        # upload the index file
        with httpopen_with_retry(bundle_read_str) as ttf, tempfile.NamedTemporaryFile(
            suffix=".sqlite"
        ) as tmp_index_file:
            SQLiteIndexedTar(
                fileObject=ttf,
                tarFileName="contents",
                writeIndex=True,
                clearIndexCache=True,
                indexFilePath=tmp_index_file.name,
            )
            upload_with_chunked_encoding(
                method='PUT',
                base_url=index_conn_str,
                headers={'Content-type': 'application/octet-stream'},
                query_params={},
                fileobj=open(tmp_index_file.name, "rb"),
                progress_callback=None,
            )
