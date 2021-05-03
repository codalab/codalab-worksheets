import os
import shutil
from typing import Union, Tuple, IO, cast

from codalab.common import UsageError, StorageType, urlopen_with_retry
from codalab.lib import file_util, path_util
from codalab.objects.bundle import Bundle

Source = Union[str, Tuple[str, IO[bytes]]]


class UploadManager(object):
    """
    Contains logic for uploading bundle data to the bundle store and updating
    the associated bundle metadata in the database.
    """

    def __init__(self, bundle_model, bundle_store):
        from codalab.lib import zip_util

        # exclude these patterns by default
        self._bundle_model = bundle_model
        self._bundle_store = bundle_store
        self.zip_util = zip_util

    def upload_to_bundle_store(
        self, bundle: Bundle, source: Source, git: bool, unpack: bool, use_azure_blob_beta: bool,
    ):
        """
        Uploads contents for the given bundle to the bundle store.

        |bundle|: specifies the bundle associated with the contents to upload.
        |source|: specifies the location of the contents to upload. Each element is
                   either a URL or a tuple (filename, binary file-like object).
        |git|: for URLs, whether |source| is a git repo to clone.
        |unpack|: whether to unpack |source| if it's an archive.
        |use_azure_blob_beta|: whether to use Azure Blob Storage.

        Exceptions:
        - If |git|, then the bundle contains the result of running 'git clone |source|'
        - If |unpack| is True or a source is an archive (zip, tar.gz, etc.), then unpack the source.
        """
        bundle_path = self._bundle_store.get_bundle_location(bundle.uuid)
        try:
            is_url, is_fileobj, filename = self._interpret_source(source)
            if is_url:
                assert isinstance(source, str)
                if git:
                    file_util.git_clone(source, bundle_path)
                else:
                    # If downloading from a URL, convert the source to a file object.
                    is_fileobj = True
                    source = (filename, urlopen_with_retry(source))
            if is_fileobj:
                if unpack and self.zip_util.path_is_archive(filename):
                    self._unpack_fileobj(source[0], source[1], bundle_path)
                else:
                    with open(bundle_path, 'wb') as out:
                        shutil.copyfileobj(cast(IO, source[1]), out)

            # is_directory is True if the bundle is a directory and False if it is a single file.
            is_directory = os.path.isdir(bundle_path)
            self._bundle_model.update_bundle(
                bundle, {'storage_type': StorageType.DISK_STORAGE.value, 'is_dir': is_directory},
            )
        except UsageError:
            if os.path.exists(bundle_path):
                path_util.remove(bundle_path)
            raise

    def _interpret_source(self, source: Source):
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

    def _unpack_fileobj(self, source_filename, source_fileobj, dest_path):
        self.zip_util.unpack(
            self.zip_util.get_archive_ext(source_filename), source_fileobj, dest_path
        )

    def has_contents(self, bundle):
        # TODO: make this non-fs-specific.
        return os.path.exists(self._bundle_store.get_bundle_location(bundle.uuid))

    def cleanup_existing_contents(self, bundle):
        self._bundle_store.cleanup(bundle.uuid, dry_run=False)
        bundle_update = {'data_hash': None, 'metadata': {'data_size': 0}}
        self._bundle_model.update_bundle(bundle, bundle_update)
        self._bundle_model.update_user_disk_used(bundle.owner_id)
