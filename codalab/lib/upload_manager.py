import os
import shutil
from typing import Optional, Union, Tuple, IO, cast

from codalab.common import UsageError
from codalab.common import StorageType
from codalab.lib import crypt_util, file_util, path_util
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
        self,
        bundle: Bundle,
        source: Source,
        git: bool,
        unpack: bool,
        simplify_archives: bool,
        use_azure_blob_beta: bool,
    ):
        """
        Uploads contents for the given bundle to the bundle store.

        |source|: specifies the location of the contents to upload. Each element is
                   either a URL or a tuple (filename, binary file-like object).
        |git|: for URLs, whether |source| is a git repo to clone.
        |unpack|: whether to unpack |source| if it's an archive.
        |simplify_archives|: whether to simplify unpacked archives so that if they
                             contain a single file, the final path is just that file,
                             not a directory containing that file.
        |use_azure_blob_beta|: whether to use Azure Blob Storage.

        Exceptions:
        - If |git|, then the bundle contains the result of running 'git clone |source|'
        - If |unpack| is True or a source is an archive (zip, tar.gz, etc.), then unpack the source.
        """
        bundle_path = self._bundle_store.get_bundle_location(bundle.uuid)
        try:
            path_util.make_directory(bundle_path)
            # Note that the directory structure is simplified at the end.
            is_url, is_fileobj, filename = self._interpret_source(source)
            source_output_path = os.path.join(bundle_path, filename)
            if is_url:
                assert isinstance(source, str)
                if git:
                    source_output_path = file_util.strip_git_ext(source_output_path)
                    file_util.git_clone(source, source_output_path)
                else:
                    file_util.download_url(source, source_output_path)
                    if unpack and self._can_unpack_file(source_output_path):
                        self._unpack_file(
                            source_output_path,
                            self.zip_util.strip_archive_ext(source_output_path),
                            remove_source=True,
                            simplify_archive=simplify_archives,
                        )
            elif is_fileobj:
                if unpack and self.zip_util.path_is_archive(filename):
                    self._unpack_fileobj(
                        source[0],
                        source[1],
                        self.zip_util.strip_archive_ext(source_output_path),
                        simplify_archive=simplify_archives,
                    )
                else:
                    with open(source_output_path, 'wb') as out:
                        shutil.copyfileobj(cast(IO, source[1]), out)

            self._simplify_directory(bundle_path)
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

    def _can_unpack_file(self, path):
        return os.path.isfile(path) and self.zip_util.path_is_archive(path)

    def _unpack_file(self, source_path, dest_path, remove_source, simplify_archive):
        self.zip_util.unpack(self.zip_util.get_archive_ext(source_path), source_path, dest_path)
        if remove_source:
            path_util.remove(source_path)
        if simplify_archive:
            self._simplify_archive(dest_path)

    def _unpack_fileobj(self, source_filename, source_fileobj, dest_path, simplify_archive):
        self.zip_util.unpack(
            self.zip_util.get_archive_ext(source_filename), source_fileobj, dest_path
        )
        if simplify_archive:
            self._simplify_archive(dest_path)

    def _simplify_archive(self, path: str) -> None:
        """
        Modifies |path| in place: If |path| is a directory containing exactly
        one file / directory, then replace |path| with that file / directory.
        """
        if not os.path.isdir(path):
            return

        files = os.listdir(path)
        if len(files) == 1:
            self._simplify_directory(path, files[0])

    def _simplify_directory(self, path: str, child_path: Optional[str] = None) -> None:
        """
        Modifies |path| in place by replacing |path| with its first child file / directory.
        This method should only be called after checking to see if the |path| directory
        contains exactly one file / directory.
        """
        if child_path is None:
            child_path = os.listdir(path)[0]

        temp_path = path + crypt_util.get_random_string()
        path_util.rename(path, temp_path)
        child_path = os.path.join(temp_path, child_path)
        path_util.rename(child_path, path)
        path_util.remove(temp_path)

    def has_contents(self, bundle):
        # TODO: make this non-fs-specific.
        return os.path.exists(self._bundle_store.get_bundle_location(bundle.uuid))

    def cleanup_existing_contents(self, bundle):
        self._bundle_store.cleanup(bundle.uuid, dry_run=False)
        bundle_update = {'data_hash': None, 'metadata': {'data_size': 0}}
        self._bundle_model.update_bundle(bundle, bundle_update)
        self._bundle_model.update_user_disk_used(bundle.owner_id)
