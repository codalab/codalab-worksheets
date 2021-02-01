import re
import os
import shutil

from codalab.common import UsageError
from codalab.lib import crypt_util, file_util, path_util, zip_util
from codalab.worker.file_util import tar_gzip_directory
from codalab.lib.beam.filesystems import AZURE_BLOB_ACCOUNT_NAME
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from codalab.common import parse_linked_bundle_url
from codalab.worker.bundle_state import LinkFormat
import tempfile
import logging
from codalab.lib.beam.ratarmount import SQLiteIndexedTar


class UploadManager(object):
    """
    Contains logic for uploading bundle data to the bundle store and updating
    the associated bundle metadata in the database.
    """

    def __init__(self, bundle_model, bundle_store):
        # exclude these patterns by default
        DEFAULT_EXCLUDE_PATTERNS = ['.DS_Store', '__MACOSX', '^\._.*']
        self._bundle_model = bundle_model
        self._bundle_store = bundle_store
        self._default_exclude_patterns = DEFAULT_EXCLUDE_PATTERNS

    def upload_to_bundle_store(
        self,
        bundle,
        sources,
        follow_symlinks,
        exclude_patterns,
        remove_sources,
        git,
        unpack,
        simplify_archives,
        use_azure_blob_beta,
    ):
        """
        Uploads contents for the given bundle to the bundle store.

        |sources|: specifies the locations of the contents to upload. Each element is
                   either a URL, a local path or a tuple (filename, binary file-like object).
        |follow_symlinks|: for local path(s), whether to follow (resolve) symlinks,
                           but only if remove_sources is False.
        |exclude_patterns|: for local path(s), don't upload these patterns (e.g., *.o),
                            but only if remove_sources is False.
        |remove_sources|: for local path(s), whether |sources| should be removed
        |git|: for URLs, whether |source| is a git repo to clone.
        |unpack|: for each source in |sources|, whether to unpack it if it's an archive.
        |simplify_archives|: whether to simplify unpacked archives so that if they
                             contain a single file, the final path is just that file,
                             not a directory containing that file.
        |use_azure_blob_beta|: whether to use Azure Blob Storage.

        If |sources| contains one source, then the bundle contents will be that source.
        Otherwise, the bundle contents will be a directory with each of the sources.
        Exceptions:
        - If |git|, then each source is replaced with the result of running 'git clone |source|'
        - If |unpack| is True or a source is an archive (zip, tar.gz, etc.), then unpack the source.
        """
        exclude_patterns = (
            self._default_exclude_patterns + exclude_patterns
            if exclude_patterns
            else self._default_exclude_patterns
        )
        # If on Azure Blob Storage, handle source download / unpacking
        # in a temporary directory. Otherwise, download / unpack the bundle
        # directly in its final location in the disk-based bundle store.
        if use_azure_blob_beta:
            bundle_path = tempfile.mkdtemp()
        else:
            bundle_path = self._bundle_store.get_bundle_location(bundle.uuid)
        try:
            path_util.make_directory(bundle_path)
            # Note that for uploads with a single source, the directory
            # structure is simplified at the end.
            for source in sources:
                is_url, is_local_path, is_fileobj, filename = self._interpret_source(source)
                source_output_path = os.path.join(bundle_path, filename)
                if is_url:
                    if git:
                        source_output_path = file_util.strip_git_ext(source_output_path)
                        file_util.git_clone(source, source_output_path)
                    else:
                        file_util.download_url(source, source_output_path)
                        if unpack and self._can_unpack_file(source_output_path):
                            self._unpack_file(
                                source_output_path,
                                zip_util.strip_archive_ext(source_output_path),
                                remove_source=True,
                                simplify_archive=simplify_archives,
                            )
                elif is_local_path:
                    # TODO (Ashwin): check if this code path is used (it appears not),
                    # and if not, remove it.
                    source_path = path_util.normalize(source)
                    path_util.check_isvalid(source_path, 'upload')

                    if unpack and self._can_unpack_file(source_path):
                        self._unpack_file(
                            source_path,
                            zip_util.strip_archive_ext(source_output_path),
                            remove_source=remove_sources,
                            simplify_archive=simplify_archives,
                        )
                    elif remove_sources:
                        path_util.rename(source_path, source_output_path)
                    else:
                        path_util.copy(
                            source_path,
                            source_output_path,
                            follow_symlinks=follow_symlinks,
                            exclude_patterns=exclude_patterns,
                        )
                elif is_fileobj:
                    if unpack and zip_util.path_is_archive(filename):
                        self._unpack_fileobj(
                            source[0],
                            source[1],
                            zip_util.strip_archive_ext(source_output_path),
                            simplify_archive=simplify_archives,
                        )
                    else:
                        # We reach this code path if we are uploading archive files to Azure
                        # (in which case unpack is set to False), or uploading a single file regularly.
                        with open(source_output_path, 'wb') as out:
                            shutil.copyfileobj(source[1], out)

            if len(sources) == 1:
                self._simplify_directory(bundle_path)

            if use_azure_blob_beta:
                # Now, upload the contents of the temp directory to Azure Blob Storage.

                # is_directory is True if the bundle is a directory and False if it is a single file.
                is_directory = os.path.isdir(bundle_path)
                link_url = f"azfs://{AZURE_BLOB_ACCOUNT_NAME}/bundles/{bundle.uuid}{'/contents.tar.gz' if is_directory else '/contents'}"
                with FileSystems.create(
                    link_url, compression_type=CompressionTypes.UNCOMPRESSED
                ) as out, tempfile.NamedTemporaryFile(
                    suffix=".tar.gz"
                ) as tmp_tar_file, tempfile.NamedTemporaryFile(
                    suffix=".sqlite"
                ) as tmp_index_file:
                    if is_directory:
                        tar_gzip_directory(bundle_path, output_path=tmp_tar_file.name)
                        shutil.copyfileobj(tmp_tar_file, out)
                        with open(tmp_tar_file.name, "rb") as ttf:
                            SQLiteIndexedTar(
                                fileObject=ttf,
                                tarFileName=bundle.uuid,
                                writeIndex=True,
                                clearIndexCache=True,
                                indexFileName=tmp_index_file.name,
                            )
                        with FileSystems.create(
                            link_url.replace("/contents.tar.gz", "/index.sqlite"),
                            compression_type=CompressionTypes.UNCOMPRESSED,
                        ) as out_index_file, open(tmp_index_file.name, "rb") as tif:
                            shutil.copyfileobj(tif, out_index_file)
                    else:
                        shutil.copyfileobj(open(bundle_path, 'rb'), out)

                # If uploading a file using Azure Blob Storage, we set the link_url and link_format
                # appropriately so that the bundle is recognized as a .zip file on Azure Blob Storage.
                self._bundle_model.update_bundle(
                    bundle,
                    {
                        'metadata': {
                            'link_url': link_url,
                            'link_format': LinkFormat.ZIP if is_directory else LinkFormat.RAW,
                        }
                    },
                )
        except UsageError:
            if FileSystems.exists(bundle_path):
                path_util.remove(bundle_path)
            raise
        finally:
            if use_azure_blob_beta:
                shutil.rmtree(bundle_path) if os.path.isdir(bundle_path) else os.remove(bundle_path)

    def _interpret_source(self, source):
        is_url, is_local_path, is_fileobj = False, False, False
        if isinstance(source, str):
            if path_util.path_is_url(source):
                is_url = True
                source = source.rsplit('?', 1)[0]  # Remove query string from URL, if present
            else:
                is_local_path = True
            filename = os.path.basename(os.path.normpath(source))
        else:
            is_fileobj = True
            filename = source[0]
        return is_url, is_local_path, is_fileobj, filename

    def _can_unpack_file(self, path):
        return os.path.isfile(path) and zip_util.path_is_archive(path)

    def _unpack_file(self, source_path, dest_path, remove_source, simplify_archive):
        zip_util.unpack(zip_util.get_archive_ext(source_path), source_path, dest_path)
        if remove_source:
            path_util.remove(source_path)
        if simplify_archive:
            self._simplify_archive(dest_path)

    def _unpack_fileobj(self, source_filename, source_fileobj, dest_path, simplify_archive):
        zip_util.unpack(zip_util.get_archive_ext(source_filename), source_fileobj, dest_path)
        if simplify_archive:
            self._simplify_archive(dest_path)

    def _simplify_archive(self, path):
        """
        Modifies |path| in place: If |path| is a directory containing exactly
        one file / directory that is not ignored, then replace |path| with that
        file / directory.
        """
        if not os.path.isdir(path):
            return

        files = [f for f in os.listdir(path) if not self._ignore_file_in_archive(f)]
        if len(files) == 1:
            self._simplify_directory(path, files[0])

    def _ignore_file_in_archive(self, filename):
        matchers = [re.compile(s) for s in self._default_exclude_patterns]
        return any([matcher.match(filename) for matcher in matchers])

    def _simplify_directory(self, path, child_path=None):
        """
        Modifies |path| in place: If the |path| directory contains exactly
        one file / directory, then replace |path| with that file / directory.
        """

        # If the file is using Azure, then use the Apache Beam FileSystems.rename
        # method to perform this simplification.
        if parse_linked_bundle_url(path).uses_beam:
            if child_path is None:
                child_path = FileSystems.match([path + "/*"])[0].metadata_list[0].path
            FileSystems.rename([child_path], [path])
            return

        if child_path is None:
            child_path = os.listdir(path)[0]
        temp_path = path + crypt_util.get_random_string()
        path_util.rename(path, temp_path)
        child_path = os.path.join(temp_path, child_path)
        path_util.rename(child_path, path)
        path_util.remove(temp_path)

    def has_contents(self, bundle):
        return FileSystems.exists(self._bundle_store.get_bundle_location(bundle.uuid))

    def cleanup_existing_contents(self, bundle):
        self._bundle_store.cleanup(bundle.uuid, dry_run=False)
        bundle_update = {'data_hash': None, 'metadata': {'data_size': 0}}
        self._bundle_model.update_bundle(bundle, bundle_update)
        self._bundle_model.update_user_disk_used(bundle.owner_id)
