"""
zip_util provides helpers for unzipping a few standard archive types when
the user uploads an archive of a known type.
"""
from fnmatch import fnmatch
import os
import shutil
import tarfile
import tempfile
import logging
from typing import IO

from codalab.common import UsageError
from codalab.worker.file_util import (
    gzip_file,
    tar_gzip_directory,
    un_bz2_file,
    unzip_directory,
    GzipStream,
)
from codalab.worker.un_gzip_stream import un_gzip_stream, UnBz2Stream, ZipToTarStream
from codalab.worker.un_tar_directory import un_tar_directory


# Files with these extensions are considered archive.
ARCHIVE_EXTS = ['.tar.gz', '.tgz', '.tar.bz2', '.zip', '.gz', '.bz2']
ARCHIVE_EXTS_DIR = ['.tar.gz', '.tgz', '.tar.bz2', '.zip']


def path_is_archive(path):
    if isinstance(path, str):
        for ext in ARCHIVE_EXTS:
            if path.endswith(ext):
                return True
    return False


def get_archive_ext(fname):
    """Returns the extension for fname if it's an archive, empty string otherwise."""
    for ext in ARCHIVE_EXTS:
        if fname.endswith(ext):
            return ext
    return ''


def strip_archive_ext(path):
    for ext in ARCHIVE_EXTS:
        if path.endswith(ext):
            return path[: -len(ext)]
    raise UsageError('Not an archive: %s' % path)


def unpack(ext: str, source: IO[bytes], dest_path: str):
    """Unpack the archive |source| to |dest_path|.

    Args:
        ext (str): Extension of the archive.
        source (IO[bytes]): File handle to the source.
        dest_path ([type]): Destination path to unpack to.

    """
    try:

        if ext == '.tar.gz' or ext == '.tgz':
            un_tar_directory(source, dest_path, 'gz')
        elif ext == '.tar.bz2':
            un_tar_directory(source, dest_path, 'bz2')
        elif ext == '.bz2':
            un_bz2_file(source, dest_path)
        elif ext == '.gz':
            with open(dest_path, 'wb') as f:
                shutil.copyfileobj(un_gzip_stream(source), f)
        elif ext == '.zip':
            unzip_directory(source, dest_path)
        else:
            raise UsageError('Not an archive.')
    except (tarfile.TarError, IOError) as e:
        logging.error("Invalid archive upload: failed to unpack archive: %s", e)
        raise UsageError('Invalid archive upload: failed to unpack archive.')


def unpack_to_archive(ext: str, source: IO[bytes]) -> IO[bytes]:
    """Unpack the archive |source| and returns the unpacked fileobj.
    If |source| is an archive, unpacks to a .tar.gz archive file.
    If |source| is a non-archive file, unpacks to a .gz file.

    Args:
        ext (str): Extension of the source archive.
        source (IO[bytes]): File handle to the source.

    Returns:
        IO[bytes]: File object with the archive.
    """
    try:

        if ext == '.tar.gz' or ext == '.tgz':
            return source
        elif ext == '.tar.bz2':
            return GzipStream(UnBz2Stream(source))
        elif ext == '.bz2':
            return GzipStream(UnBz2Stream(source))
        elif ext == '.gz':
            return source
        elif ext == '.zip':
            return GzipStream(ZipToTarStream(source))
        else:
            raise UsageError('Not an archive.')
    except (tarfile.TarError, IOError) as e:
        logging.error("Invalid archive upload: failed to unpack archive: %s", e)
        raise UsageError('Invalid archive upload: failed to unpack archive: %s' % e)


def pack_files_for_upload(
    sources,
    should_unpack,
    follow_symlinks,
    exclude_patterns=None,
    force_compression=False,
    ignore_file=None,
):
    """
    Create a single flat tarfile containing all the sources.
    Caller is responsible for closing the returned fileobj.

    Note: It may be possible to achieve additional speed gains on certain
    cases if we disable compression when tar-ing directories. But for now,
    force_compression only affects the case of single, uncompressed files.

    :param sources: list of paths to files to pack
    :param should_unpack: will unpack archives iff True
    :param follow_symlinks: will follow symlinks if True else behavior undefined
    :param exclude_patterns: list of glob patterns for files to ignore, or
                             None to include all files
    :param force_compression: True to always use compression
    :param ignore_file: Name of the file where exclusion patterns are read from
                        when archiving
    :return: dict with {
        'fileobj': <file object of archive>,
        'filename': <name of archive file>,
        'filesize': <size of archive in bytes, or None if unknown>,
        'should_unpack': <True iff archive should be unpacked at server>
        }
    """
    exclude_patterns = exclude_patterns or []

    def resolve_source(source):
        # Resolve symlink if desired
        resolved_source = source
        if follow_symlinks:
            resolved_source = os.path.realpath(source)
            if not os.path.exists(resolved_source):
                raise UsageError('Broken symlink')
        elif os.path.islink(source):
            raise UsageError('Not following symlinks.')
        return resolved_source

    sources = list(map(resolve_source, sources))

    # For efficiency, return single files and directories directly
    if len(sources) == 1:
        source = sources[0]
        filename = os.path.basename(source)
        if os.path.isdir(sources[0]):
            archived = tar_gzip_directory(
                source,
                follow_symlinks=follow_symlinks,
                exclude_patterns=exclude_patterns,
                ignore_file=ignore_file,
            )
            return {
                'fileobj': archived,
                'filename': filename + '.tar.gz',
                'filesize': None,
                'should_unpack': True,
            }
        elif path_is_archive(source):
            return {
                'fileobj': open(source, mode='rb'),
                'filename': filename,
                'filesize': os.path.getsize(source),
                'should_unpack': should_unpack,
            }
        elif force_compression:
            return {
                'fileobj': gzip_file(source),
                'filename': filename + '.gz',
                'filesize': None,
                'should_unpack': True,
            }
        else:
            return {
                'fileobj': open(source, mode='rb'),
                'filename': filename,
                'filesize': os.path.getsize(source),
                'should_unpack': False,
            }

    # Build archive file incrementally from all sources
    # TODO: For further optimization, could either uses a temporary named pipe
    # or a wrapper around a TemporaryFile to concurrently write to the tarfile
    # while the REST client reads and sends it to the server. At the moment,
    # we wait for the tarfile to be created until we rewind and pass the file
    # to the client to be sent to the server.
    scratch_dir = tempfile.mkdtemp()
    archive_fileobj = tempfile.SpooledTemporaryFile()
    archive = tarfile.open(name='we', mode='w:gz', fileobj=archive_fileobj)

    def should_exclude(fn):
        basefn = os.path.basename(fn)
        return any(fnmatch(basefn, p) for p in exclude_patterns)

    def filter(tarinfo):
        return None if should_exclude(tarinfo.name) else tarinfo

    for source in sources:
        # Add file to archive, or add files recursively if directory
        archive.add(source, arcname=os.path.basename(source), recursive=True, filter=filter)

    # Clean up, rewind archive file, and return it
    archive.close()
    shutil.rmtree(scratch_dir)
    filesize = archive_fileobj.tell()
    archive_fileobj.seek(0)
    return {
        'fileobj': archive_fileobj,
        'filename': 'contents.tar.gz',
        'filesize': filesize,
        'should_unpack': True,
    }
