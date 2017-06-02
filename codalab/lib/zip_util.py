"""
zip_util provides helpers for unzipping a few standard archive types when
the user uploads an archive of a known type.
"""
from fnmatch import fnmatch
import os
import shutil
import subprocess
import tarfile
import tempfile

from codalab.common import UsageError
from codalab.lib import path_util
from codalabworker.file_util import (
    gzip_file,
    tar_gzip_directory,
    un_gzip_stream,
    un_tar_directory,
)


# Files with these extensions are considered archive.
ARCHIVE_EXTS = ['.tar.gz', '.tgz', '.tar.bz2', '.zip', '.gz']


def path_is_archive(path):
    if isinstance(path, basestring):
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
            return path[:-len(ext)]
    raise UsageError('Not an archive: %s' % path)


def unpack(ext, source, dest_path):
    """
    Unpack the archive |source| to |dest_path|.
    Note: |source| can be a file handle or a path.
    |ext| contains the extension of the archive.
    """
    if ext != '.zip':
        close_source = False
        try:
            if isinstance(source, basestring):
                source = open(source, 'rb')
                close_source = True

            if ext == '.tar.gz' or ext == '.tgz':
                un_tar_directory(source, dest_path, 'gz')
            elif ext == '.tar.bz2':
                un_tar_directory(source, dest_path, 'bz2')
            elif ext == '.gz':
                with open(dest_path, 'wb') as f:
                    shutil.copyfileobj(un_gzip_stream(source), f)
            else:
                raise UsageError('Not an archive.')
        except (tarfile.TarError, IOError):
            raise UsageError('Invalid archive upload.')
        finally:
            if close_source:
                source.close()
    else:
        delete_source = False
        try:
            # unzip doesn't accept input from standard input, so we have to save
            # to a temporary file.
            if not isinstance(source, basestring):
                temp_path = dest_path + '.zip'
                with open(temp_path, 'wb') as f:
                    shutil.copyfileobj(source, f)
                source = temp_path
                delete_source = True

            exitcode = subprocess.call(['unzip', '-q', source, '-d', dest_path])
            if exitcode != 0:
                raise UsageError('Invalid archive upload.')
        finally:
            if delete_source:
                path_util.remove(source)


def pack_files_for_upload(sources, should_unpack, follow_symlinks,
                          exclude_patterns=None, force_compression=False):
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
    :return: dict with {
        'fileobj': <file object of archive>,
        'filename': <name of archive file>,
        'filesize': <size of archive in bytes, or None if unknown>,
        'should_unpack': <True iff archive should be unpacked at server>,
        'should_simplify': <True iff directory should be 'simplified' at server>
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

    sources = map(resolve_source, sources)

    # For efficiency, return single files and directories directly
    if len(sources) == 1:
        source = sources[0]
        filename = os.path.basename(source)
        if os.path.isdir(sources[0]):
            archived = tar_gzip_directory(
                source, follow_symlinks=follow_symlinks,
                exclude_patterns=exclude_patterns)
            return {
                'fileobj': archived,
                'filename': filename + '.tar.gz',
                'filesize': None,
                'should_unpack': True,
                'should_simplify': False,
            }
        elif path_is_archive(source):
            return {
                'fileobj': open(source),
                'filename': filename,
                'filesize': os.path.getsize(source),
                'should_unpack': should_unpack,
                'should_simplify': True,
            }
        elif force_compression:
            return {
                'fileobj': gzip_file(source),
                'filename': filename + '.gz',
                'filesize': None,
                'should_unpack': True,
                'should_simplify': False,
            }
        else:
            return {
                'fileobj': open(source),
                'filename': filename,
                'filesize': os.path.getsize(source),
                'should_unpack': False,
                'should_simplify': False,
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

    for source in sources:
        if should_unpack and path_is_archive(source):
            # Unpack archive into scratch space
            dest_basename = strip_archive_ext(os.path.basename(source))
            dest_path = os.path.join(scratch_dir, dest_basename)
            unpack(get_archive_ext(source), source, dest_path)

            # Add file or directory to archive
            archive.add(dest_path, arcname=dest_basename, recursive=True)
        else:
            # Add file to archive, or add files recursively if directory
            archive.add(source, arcname=os.path.basename(source),
                        recursive=True, exclude=should_exclude)

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
        'should_simplify': False,
    }
