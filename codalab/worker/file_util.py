from contextlib import closing
from io import BytesIO, TextIOWrapper
import gzip
import logging
import os
import shutil
import subprocess
import bz2
import hashlib
import stat

from codalab.common import BINARY_PLACEHOLDER, UsageError
from codalab.common import parse_linked_bundle_url
from codalab.worker.un_gzip_stream import BytesBuffer
from codalab.worker.tar_subdir_stream import TarSubdirStream
from codalab.worker.tar_file_stream import TarFileStream
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
import tempfile
from ratarmountcore import SQLiteIndexedTar, FileInfo
from typing import IO, cast

NONE_PLACEHOLDER = '<none>'

# Patterns to always ignore when zipping up directories
ALWAYS_IGNORE_PATTERNS = ['.git', '._*', '__MACOSX']


def get_tar_version_output():
    """
    Gets the current tar library's version information by returning the stdout
    of running `tar --version`.
    """
    try:
        return subprocess.getoutput('tar --version')
    except subprocess.CalledProcessError as e:
        raise IOError(e.output)


def get_path_exists(path):
    """
    Returns whether the given path exists.
    """
    return FileSystems.exists(path)


def tar_gzip_directory(
    directory_path,
    follow_symlinks=False,
    exclude_patterns=None,
    exclude_names=None,
    ignore_file=None,
):
    """
    Returns a file-like object containing a tarred and gzipped archive of the
    given directory.

    follow_symlinks: Whether symbolic links should be followed.
    exclude_names: Any top-level directory entries with names in exclude_names
                   are not included.
    exclude_patterns: Any directory entries with the given names at any depth in
                      the directory structure are excluded.
    ignore_file: Name of the file where exclusion patterns are read from.
    """
    args = ['tar', 'czf', '-', '-C', directory_path]

    # If the BSD tar library is being used, append --disable-copy to prevent creating ._* files
    if 'bsdtar' in get_tar_version_output():
        args.append('--disable-copyfile')

    if ignore_file:
        # Ignore entries specified by the ignore file (e.g. .gitignore)
        args.append('--exclude-ignore=' + ignore_file)
    if follow_symlinks:
        args.append('-h')
    if not exclude_patterns:
        exclude_patterns = []

    exclude_patterns.extend(ALWAYS_IGNORE_PATTERNS)
    for pattern in exclude_patterns:
        args.append('--exclude=' + pattern)

    if exclude_names:
        for name in exclude_names:
            # Exclude top-level entries provided by exclude_names
            args.append('--exclude=./' + name)
    # Add everything in the current directory
    args.append('.')
    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE)
        return proc.stdout
    except subprocess.CalledProcessError as e:
        raise IOError(e.output)


def zip_directory(
    directory_path,
    follow_symlinks=False,
    exclude_patterns=None,
    exclude_names=None,
    ignore_file=None,
):
    """
    Returns a file-like object containing a zipped archive of the given directory.

    follow_symlinks: Whether symbolic links should be followed.
    exclude_names: Any top-level directory entries with names in exclude_names
                   are not included.
    exclude_patterns: Any directory entries with the given names at any depth in
                      the directory structure are excluded.
    ignore_file: Name of the file where exclusion patterns are read from.
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp_zip_name = os.path.join(tmp, "tmp.zip")
        args = [
            'zip',
            '-rq',
            # Unlike with tar_gzip_directory, we cannot send output to stdout because of this bug in zip
            # (https://bugs.launchpad.net/ubuntu/+source/zip/+bug/1892338). Thus, we have to write to a
            # temporary file and then read the output.
            tmp_zip_name,
            # zip needs to be used with relative paths, so that the final directory structure
            # is correct -- https://stackoverflow.com/questions/11249624/zip-stating-absolute-paths-but-only-keeping-part-of-them.
            '.',
        ]

        if ignore_file:
            # Ignore entries specified by the ignore file (e.g. .gitignore)
            args.append('-x@' + ignore_file)
        if not follow_symlinks:
            args.append('-y')
        if not exclude_patterns:
            exclude_patterns = []

        exclude_patterns.extend(ALWAYS_IGNORE_PATTERNS)
        for pattern in exclude_patterns:
            args.append(f'--exclude=*{pattern}*')

        if exclude_names:
            for name in exclude_names:
                # Exclude top-level entries provided by exclude_names
                args.append('--exclude=./' + name)

        try:
            proc = subprocess.Popen(args, stdout=subprocess.PIPE, cwd=directory_path)
            proc.wait()
            with open(tmp_zip_name, "rb") as out:
                return BytesIO(out.read())
        except subprocess.CalledProcessError as e:
            raise IOError(e.output)


def unzip_directory(fileobj: IO[bytes], directory_path: str, force: bool = False):
    """
    Extracts the given file-like object containing a zip archive into the given
    directory, which will be created and should not already exist. If it already exists,
    and `force` is `False`, an error is raised. If it already exists, and `force` is `True`,
    the directory is removed and recreated.
    """
    directory_path = os.path.realpath(directory_path)
    if force:
        remove_path(directory_path)
    os.mkdir(directory_path)

    # TODO (Ashwin): re-enable streaming zip files once this works again. Disabled because of https://github.com/codalab/codalab-worksheets/issues/3579.
    # with StreamingZipFile(fileobj) as zf:
    #     for member in zf:  # type: ignore
    #         # Make sure that there is no trickery going on (see note in
    #         # ZipFile.extractall() documentation).
    #         member_path = os.path.realpath(os.path.join(directory_path, member.filename))
    #         if not member_path.startswith(directory_path):
    #             raise UsageError('Archive member extracts outside the directory.')
    #         zf.extract(member, directory_path)

    # We have to save fileobj to a temporary file, because unzip doesn't accept input from standard input.
    with tempfile.NamedTemporaryFile() as f:
        shutil.copyfileobj(fileobj, f)
        f.flush()
        proc = subprocess.Popen(
            ['unzip', '-q', f.name, '-d', directory_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        exitcode = proc.wait()
        if exitcode != 0:
            logging.error(
                "Invalid archive upload: failed to unzip .zip file. stderr: <%s>. stdout: <%s>.",
                proc.stderr.read() if proc.stderr is not None else "",
                proc.stdout.read() if proc.stdout is not None else "",
            )
            raise UsageError('Invalid archive upload: failed to unzip .zip file.')


class OpenIndexedArchiveFile(object):
    """Open an archive file (.tar.gz / .gz) specified by the provided path on Azure Blob Storage.
    Also reads this file's associated index.sqlite file, then opens the file as an
    SQLiteIndexedTar object.

    This way, the archive file can be read and specific files can be extracted without
    needing to download the entire archive file.

    Returns the SQLiteIndexedTar object.
    """

    def __init__(self, path: str):
        self.f = FileSystems.open(path, compression_type=CompressionTypes.UNCOMPRESSED)
        self.path = path
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as index_fileobj:
            self.index_file_name = index_fileobj.name
            shutil.copyfileobj(
                FileSystems.open(
                    parse_linked_bundle_url(self.path).index_path,
                    compression_type=CompressionTypes.UNCOMPRESSED,
                ),
                index_fileobj,
            )

    def __enter__(self) -> SQLiteIndexedTar:
        return SQLiteIndexedTar(
            fileObject=self.f,
            tarFileName="contents",
            writeIndex=False,
            clearIndexCache=False,
            indexFilePath=self.index_file_name,
        )

    def __exit__(self, type, value, traceback):
        os.remove(self.index_file_name)


class OpenFile(object):
    """Opens the file indicated by the given file path and returns a handle
    to the associated file object. Can be in a directory.

    The file path can also refer to an archive file on Blob Storage.
    """

    path: str
    mode: str
    gzipped: bool

    def __init__(self, path: str, mode='rb', gzipped=False):
        """Initialize OpenFile.

        Args:
            path (str): Path to open; can be a path on disk or a path on Blob Storage.
            mode (str): Mode with which to open the file. Default is "rb". This is only
            gzipped (bool): Whether the output should be gzipped. Must be True if downloading a directory;
                can be True or False if downloading a file. Note that as of now, gzipping local files from disk
                from OpenFile is not yet supported (only from Blob Storage).
        """
        self.path = path
        self.mode = mode
        self.gzipped = gzipped

    def __enter__(self) -> IO[bytes]:
        linked_bundle_path = parse_linked_bundle_url(self.path)
        if linked_bundle_path.uses_beam and linked_bundle_path.is_archive:
            # Stream an entire, single .gz file from Blob Storage. This is gzipped by default,
            # so if the user requested a gzipped version of the entire file, just read and return it.
            if not linked_bundle_path.is_archive_dir and self.gzipped:
                return FileSystems.open(self.path, compression_type=CompressionTypes.UNCOMPRESSED)
            # Stream an entire, single .tar.gz file from Blob Storage. This is gzipped by default,
            # and directories are always gzipped, so just read and return it.
            if linked_bundle_path.is_archive_dir and not linked_bundle_path.archive_subpath:
                if not self.gzipped:
                    raise IOError("Directories must be gzipped.")
                return FileSystems.open(self.path, compression_type=CompressionTypes.UNCOMPRESSED)
            # If a file path is specified within an archive file on Blob Storage, open the specified path within the archive.
            with OpenIndexedArchiveFile(linked_bundle_path.bundle_path) as tf:
                isdir = lambda finfo: stat.S_ISDIR(finfo.mode)
                # If the archive file is a .tar.gz file, open the specified archive subpath within the archive.
                # If it is a .gz file, open the "/contents" entry, which represents the actual gzipped file.
                fpath = (
                    "/" + linked_bundle_path.archive_subpath
                    if linked_bundle_path.is_archive_dir
                    else "/contents"
                )
                finfo = cast(FileInfo, tf.getFileInfo(fpath))
                if finfo is None:
                    raise FileNotFoundError(fpath)
                if isdir(finfo):
                    # Stream a directory from within the archive
                    if not self.gzipped:
                        raise IOError("Directories must be gzipped.")
                    return GzipStream(TarSubdirStream(self.path))
                else:
                    # Stream a single file from within the archive
                    fs = TarFileStream(tf, finfo)
                    return GzipStream(fs) if self.gzipped else fs
        else:
            # Stream a directory or file from disk storage.
            if os.path.isdir(self.path):
                if not self.gzipped:
                    raise IOError("Directories must be gzipped.")
                return tar_gzip_directory(self.path)
            if self.gzipped:
                raise IOError(
                    "Gzipping local files from disk from OpenFile is not yet supported. Please use file_util.gzip_file instead."
                )
            return open(self.path, self.mode)

    def __exit__(self, type, value, traceback):
        pass


class GzipStream(BytesIO):
    """A stream that gzips a file in chunks.
    """

    def __init__(self, fileobj: IO[bytes]):
        self.__input = fileobj
        self.__buffer = BytesBuffer()
        self.__gzip = gzip.GzipFile(None, mode='wb', fileobj=self.__buffer)

    def read(self, num_bytes=None) -> bytes:
        while num_bytes is None or len(self.__buffer) < num_bytes:
            s = self.__input.read(num_bytes)
            if not s:
                self.__gzip.close()
                break
            self.__gzip.write(s)
        return self.__buffer.read(num_bytes)

    def close(self):
        self.__input.close()


def gzip_file(file_path: str) -> IO[bytes]:
    """
    Returns a file-like object containing the gzipped version of the given file.
    Note: For right now, it's important for gzip to run in a separate process,
    otherwise things on CodaLab grind to a halt!
    """

    if parse_linked_bundle_url(file_path).uses_beam:
        try:
            with OpenFile(file_path, gzipped=True) as file_path_obj:
                return file_path_obj
        except Exception as e:
            raise IOError(e)

    args = ['gzip', '-c', '-n', file_path]
    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE)
        if proc.stdout is None:
            raise IOError("Stdout is empty")
        return proc.stdout
    except subprocess.CalledProcessError as e:
        raise IOError(e.output)


def un_bz2_file(source, dest_path):
    """
    Unzips the source bz2 file object and writes the output to the file at
    dest_path
    """
    # Note, that we don't use bz2.BZ2File or the bunzip2 shell command since
    # they require the input file-like object to support either tell() or
    # fileno(). Our version requires only read() and close().

    BZ2_BUFFER_SIZE = 100 * 1024 * 1024  # Unzip in chunks of 100MB
    with open(dest_path, 'wb') as dest:
        decompressor = bz2.BZ2Decompressor()
        for data in iter(lambda: source.read(BZ2_BUFFER_SIZE), b''):
            dest.write(decompressor.decompress(data))


def gzip_bytestring(bytestring):
    """
    Gzips the given bytestring.  Return bytes.
    """
    with closing(BytesIO()) as output_fileobj:
        with gzip.GzipFile(None, 'wb', 6, output_fileobj) as fileobj:
            fileobj.write(bytestring)
        return output_fileobj.getvalue()


def un_gzip_bytestring(bytestring):
    """
    Gunzips the given bytestring.  Return bytes.
    Raises an IOError if the archive is not valid.
    """
    with closing(BytesIO(bytestring)) as input_fileobj:
        with gzip.GzipFile(None, 'rb', fileobj=input_fileobj) as fileobj:
            return fileobj.read()


def get_file_size(file_path):
    """
    Gets the size of the file, in bytes. If file is not found, raises a
    FileNotFoundError.
    """
    linked_bundle_path = parse_linked_bundle_url(file_path)
    if linked_bundle_path.uses_beam and linked_bundle_path.is_archive:
        # If no archive subpath is specified for a .tar.gz or .gz file, get the uncompressed size of the entire file,
        # or the compressed size of the entire directory.
        if not linked_bundle_path.archive_subpath:
            if linked_bundle_path.is_archive_dir:
                filesystem = FileSystems.get_filesystem(linked_bundle_path.bundle_path)
                return filesystem.size(linked_bundle_path.bundle_path)
            else:
                with OpenFile(linked_bundle_path.bundle_path, 'rb') as fileobj:
                    fileobj.seek(0, os.SEEK_END)
                    return fileobj.tell()
        # If the archive file is a .tar.gz file on Azure, open the specified archive subpath within the archive.
        # If it is a .gz file on Azure, open the "/contents" entry, which represents the actual gzipped file.
        with OpenIndexedArchiveFile(linked_bundle_path.bundle_path) as tf:
            assert linked_bundle_path.is_archive_dir
            fpath = "/" + linked_bundle_path.archive_subpath
            finfo = tf.getFileInfo(fpath)
            if finfo is None:
                raise FileNotFoundError(fpath)
            return finfo.size
    if not get_path_exists(file_path):
        raise FileNotFoundError(file_path)
    # Local path
    return os.stat(file_path).st_size


def read_file_section(file_path, offset, length):
    """
    Reads length bytes of the given file from the given offset.
    Return bytes.
    """
    if offset >= get_file_size(file_path):
        return b''
    with OpenFile(file_path, 'rb') as fileobj:
        fileobj.seek(offset, os.SEEK_SET)
        return fileobj.read(length)


def summarize_file(file_path, num_head_lines, num_tail_lines, max_line_length, truncation_text):
    """
    Summarizes the file at the given path, returning a string containing the
    given numbers of lines from beginning and end of the file. If the file needs
    to be truncated, places truncation_text at the truncation point.
    Unlike other methods, which traffic bytes, this method returns a string.
    """
    assert num_head_lines > 0 or num_tail_lines > 0

    def ensure_ends_with_newline(lines, remove_line_without_newline=False):
        if lines and not lines[-1].endswith('\n'):
            if remove_line_without_newline:
                lines.pop()
            else:
                lines[-1] += '\n'

    try:
        file_size = get_file_size(file_path)
    except FileNotFoundError:
        return NONE_PLACEHOLDER

    with OpenFile(file_path) as f, TextIOWrapper(f) as fileobj:
        if file_size > (num_head_lines + num_tail_lines) * max_line_length:
            if num_head_lines > 0:
                # To ensure that the last line is a whole line, we remove the
                # last line if it doesn't have a newline character.
                try:
                    head_lines = fileobj.read(num_head_lines * max_line_length).splitlines(True)[
                        :num_head_lines
                    ]
                except UnicodeDecodeError:
                    return BINARY_PLACEHOLDER
                ensure_ends_with_newline(head_lines, remove_line_without_newline=True)

            if num_tail_lines > 0:
                # To ensure that the first line is a whole line, we read an
                # extra character and always remove the first line. If the first
                # character is a newline, then the first line will just be
                # empty and the second line is a whole line. If the first
                # character is not a new line, then the first line, had we not
                # read the extra character, would not be a whole line. Thus, it
                # should also be dropped.
                fileobj.seek(file_size - num_tail_lines * max_line_length - 1, os.SEEK_SET)
                try:
                    tail_lines = fileobj.read(num_tail_lines * max_line_length).splitlines(True)[
                        1:
                    ][-num_tail_lines:]
                except UnicodeDecodeError:
                    return BINARY_PLACEHOLDER
                ensure_ends_with_newline(tail_lines)

            if num_head_lines > 0 and num_tail_lines > 0:
                lines = head_lines + [truncation_text] + tail_lines
            elif num_head_lines > 0:
                lines = head_lines
            else:
                lines = tail_lines
        else:
            try:
                lines = fileobj.read().splitlines(True)
            except UnicodeDecodeError:
                return BINARY_PLACEHOLDER
            ensure_ends_with_newline(lines)
            if len(lines) > num_head_lines + num_tail_lines:
                if num_head_lines > 0 and num_tail_lines > 0:
                    lines = lines[:num_head_lines] + [truncation_text] + lines[-num_tail_lines:]
                elif num_head_lines > 0:
                    lines = lines[:num_head_lines]
                else:
                    lines = lines[-num_tail_lines:]

    return ''.join(lines)


def get_path_size(path, exclude_names=[], ignore_nonexistent_path=False):
    """
    Returns the size of the contents of the given path, in bytes.

    If path is a directory, any directory entries in exclude_names will be
    ignored.

    If ignore_nonexistent_path is True and the input path is nonexistent, the value
    0 is returned. Else, an exception is raised (FileNotFoundError).
    """
    if parse_linked_bundle_url(path).uses_beam:
        # On Azure, use Apache Beam methods, not native os methods,
        # to get the path size.

        # Get the size of the specified path (file / directory).
        # This will only get the right size of files, not of directories (but we don't
        # store any bundles as directories on Azure).
        return get_file_size(path)

    try:
        result = os.lstat(path).st_size
    except FileNotFoundError:
        if ignore_nonexistent_path:
            # If we are to ignore nonexistent paths, return the size of this path as 0
            return 0
        # Raise the FileNotFoundError
        raise
    if not os.path.islink(path) and os.path.isdir(path):
        for child in os.listdir(path):
            if child not in exclude_names:
                try:
                    full_child_path = os.path.join(path, child)
                except UnicodeDecodeError:
                    full_child_path = os.path.join(path.decode('utf-8'), child.decode('utf-8'))
                result += get_path_size(full_child_path, ignore_nonexistent_path=True)
    return result


def remove_path(path):
    """
    Removes a path if it exists.
    """
    # We need to include this first if statement
    # to allow local broken symbolic links to be deleted
    # as well (which aren't matched by the Beam methods).
    if os.path.islink(path):
        os.remove(path)
    elif get_path_exists(path):
        FileSystems.delete([path])


def path_is_parent(parent_path, child_path):
    """
    Given a parent_path and a child_path, determine if the child path
    is a strict subpath of the parent_path. In the case that the resolved
    parent_path is equivalent to the resolved child_path, this function returns
    False.

    Note that this function does not dereference symbolic links.
    """
    # Remove relative path references.
    parent_path = os.path.abspath(parent_path)
    child_path = os.path.abspath(child_path)

    # Explicitly handle the case where the parent_path equals the child_path
    if parent_path == child_path:
        return False

    # Compare the common path of the parent and child path with the common
    # path of just the parent path. Using the commonpath method on just
    # the parent path will regularize the path name in the same way as the
    # comparison that deals with both paths, removing any trailing path separator.
    return os.path.commonpath([parent_path]) == os.path.commonpath([parent_path, child_path])


def sha256(file: str) -> str:
    """
    Return the sha256 of the contents of the given file.
    """
    sha256_hash = hashlib.sha256()
    with open(file, "rb") as f:
        # Read and update hash string value in blocks of 4K -- good for large files
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
