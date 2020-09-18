from contextlib import closing
from io import BytesIO
import gzip
import os
import shutil
import subprocess
import tarfile
import zlib
import bz2

from codalab.common import BINARY_PLACEHOLDER

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


def tar_gzip_directory(
    directory_path, follow_symlinks=False, exclude_patterns=[], exclude_names=[], ignore_file=None
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


def un_tar_directory(fileobj, directory_path, compression='', force=False):
    """
    Extracts the given file-like object containing a tar archive into the given
    directory, which will be created and should not already exist. If it already exists,
    and `force` is `False`, an error is raised. If it already exists, and `force` is `True`,
    the directory is removed and recreated.

    compression specifies the compression scheme and can be one of '', 'gz' or
    'bz2'.

    Raises tarfile.TarError if the archive is not valid.
    """
    directory_path = os.path.realpath(directory_path)
    if force:
        remove_path(directory_path)
    os.mkdir(directory_path)
    with tarfile.open(fileobj=fileobj, mode='r|' + compression) as tar:
        for member in tar:
            # Make sure that there is no trickery going on (see note in
            # TarFile.extractall() documentation.
            member_path = os.path.realpath(os.path.join(directory_path, member.name))
            if not member_path.startswith(directory_path):
                raise tarfile.TarError('Archive member extracts outside the directory.')

            tar.extract(member, directory_path)


def gzip_file(file_path):
    """
    Returns a file-like object containing the gzipped version of the given file.
    """
    args = ['gzip', '-c', '-n', file_path]
    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE)
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


def un_gzip_stream(fileobj):
    """
    Returns a file-like object containing the contents of the given file-like
    object after gunzipping.

    Raises an IOError if the archive is not valid.
    """

    class UnGzipStream(object):
        def __init__(self, fileobj):
            self._fileobj = fileobj
            self._decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
            self._buffer = b''
            self._finished = False

        def read(self, num_bytes=None):
            # Read more data, if we need to.
            while not self._finished and (num_bytes is None or len(self._buffer) < num_bytes):
                chunk = (
                    self._fileobj.read(num_bytes) if num_bytes is not None else self._fileobj.read()
                )
                if chunk:
                    self._buffer += self._decoder.decompress(chunk)
                else:
                    self._buffer += self._decoder.flush()
                    self._finished = True
            if num_bytes is None:
                num_bytes = len(self._buffer)
            result = self._buffer[:num_bytes]
            self._buffer = self._buffer[num_bytes:]
            return result

        def close(self):
            self._fileobj.close()

        def __getattr__(self, name):
            """
            Proxy any methods/attributes besides read() and close() to the
            fileobj (for example, if we're wrapping an HTTP response object.)
            Behavior is undefined if other file methods such as tell() are
            attempted through this proxy.
            """
            return getattr(self._fileobj, name)

    # Note, that we don't use gzip.GzipFile or the gunzip shell command since
    # they require the input file-like object to support either tell() or
    # fileno(). Our version requires only read() and close().
    return UnGzipStream(fileobj)


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


def read_file_section(file_path, offset, length):
    """
    Reads length bytes of the given file from the given offset.
    Return bytes.
    """
    file_size = os.stat(file_path).st_size
    if offset >= file_size:
        return b''
    with open(file_path, 'rb') as fileobj:
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
        file_size = os.stat(file_path).st_size
    except FileNotFoundError:
        return NONE_PLACEHOLDER

    with open(file_path) as fileobj:
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
                lines = fileobj.readlines()
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
    if os.path.islink(path) or os.path.exists(path):
        if os.path.islink(path):
            os.remove(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)


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
