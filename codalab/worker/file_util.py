from contextlib import closing
from io import BytesIO, TextIOWrapper
import gzip
import os
import shutil
import subprocess
import bz2

from codalab.common import BINARY_PLACEHOLDER, UsageError
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
import tempfile

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


def unzip_directory(fileobj_or_name, directory_path, force=False):
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

    def do_unzip(filename):
        # TODO(Ashwin): preserve permissions with -X.
        exitcode = subprocess.call(['unzip', '-q', filename, '-d', directory_path])
        if exitcode != 0:
            raise UsageError('Invalid archive upload. ')

    # If fileobj_or_name is a file object, we have to save it
    # to a temporary file, because unzip doesn't accept input from standard input.
    if not isinstance(fileobj_or_name, str):
        with tempfile.NamedTemporaryFile() as f:
            shutil.copyfileobj(fileobj_or_name, f)
            f.seek(0)
            do_unzip(f.name)
    else:
        # In this case, fileobj_or_name is a file name.
        do_unzip(fileobj_or_name)


def open_file(file_path, mode='r', compression_type=CompressionTypes.UNCOMPRESSED):
    """
    Opens the given file. Can be in a directory.
    """
    return FileSystems.open(file_path, mode, compression_type)


def gzip_file(file_path):
    """
    Returns a file-like object containing the gzipped version of the given file.
    Note: For right now, it's important for gzip to run in a separate process,
    otherwise things on CodaLab grind to a halt!
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
    with FileSystems.create(dest_path, compression_type=CompressionTypes.UNCOMPRESSED) as dest:
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
    if not get_path_exists(file_path):
        raise FileNotFoundError
    # TODO: add a FileSystems.size() method to Apache Beam to make this less verbose.
    filesystem = FileSystems.get_filesystem(file_path)
    return filesystem.size(file_path)


def read_file_section(file_path, offset, length):
    """
    Reads length bytes of the given file from the given offset.
    Return bytes.
    """
    if offset >= get_file_size(file_path):
        return b''
    with open_file(file_path, 'rb') as fileobj:
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

    with TextIOWrapper(open_file(file_path)) as fileobj:
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
