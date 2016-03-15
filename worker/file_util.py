from contextlib import closing
import copy
from cStringIO import StringIO
import gzip
import os
import subprocess
import tarfile
import zlib


def index_contents(path, depth=1000000):
    """
    Generates an index of the contents of the given path. The index contains
    the fields:
        name: Name of the entry.
        type: Type of the entry, one of 'file', 'directory' or 'link'.
        size: Size of the entry.
        perm: Permissions of the entry.
        link: If type is 'link', where the symbolic link points to.
        contents: If type is 'directory', a list of entries for the contents.
    """
    stat = os.lstat(path)

    result = {}
    result['name'] = os.path.basename(path)
    result['size'] = stat.st_size
    result['perm'] = stat.st_mode & 0777
    if os.path.islink(path):
        result['type'] = 'link'
        result['link'] = os.readlink(path)
    elif os.path.isfile(path):
        result['type'] = 'file'
    elif os.path.isdir(path):
        result['type'] = 'directory'
        if depth > 0:
            result['contents'] = [
                index_contents(os.path.join(path, file_name), depth - 1)
                for file_name in os.listdir(path)]
    return result


def restrict_contents_index(index, path, depth=None):
    """
    Given an index generates a restricted version.
    
    First, it finds the entry in the index for the given path. If no entry is
    found, None is returned.

    Then, if depth is not None, it filters out any entries more than depth levels
    deep. Depth 0, for example, means only the top-level entry is included, and
    no contents. Depth 1 means the contents of the top-level are included, but
    nothing deeper.
    """
    index = copy.deepcopy(index)

    if path:
        names = path.split(os.path.sep)
        for name in names:
            if index['type'] != 'directory':
                return None
            found = False
            for sub_index in index['contents']:
                if sub_index['name'] == name:
                    found = True
                    index = sub_index
                    break
            if not found:
                return None

    if depth is not None:
        def restrict_to_depth(index, depth):
            if index['type'] == 'directory':
                if depth > 0:
                    for sub_index in index['contents']:
                        restrict_to_depth(sub_index, depth - 1)
                else:
                    del index['contents']
        restrict_to_depth(index, depth)

    return index


def tar_gzip_directory(directory_path, follow_symlinks=False,
                       exclude_patterns=[], exclude_names=[]):
    """
    Returns a file-like object containing a tarred and gzipped archive of the
    given directory.

    follow_symlinks: Whether symbolic links should be followed.
    ignore_names: Any top-level directory entries with names in ignore_names
                  are not included.
    exclude_patterns: Any directory entries with the given names at any depth in
                      the directory structure are excluded.
    """
    args = ['tar', 'czf', '-', '-C', directory_path]
    if follow_symlinks:
        args.append('-h')
    if exclude_patterns:
        for pattern in exclude_patterns:
            args.append('--exclude=' + pattern)
    names = [name for name in os.listdir(directory_path) if name not in exclude_names]
    if names:
        args.extend(names)
    else:
        args.extend(['--files-from', '/dev/null'])
    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE)
        return proc.stdout
    except subprocess.CalledProcessError as e:
        raise IOError(e.output)


def un_tar_gzip_directory(fileobj, directory_path):
    """
    Extracts the given file-like object containing a tarred and gzipped archive
    into the given directory. The directory should already exist.

    Raises tarfile.TarError if the archive is not valid.
    """
    directory_path = os.path.realpath(directory_path)
    with tarfile.open(fileobj=fileobj, mode='r|gz') as tar:
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
            self._buffer = ''
            self._finished = False
    
        def read(self, num_bytes=None):
            # Read more data, if we need to.
            while not self._finished and (num_bytes is None or len(self._buffer) < num_bytes):
                chunk = self._fileobj.read(num_bytes) if num_bytes is not None else self._fileobj.read()
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
    
    # Note, that we don't use gzip.GzipFile or the gunzip shell command since
    # they require the input file-like object to support either tell() or
    # fileno(). Our version requires only read() and close().
    return UnGzipStream(fileobj)


def gzip_string(string):
    """
    Gzips the given string.
    """
    with closing(StringIO()) as output_fileobj:
        with gzip.GzipFile(None, 'wb', 6, output_fileobj) as fileobj:
            fileobj.write(string)
        return output_fileobj.getvalue()


def un_gzip_string(string):
    """
    Gunzips the given string.

    Raises an IOError if the archive is not valid.
    """
    with closing(StringIO(string)) as input_fileobj:
        with gzip.GzipFile(None, 'rb', fileobj=input_fileobj) as fileobj:
            return fileobj.read()


def read_file_section(file_path, offset, length):
    """
    Reads length bytes of the given file from the given offset.
    """
    file_size = os.stat(file_path).st_size
    if offset >= file_size:
        return ''
    with open(file_path) as fileobj:
        fileobj.seek(offset, os.SEEK_SET)
        return fileobj.read(length)


def summarize_file(file_path, num_head_lines, num_tail_lines, max_line_length, truncation_text):
    """
    Summarizes the file at the given path, returning a string containing the
    given numbers of lines from beginning and end of the file. If the file needs
    to be truncated, places truncation_text at the truncation point.
    """
    assert(num_head_lines > 0 or num_tail_lines > 0)

    def ensure_ends_with_newline(lines, remove_line_without_newline=False):
        if lines and not lines[-1].endswith('\n'):
            if remove_line_without_newline:
                lines.pop()
            else:
                lines[-1] += '\n'
    
    file_size = os.stat(file_path).st_size
    with open(file_path) as fileobj:
        if file_size > (num_head_lines + num_tail_lines) * max_line_length:
            if num_head_lines > 0:
                # To ensure that the last line is a whole line, we remove the
                # last line if it doesn't have a newline character.
                head_lines = fileobj.read(num_head_lines * max_line_length).splitlines(True)[:num_head_lines]
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
                tail_lines = fileobj.read(num_tail_lines * max_line_length).splitlines(True)[1:][-num_tail_lines:]
                ensure_ends_with_newline(tail_lines)
            
            if num_head_lines > 0 and num_tail_lines > 0:
                lines = head_lines + [truncation_text] + tail_lines
            elif num_head_lines > 0:
                lines = head_lines
            else:
                lines = tail_lines
        else:
            lines = fileobj.readlines()
            ensure_ends_with_newline(lines)
            if len(lines) > num_head_lines + num_tail_lines:
                if num_head_lines > 0 and num_tail_lines > 0:
                    lines = lines[:num_head_lines] + [truncation_text] + lines[-num_tail_lines:]
                elif num_head_lines > 0:
                    lines = lines[:num_head_lines]
                else:
                    lines = lines[-num_tail_lines:]
    
    return ''.join(lines)
