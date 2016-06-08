"""
path_util contains helpers for working with local filesystem paths.
There are a few classes of methods provided here:

  Functions to normalize paths and check that they are in normal form:
    normalize, check_isvalid, check_isdir, check_isfile, path_is_url

  Functions to list directories and to deal with subpaths of paths:
    safe_join, get_relative_path, ls, recursive_ls

  Functions to read files to compute hashes, write results to stdout, etc:
    getmtime, get_size, hash_directory, hash_file_contents

  Functions that modify that filesystem in controlled ways:
    copy, make_directory, set_write_permissions, rename, remove
"""
import errno
import hashlib
import itertools
import os
import shutil
import subprocess
import sys

from codalab.common import (
  precondition,
  UsageError,
)
from codalab.lib import file_util


# Block sizes and canonical strings used when hashing files.
BLOCK_SIZE = 0x40000
FILE_PREFIX = 'file'
LINK_PREFIX = 'link'


def path_error(message, path):
    """
    Raised when a user-supplied path causes an exception.
    """
    return UsageError(message + ': ' + path)


################################################################################
# Functions to normalize paths and check that they are in normal form.
################################################################################


def normalize(path):
    """
    Return the absolute path of the location specified by the given path.
    This path is returned in a "canonical form", without ~'s, .'s, ..'s.
    """
    if path == '-':
        return '/dev/stdin'
    elif path_is_url(path):
        return path
    else:
        return os.path.abspath(os.path.expanduser(path))


def check_isvalid(path, fn_name):
    """
    Raise a PreconditionViolation if the path is not absolute or normalized.
    Raise a UsageError if the file at that path does not exist.
    """
    precondition(os.path.isabs(path), '%s got relative path: %s' % (fn_name, path))
    # Broken symbolic links are valid paths, so we use lexists instead of exists.
    if not os.path.lexists(path):
        raise path_error('%s got non-existent path:' % (fn_name,), path)


def check_isdir(path, fn_name):
    """
    Check that the path is valid, then raise UsageError if the path is a file.
    """
    check_isvalid(path, fn_name)
    if not os.path.isdir(path):
        raise path_error('%s got non-directory:' % (fn_name,), path)


def check_isfile(path, fn_name):
    """
    Check that the path is valid, then raise UsageError if the path is a file.
    """
    check_isvalid(path, fn_name)
    if os.path.isdir(path):
        raise path_error('%s got directory:' % (fn_name,), path)


def path_is_url(path):
    if isinstance(path, basestring):
        for prefix in ['http', 'https', 'ftp', 'file']:
            if path.startswith(prefix + '://'):
                return True
    return False

################################################################################
# Functions to list directories and to deal with subpaths of paths.
################################################################################


def safe_join(*paths):
    """
    Join a sequence of paths but filter out any that are empty. Used for targets.
    Note that os.path.join has this functionality EXCEPT at the end of the list,
    which causes problems when a target subpath is empty.
    """
    return os.path.join(*filter(None, paths))


def get_relative_path(root, path):
    """
    Return the relative path from root to path, which should be nested under root.
    """
    precondition(path.startswith(root), '%s is not under %s' % (path, root))
    return path[len(root):]


def ls(path):
    """
    Return a (list of directories, list of files) in the given directory.
    """
    check_isdir(path, 'ls')
    (directories, files) = ([], [])
    for file_name in os.listdir(path):
        if os.path.isfile(os.path.join(path, file_name)):
            files.append(file_name)
        else:
            directories.append(file_name)
    return (directories, files)


def recursive_ls(path):
    """
    Return a (list of directories, list of files) in the given directory and
    all of its nested subdirectories. All paths returned are absolute.

    Symlinks are returned in the list of files, even if they point to directories.
    This makes it possible to distinguish between real and symlinked directories
    when computing the hash of a directory. This function will NOT descend into
    symlinked directories.
    """
    check_isdir(path, 'recursive_ls')
    (directories, files) = ([], [])
    for (root, _, file_names) in os.walk(path):
        assert(os.path.isabs(root)), 'Got relative root in os.walk: %s' % (root,)
        directories.append(root)
        for file_name in file_names:
            files.append(os.path.join(root, file_name))
        # os.walk ignores symlinks to directories, but we should count them as files.
        # However, we can't used the followlinks parameter, because a) we don't want
        # to descend into directories and b) we could end up in an infinite loop if
        # we were to pass that flag. Instead, we handle symlinks here:
        for subpath in os.listdir(root):
            full_subpath = os.path.join(root, subpath)
            if os.path.islink(full_subpath) and os.path.isdir(full_subpath):
                files.append(full_subpath)
    return (directories, files)


################################################################################
# Functions to read files to compute hashes, write results to stdout, etc.
################################################################################


def getmtime(path):
    """
    Like os.path.getmtime, but does not follow symlinks.
    """
    return os.lstat(path).st_mtime


def get_size(path, dirs_and_files=None):
    """
    Get the size (in bytes) of the file or directory at or under the given path.
    Does not include symlinked files and directories.
    """
    if os.path.islink(path) or not os.path.isdir(path):
        return os.lstat(path).st_size
    dirs_and_files = dirs_and_files or recursive_ls(path)
    return sum(os.lstat(path).st_size for path in itertools.chain(*dirs_and_files))


def hash_directory(path, dirs_and_files=None):
    """
    Return the hash of the contents of the folder at the given path.
    This hash is independent of the path itself - if you were to move the
    directory and call get_hash again, you would get the same result.
    """
    (directories, files) = dirs_and_files or recursive_ls(path)
    # Sort and then hash all directories and then compute a hash of the hashes.
    # This two-level hash is necessary so that the overall hash is unambiguous -
    # if we updated directory_hash with the directory names themselves, then
    # we'd be hashing the concatenation of these names, which could be generated
    # in multiple ways.
    directory_hash = hashlib.sha1()
    for directory in sorted(directories):
        relative_path = get_relative_path(path, directory)
        directory_hash.update(hashlib.sha1(relative_path).hexdigest())
    # Use a similar two-level hashing scheme for all files, but incorporate a
    # hash of both the file name and contents.
    file_hash = hashlib.sha1()
    for file_name in sorted(files):
        relative_path = get_relative_path(path, file_name)
        file_hash.update(hashlib.sha1(relative_path).hexdigest())
        file_hash.update(hash_file_contents(file_name))
    # Return a hash of the two hashes.
    overall_hash = hashlib.sha1(directory_hash.hexdigest())
    overall_hash.update(file_hash.hexdigest())
    return overall_hash.hexdigest()


def hash_file_contents(path):
    """
    Return the hash of the file's contents, read in blocks of size BLOCK_SIZE.
    """
    message = 'hash_file called with relative path: %s' % (path,)
    precondition(os.path.isabs(path), message)
    if os.path.islink(path):
        contents_hash = hashlib.sha1(LINK_PREFIX)
        contents_hash.update(os.readlink(path))
    else:
        contents_hash = hashlib.sha1(FILE_PREFIX)
        with open(path, 'rb') as file_handle:
            while True:
                data = file_handle.read(BLOCK_SIZE)
                if not data:
                    break
                contents_hash.update(data)
    return contents_hash.hexdigest()


################################################################################
# Functions that modify that filesystem in controlled ways.
################################################################################

def copy(source_path, dest_path, follow_symlinks=False, exclude_patterns=None):
    """
    Copy |source_path| to |dest_path|.
    Assume dest_path doesn't exist.
    |follow_symlinks|: whether to follow symlinks
    |exclude_patterns|: patterns to not copy
    Note: this only works in Linux.
    """
    if os.path.exists(dest_path):
        raise path_error('already exists', dest_path)

    if source_path == '/dev/stdin':
        with open(dest_path, 'wb') as dest:
            file_util.copy(sys.stdin, dest, autoflush=False, print_status='Copying %s to %s' % (source_path, dest_path))
    else:
        if not follow_symlinks and os.path.islink(source_path):
            raise path_error('not following symlinks', source_path)
        if not os.path.exists(source_path):
            raise path_error('does not exist', source_path)
        command = [
            'rsync',
            '-pr%s' % ('L' if follow_symlinks else 'l'),
            source_path + ('/' if not os.path.islink(source_path) and os.path.isdir(source_path) else ''),
            dest_path,
        ]
        if exclude_patterns is not None:
            for pattern in exclude_patterns:
                command.extend(['--exclude', pattern])
        if subprocess.call(command) != 0:
            raise path_error('Unable to copy %s to' % source_path, dest_path)


def make_directory(path):
    """
    Create the directory at the given path.
    """
    try:
        os.mkdir(path)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise
    check_isdir(path, 'make_directories')


def set_write_permissions(path):
    # Recursively give give write permissions to |path|, so that we can operate
    # on it.
    if not os.path.islink(path):  # Don't need write permissions if symlink
        subprocess.call(['chmod', '-R', 'u+w', path])


def rename(old_path, new_path):
    # Allow write permissions, or else the move will fail.
    set_write_permissions(old_path)
    subprocess.call(['mv', old_path, new_path])


def remove(path):
    """
    Remove the given path, whether it is a directory, file, or link.
    """
    check_isvalid(path, 'remove')
    set_write_permissions(path)  # Allow permissions
    if os.path.islink(path):
        os.unlink(path)
    elif os.path.isdir(path):
        try:
            shutil.rmtree(path)
        except shutil.Error:
            pass
    else:
        os.remove(path)
    if os.path.exists(path):
        print 'Failed to remove %s' % path

def soft_link(source, path):
    """
    Create a symbolic link to source at path. This is basically the same as doing "ln -s $source $path"
    """
    check_isvalid(source, 'soft_link')
    os.symlink(source, path)
