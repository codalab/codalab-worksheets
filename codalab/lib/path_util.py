'''
path_util contains helpers for working with local filesystem paths.
There are a few classes of methods provided here:

  Functions to normalize paths and check that they are in normal form:
    normalize, check_isvalid, check_isdir, check_isfile, check_for_symlinks

  Functions to list directories and to deal with subpaths of paths:
    safe_join, get_relative_path, ls, recursive_ls

  Functions to read files to compute hashes, write results to stdout, etc:
    cat, getmtime, get_size, hash_directory, hash_file_contents

  Functions that modify that filesystem in controlled ways:
    copy, make_directory, remove, remove_symlinks, set_permissions
'''
import contextlib
import errno
import hashlib
import itertools
import os
import shutil
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


class TargetPath(unicode):
    '''
    Wrapper around unicode objects that allows us to add extra attributes to them.
    In particular, canonicalize.get_target_path will return a TargetPath with the
    'target' attribute set to the un-canonicalized target.
    '''


def path_error(message, path):
    '''
    Raised when a user-supplied path causes an exception. If the path passed to
    this error's constructor came from a call to get_target_path, the target will
    be appended to the message instead of the computed path.
    '''
    if isinstance(path, TargetPath):
        path = safe_join(*path.target)
    return UsageError(' '.join((message, path)))


@contextlib.contextmanager
def chdir(new_dir):
    '''
    Context manager that changes the current working directory of this process
    for the duration of the context.
    '''
    cur_dir = os.getcwd()
    try:
        os.chdir(new_dir)
        yield
    finally:
        os.chdir(cur_dir)


################################################################################
# Functions to normalize paths and check that they are in normal form.
################################################################################


def normalize(path):
    '''
    Return the absolute path of the location specified by the given path.
    This path is returned in a "canonical form", without ~'s, .'s, ..'s.
    '''
    return os.path.abspath(os.path.expanduser(path))


def check_isvalid(path, fn_name):
    '''
    Raise a PreconditionViolation if the path is not absolute or normalized.
    Raise a UsageError if the file at that path does not exist.
    '''
    precondition(os.path.isabs(path), '%s got relative path: %s' % (fn_name, path))
    # Broken symbolic links are valid paths, so we use lexists instead of exists.
    # This case will come up when executing a make bundle with an anonymous target,
    # because the symlink will be broken until it is moved into the bundle store.
    if not os.path.lexists(path):
        raise path_error('%s got non-existent path:' % (fn_name,), path)


def check_isdir(path, fn_name):
    '''
    Check that the path is valid, then raise UsageError if the path is a file.
    '''
    check_isvalid(path, fn_name)
    if not os.path.isdir(path):
        raise path_error('%s got non-directory:' % (fn_name,), path)


def check_isfile(path, fn_name):
    '''
    Check that the path is valid, then raise UsageError if the path is a file.
    '''
    check_isvalid(path, fn_name)
    if os.path.isdir(path):
        raise path_error('%s got directory:' % (fn_name,), path)


def check_for_symlinks(root, dirs_and_files=None):
    '''
    Raise UsageError if there are any symlinks under the given path.
    '''
    (directories, files) = dirs_and_files or recursive_ls(root)
    for path in itertools.chain(directories, files):
        if os.path.islink(path):
            relative_path = get_relative_path(root, path)
            raise path_error('Found symlink %s under path:' % (relative_path,), root)


################################################################################
# Functions to list directories and to deal with subpaths of paths.
################################################################################


def safe_join(*paths):
    '''
    Join a sequence of paths but filter out any that are empty. Used for targets.
    Note that os.path.join has this functionality EXCEPT at the end of the list,
    which causes problems when a target subpath is empty.
    '''
    return os.path.join(*filter(None, paths))


def get_relative_path(root, path):
    '''
    Return the relative path from root to path, which should be nested under root.
    '''
    precondition(path.startswith(root), '%s is not under %s' % (path, root))
    return path[len(root):]


def ls(path):
    '''
    Return a (list of directories, list of files) in the given directory.
    '''
    check_isdir(path, 'ls')
    (directories, files) = ([], [])
    for file_name in os.listdir(path):
        if os.path.isfile(os.path.join(path, file_name)):
            files.append(file_name)
        else:
            directories.append(file_name)
    return (directories, files)


def recursive_ls(path):
    '''
    Return a (list of directories, list of files) in the given directory and
    all of its nested subdirectories. All paths returned are absolute.

    Symlinks are returned in the list of files, even if they point to directories.
    This makes it possible to distinguish between real and symlinked directories
    when computing the hash of a directory. This function will NOT descend into
    symlinked directories.
    '''
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


def cat(path):
    '''
    Copy data from the file at the given path to stdout.
    '''
    check_isfile(path, 'cat')
    with open(path, 'rb') as file_handle:
        file_util.copy(file_handle, sys.stdout)

def readFile(path, lines=0):
    '''
    Return contents of file as string.
    '''
    check_isfile(path, 'readFile')
    with open(path, 'rb') as file_handle:
        if lines == 0:
            return file_handle.readlines()
        else:
            return list(itertools.islice(file_handle, lines))

def getmtime(path):
    '''
    Like os.path.getmtime, but does not follow symlinks.
    '''
    return os.lstat(path).st_mtime


def get_size(path, dirs_and_files=None):
    '''
    Get the size (in bytes) of the file or directory at or under the given path.
    Does not include symlinked files and directories.
    '''
    if os.path.islink(path) or not os.path.isdir(path):
        return long(os.lstat(path).st_size)
    dirs_and_files = dirs_and_files or recursive_ls(path)
    return sum(long(os.lstat(path).st_size) for path in itertools.chain(*dirs_and_files))


def hash_directory(path, dirs_and_files=None):
    '''
    Return the hash of the contents of the folder at the given path.
    This hash is independent of the path itself - if you were to move the
    directory and call get_hash again, you would get the same result.
    '''
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
    '''
    Return the hash of the file's contents, read in blocks of size BLOCK_SIZE.
    '''
    message = 'hash_file called with relative path: %s' % (path,)
    precondition(os.path.isabs(path), message)
    contents_hash = hashlib.sha1()
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


def copy(source_path, dest_path):
    if os.path.islink(source_path):
        link_target = os.readlink(source_path)
        os.symlink(link_target, dest_path)
    elif os.path.isdir(source_path):
        shutil.copytree(source_path, dest_path, symlinks=True)
    else:
        shutil.copyfile(source_path, dest_path)


def make_directory(path):
    '''
    Create the directory at the given path.
    '''
    try:
        os.mkdir(path)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise
    check_isdir(path, 'make_directories')


def remove(path):
    '''
    Removethe given path, whether it is a directory, file, or link.
    '''
    check_isvalid(path, 'remove')
    if os.path.islink(path):
        os.unlink(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)
    else:
        os.remove(path)


def remove_symlinks(root, dirs_and_files=None):
    '''
    Delete any existing symlinks under the given path.
    '''
    (directories, files) = dirs_and_files or recursive_ls(root)
    for path in itertools.chain(directories, files):
        if os.path.islink(path):
            os.unlink(path)


def set_permissions(path, permissions, dirs_and_files=None):
    '''
    Sets the permissions bits for all directories and files under the path.
    '''
    (directories, files) = dirs_and_files or recursive_ls(path)
    for subpath in itertools.chain(directories, files):
        try:
            os.chmod(subpath, permissions)
        except OSError, e:
            # chmod-ing a broken symlink will raise ENOENT, so we pass on this case.
            if not (e.errno == errno.ENOENT and os.path.islink(subpath)):
                raise
