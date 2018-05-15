import os


class PathException(Exception):
    pass


def get_target_info(bundle_path, uuid, path, depth):
    """
    Generates an index of the contents of the given path. The index contains
    the fields:
        name: Name of the entry.
        type: Type of the entry, one of 'file', 'directory' or 'link'.
        size: Size of the entry.
        perm: Permissions of the entry.
        link: If type is 'link', where the symbolic link points to.
        contents: If type is 'directory', a list of entries for the contents.

    Any entries more than depth levels deep are filtered out. Depth 0, for
    example, means only the top-level entry is included, and no contents. Depth
    1 means the contents of the top-level are included, but nothing deeper.

    If the given path does not exist, returns None.

    If reading the given path is not secure, raises a PathException.
    """
    final_path = _get_normalized_target_path(bundle_path, uuid, path)

    if not os.path.islink(final_path) and not os.path.exists(final_path):
        return None

    return _compute_target_info(final_path, depth)


def get_target_path(bundle_path, uuid, path):
    """
    Returns the path to the given target, which is assumed to exist.
    
    If reading the given path is not secure, raises a PathException.
    """
    final_path = _get_normalized_target_path(bundle_path, uuid, path)
    error_path = _get_target_path(uuid, path)

    if os.path.islink(final_path):
        # We shouldn't get here, unless the user is a hacker or a developer
        # didn't use get_target_info correctly.
        raise PathException('%s is a symlink and following symlinks is not allowed.' % error_path)

    return final_path


BUNDLE_NO_LONGER_RUNNING_MESSAGE = 'Bundle no longer running'


def _get_normalized_target_path(bundle_path, uuid, path):
    real_bundle_path = os.path.realpath(bundle_path)
    normalized_target_path = os.path.normpath(_get_target_path(real_bundle_path, path))
    error_path = _get_target_path(uuid, path)

    if not normalized_target_path.startswith(real_bundle_path):
        raise PathException('%s is not inside the bundle.' % error_path)

    return normalized_target_path


def _get_target_path(bundle_path, path):
    if path:
        # Don't use os.path.join, since we don't want an absolute path to
        # override the bundle path.
        return bundle_path + os.path.sep + path
    else:
        return bundle_path


def _compute_target_info(path, depth):
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
                _compute_target_info(os.path.join(path, file_name), depth - 1)
                for file_name in os.listdir(path)]
    return result
