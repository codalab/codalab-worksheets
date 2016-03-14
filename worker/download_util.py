import httplib
import os


def get_and_check_real_target_path(bundle_path, uuid, path):
    """
    Checks that the path is not a symlink pointing outside the bundle and not
    a broken symlink.

    Returns 3 values: (target_path, error_code, error_string)
    On success error_code is None and target_path is the real path to the
    target, after resolving symlinks. On error, error_code is an HTTP error code
    and error_string is the message string to display to the user.
    """
    target_path = get_target_path(bundle_path, path)
    error_path = get_target_path(uuid, path)

    if not os.path.realpath(target_path).startswith(os.path.realpath(bundle_path)):
        return None, httplib.FORBIDDEN, '%s is not inside the bundle.' % error_path

    if os.path.islink(target_path) and not os.path.exists(target_path):
        return None, httplib.NOT_FOUND, 'Symlink at %s is broken' % error_path

    return os.path.realpath(target_path), None, None


def get_target_path(bundle_path, path):
    if path:
        return os.path.join(bundle_path, path)
    else:
        return bundle_path
