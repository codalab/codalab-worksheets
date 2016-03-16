import os

def get_and_check_target_path(bundle_path, uuid, path):
    """
    Security checks to ensure that the contents at the given path in the bundle
    can be downloaded.

    Returns a tuple (target_path, error_message). If the contents are not
    allowed to be downloaded, error_message is not None and contains the message
    to display to the user. Otherwise, it's None.
    """
    target_path = get_target_path(bundle_path, path)
    error_path = get_target_path(uuid, path)

    # This handles the case of paths such as ..
    if not os.path.realpath(target_path).startswith(os.path.realpath(bundle_path)):
        return None, '%s is not inside the bundle.' % error_path

    if os.path.islink(target_path):
        # We shouldn't get here, unless the user is a hacker or a developer
        # didn't use get_target_info correctly.
        return None, '%s is a symlink and following symlinks is not allowed.' % error_path

    return target_path, None


def get_target_path(bundle_path, path):
    """
    Returns the path to the given target.
    """
    if path:
        # Don't use os.path.join, since we don't want an absolute path to
        # override the bundle path.
        return bundle_path + os.path.sep + path
    else:
        return bundle_path
