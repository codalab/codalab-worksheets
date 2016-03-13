import os


def is_valid_bundle_path(bundle_path, path):
    """
    Checks to ensure that the given path is inside the bundle, after resolving
    any links.
    """
    bundle_path = os.path.realpath(bundle_path)
    final_path = os.path.realpath(get_target_path(bundle_path, path))
    return final_path.startswith(bundle_path)


def get_invalid_bundle_path_error_string(path):
    return 'Path %s is not inside the bundle.' % path


def get_target_path(bundle_path, path):
    """
    Returns the path to the given target.
    """
    if path:
        return os.path.join(bundle_path, path)
    else:
        return bundle_path
