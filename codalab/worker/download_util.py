import os
from codalab.lib.beam.filesystems import FileSystems
from codalab.worker.bundle_state import LinkFormat
from zipfile import ZipFile
from codalab.lib.path_util import parse_azure_url
from pydash.objects import set_
class PathException(Exception):
    pass


class BundleTarget:
    """
        bundle_uuid: UUID of the bundle the path is actually found on. This is
            used for when a path resolves to a dependency of a bundle.
        subpath: the particular path to resolve in the bundle UUID in the end. If
            the path resolves to a dependency, then the first component of the
            path is the dependency key and should not be used within the actual
            dependency bundle. This field strips that value.

        for example if a bundle has the dependency key:dep-bundle/dep-subpath
        the bundle target (bundle, key/subpath) would resolve to
        (dep-bundle, dep-subpath/subpath)
    """

    def __init__(self, bundle_uuid, subpath):
        self.bundle_uuid = bundle_uuid
        self.subpath = subpath

    def __eq__(self, other):
        return self.bundle_uuid == other.bundle_uuid and self.subpath == other.subpath

    def __hash__(self):
        return hash((self.bundle_uuid, self.subpath))

    @classmethod
    def from_dict(cls, dct):
        return cls(dct['bundle_uuid'], dct['subpath'])

    def __str__(self):
        return "{}:{}".format(self.bundle_uuid, self.subpath)


def get_target_info(bundle_path, target, depth):
    """
    Generates an index of the contents of the given path. The index contains
    the fields:
        name: Name of the entry.
        type: Type of the entry, one of 'file', 'directory' or 'link'.
        size: Size of the entry.
        perm: Permissions of the entry.
        link: If type is 'link', where the symbolic link points to.
        contents: If type is 'directory', a list of entries for the contents.

    For the top level entry, also contains resolved_target, a BundleTarget:

    Any entries more than depth levels deep are filtered out. Depth 0, for
    example, means only the top-level entry is included, and no contents. Depth
    1 means the contents of the top-level are included, but nothing deeper.

    If the given path does not exist, raises PathException.

    If reading the given path is not secure, raises a PathException.
    """
    final_path = _get_normalized_target_path(bundle_path, target)

    if not os.path.islink(final_path) and not FileSystems.exists(final_path):
        raise PathException(
            'Path {} in bundle {} not found'.format(target.bundle_uuid, target.subpath)
        )

    info = _compute_target_info(final_path, depth)
    info['resolved_target'] = target

    return info


def get_target_path(bundle_path, target):
    """
    Returns the path to the given target, which is assumed to exist.
    If reading the given path is not secure, raises a PathException.
    """
    final_path = _get_normalized_target_path(bundle_path, target)
    error_path = _get_target_path(target.bundle_uuid, target.subpath)

    if os.path.islink(final_path):
        # We shouldn't get here, unless the user is a hacker or a developer
        # didn't use get_target_info correctly.
        raise PathException('%s is a symlink and following symlinks is not allowed.' % error_path)

    return final_path


BUNDLE_NO_LONGER_RUNNING_MESSAGE = 'Bundle no longer running'


def _get_normalized_target_path(bundle_path, target):
    real_bundle_path = bundle_path if bundle_path.startswith("azfs://") else os.path.realpath(bundle_path)
    normalized_target_path = _get_target_path(real_bundle_path, target.subpath)
    if not normalized_target_path.startswith("azfs://"):
        normalized_target_path = os.path.normpath(normalized_target_path)
    error_path = _get_target_path(target.bundle_uuid, target.subpath)

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
    if path.startswith("azfs://"):
        return _compute_target_info_beam(path, depth)
    result = {}
    result['name'] = os.path.basename(path)
    stat = os.lstat(path)
    result['size'] = stat.st_size
    result['perm'] = stat.st_mode & 0o777
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
                for file_name in os.listdir(path)
            ]
    if result is None:
        raise PathException()
    return result

def _compute_target_info_beam(path, depth):
    bundle_uuid, zip_path, zip_subpath = parse_azure_url(path)
    if zip_path is None:
        # Single file
        file = FileSystems.match([path])[0].metadata_list[0]
        return { 'name': os.path.basename(file.path), 'type': 'file', 'size': file.size_in_bytes, 'perm': 0o777 }
    # dir_structure = {}
    with ZipFile(FileSystems.open(zip_path)) as f:
        zipinfo = f.getinfo(zip_subpath)
    if not zipinfo.is_dir():
        return {
            'name': zipinfo.filename,
            'type': 'file',
            'size': zipinfo.file_size,
            'perm': 0o777
        }
    return {
        'name': zipinfo.filename,
        'type': 'directory',
        'size': zipinfo.file_size,
        'perm': 0o777,
        # TODO: support previewing nested directories.
        'contents': [{
            'name': zipinfo.filename,
            'type': 'file',
            'size': zipinfo.file_size,
            'perm': 0o777
        } for zipinfo in f.infolist() if zipinfo.filename.startswith(zip_subpath)]
    }