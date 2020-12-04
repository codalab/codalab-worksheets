import os
from apache_beam.io.filesystems import FileSystems
from zipfile import ZipFile
from codalab.lib.path_util import parse_linked_bundle_url


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
    if parse_linked_bundle_url(final_path).uses_beam:
        info = _compute_target_info_beam(final_path, depth)
    else:
        if not os.path.islink(final_path) and not os.path.exists(final_path):
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
    real_bundle_path = (
        bundle_path
        if parse_linked_bundle_url(bundle_path).uses_beam
        else os.path.realpath(bundle_path)
    )
    normalized_target_path = _get_target_path(real_bundle_path, target.subpath)
    if not parse_linked_bundle_url(normalized_target_path).uses_beam:
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
    if parse_linked_bundle_url(path).uses_beam:
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
    """Computes target info for a file that is externalized on a location
    such as Azure, by using the Apache Beam FileSystem APIs."""
    # TODO (Ashwin): properly return permissions.
    linked_bundle_path = parse_linked_bundle_url(path)
    if not linked_bundle_path.is_zip:
        # Single file
        file = FileSystems.match([path])[0].metadata_list[0]
        return {
            'name': linked_bundle_path.bundle_uuid,
            'type': 'file',
            'size': file.size_in_bytes,
            'perm': 0o777,
        }
    elif not linked_bundle_path.zip_subpath:
        # We want the entire zip file, not a subpath within it.
        with ZipFile(FileSystems.open(linked_bundle_path.bundle_path)) as f:
            base = {
                'name': linked_bundle_path.bundle_uuid,
                'type': 'directory',
                'size': sum([zipinfo.file_size for zipinfo in f.infolist()]),
                'perm': 0o777,
            }
    else:
        try:
            with ZipFile(FileSystems.open(linked_bundle_path.bundle_path)) as f:
                zipinfo = f.getinfo(linked_bundle_path.zip_subpath)
            is_dir = zipinfo.is_dir()
            filename = zipinfo.filename
            file_size = zipinfo.file_size
        except KeyError:
            # Assume we're in a directory.
            is_dir = True
            filename = linked_bundle_path.zip_subpath
            file_size = 0
        if not is_dir:
            return {
                'name': filename,
                'type': 'file',
                'size': file_size,
                'perm': 0o777,
            }
        base = {
            'name': filename,
            'type': 'directory',
            'size': file_size,
            'perm': 0o777,
        }

    def get_last_part(path):
        parts = path.split("/")
        return parts[-1]

    dirs = [
        zipinfo.filename
        for zipinfo in f.infolist()
        if zipinfo.is_dir() and not zipinfo.filename.startswith(linked_bundle_path.zip_subpath)
    ]
    if depth > 0:
        base['contents'] = [
            (
                {
                    'name': get_last_part(zipinfo.filename),
                    'type': 'directory' if zipinfo.is_dir() else 'file',
                    'size': zipinfo.file_size,
                    'perm': 0o777,
                }
                if not zipinfo.is_dir()
                else _compute_target_info_beam(
                    f"{linked_bundle_path.bundle_path}/{zipinfo.filename}", depth - 1,
                )
            )
            for zipinfo in f.infolist()
            if (
                not linked_bundle_path.zip_subpath
                or zipinfo.filename.startswith(linked_bundle_path.zip_subpath)
            )
            and not (any(zipinfo.filename.startswith(i) for i in dirs) and not zipinfo.is_dir())
        ]
    return base
