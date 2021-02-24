import os
import stat
import tarfile
import logging

from apache_beam.io.filesystems import FileSystems
from codalab.common import parse_linked_bundle_url
from codalab.worker.file_util import open_indexed_tar_gz_file


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
        # If the target is on Azure, use a special method using Apache Beam
        # to get the target info.
        try:
            info = _compute_target_info_beam(final_path, depth)
        except Exception:
            import traceback

            logging.info("error!")
            logging.info("error: %s", traceback.format_exc())
            raise PathException(
                "Path '{}' in bundle {} not found".format(target.subpath, target.bundle_uuid)
            )
    else:
        if not os.path.islink(final_path) and not os.path.exists(final_path):
            raise PathException(
                "Path '{}' in bundle {} not found".format(target.subpath, target.bundle_uuid)
            )
        info = _compute_target_info_local(final_path, depth)

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
    if parse_linked_bundle_url(bundle_path).uses_beam:
        # On Azure, don't call os.path functions on the paths (which are azfs:// URLs).
        real_bundle_path = bundle_path
        normalized_target_path = _get_target_path(real_bundle_path, target.subpath)
    else:
        real_bundle_path = os.path.realpath(bundle_path)
        normalized_target_path = os.path.normpath(
            _get_target_path(real_bundle_path, target.subpath)
        )

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


def _compute_target_info_local(path, depth):
    """Computes target info for a local file."""
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
                _compute_target_info_local(os.path.join(path, file_name), depth - 1)
                for file_name in os.listdir(path)
            ]
    if result is None:
        raise PathException()
    return result


def _compute_target_info_beam(path, depth):
    """Computes target info for a file that is externalized on a location
    such as Azure, by using the Apache Beam FileSystem APIs."""

    linked_bundle_path = parse_linked_bundle_url(path)
    if not FileSystems.exists(linked_bundle_path.bundle_path):
        raise PathException
    if not linked_bundle_path.is_archive:
        # Single file
        file = FileSystems.match([path])[0].metadata_list[0]
        return {
            'name': linked_bundle_path.bundle_uuid,
            'type': 'file',
            'size': file.size_in_bytes,
            'perm': 0o777,
        }

    tf, _ = open_indexed_tar_gz_file(linked_bundle_path.bundle_path)
    islink = lambda finfo: stat.S_ISLNK(finfo.mode)
    readlink = lambda finfo: finfo.linkname

    isfile = lambda finfo: finfo.type in tarfile.REGULAR_TYPES
    isdir = lambda finfo: finfo.type == tarfile.DIRTYPE
    listdir = lambda path: tf.getFileInfo(path, listDir=True)

    def _get_info(path, depth):
        if not path.startswith("/"):
            path = "/" + path
        finfo = tf.getFileInfo(path)
        if finfo is None:
            # Not found
            raise PathException
        result = {}
        result['name'] = path.split("/")[-1]  # get last part of path
        result['size'] = finfo.size
        result['perm'] = finfo.mode & 0o777
        if islink(finfo):
            result['type'] = 'link'
            result['link'] = readlink(finfo)
        elif isfile(finfo):
            result['type'] = 'file'
        elif isdir(finfo):
            result['type'] = 'directory'
            if depth > 0:
                result['contents'] = [
                    _get_info(path + "/" + file_name, depth - 1)
                    for file_name in listdir(path)
                    if file_name != "."
                ]
        return result

    if linked_bundle_path.archive_subpath:
        # Return the contents of a subpath within a directory.
        return _get_info(linked_bundle_path.archive_subpath, depth)
    else:
        # No subpath, return the entire directory.
        file = FileSystems.match([path])[0].metadata_list[0]
        result = {
            'name': linked_bundle_path.bundle_uuid,
            'type': 'directory',
            'size': file.size_in_bytes,
            'perm': 0o777,
        }
        if depth > 0:
            result['contents'] = [
                _get_info(file_name, depth - 1) for file_name in listdir("/") if file_name != "."
            ]
        return result
