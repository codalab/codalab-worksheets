import math
import os
import stat
import logging
import traceback
from typing import Any, Iterable, Generator, Optional, Union, cast, Dict
from typing_extensions import TypedDict

from apache_beam.io.filesystems import FileSystems
from codalab.common import parse_linked_bundle_url
from codalab.worker.file_util import OpenIndexedArchiveFile
from ratarmountcore import FileInfo


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


TargetInfo = TypedDict(
    'TargetInfo',
    {
        "name": str,
        "size": int,
        "perm": int,
        "link": Optional[str],
        "type": str,
        "contents": Optional[Iterable[Any]],
        "resolved_target": Optional[BundleTarget],
    },
    total=False,
)


def get_target_info(bundle_path: str, target: BundleTarget, depth: int) -> TargetInfo:
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
        # If the target is on Blob Storage, use a Blob-specific method
        # to get the target info.
        try:
            info = _compute_target_info_blob(final_path, depth)
        except Exception:
            logging.error(
                "Path '{}' in bundle {} not found: {}".format(
                    target.subpath, target.bundle_uuid, traceback.format_exc()
                )
            )
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


def get_target_path(bundle_path: str, target: BundleTarget) -> str:
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


def _get_normalized_target_path(bundle_path: str, target: BundleTarget) -> str:
    if parse_linked_bundle_url(bundle_path).uses_beam:
        # On Azure, don't call os.path functions on the paths (which are azfs:// URLs).
        # We can just concatenate them together.
        return f"{bundle_path}/{target.subpath}" if target.subpath else bundle_path
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


def _compute_target_info_local(path: str, depth: Union[int, float]) -> TargetInfo:
    """Computes target info for a local file."""
    stat = os.lstat(path)
    result: TargetInfo = {
        'name': os.path.basename(path),
        'size': stat.st_size,
        'perm': stat.st_mode & 0o777,
        'type': '',
    }
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


def _compute_target_info_blob(
    path: str, depth: Union[int, float], return_generators=False
) -> TargetInfo:
    """Computes target info for a file that is externalized on Blob Storage, meaning
    that it's contained within an indexed archive file.

    Args:
        path (str): The path that refers to the specified target.
        depth (Union[int, float]): Depth until which directory contents are resolved.
        return_generators (bool, optional): If set to True, the 'contents' key of directories is equal to a generator instead of a list. Defaults to False.

    Raises:
        PathException: Path not found or invalid.

    Returns:
        TargetInfo: Target info of specified path.
    """

    linked_bundle_path = parse_linked_bundle_url(path)
    if not FileSystems.exists(linked_bundle_path.bundle_path):
        raise PathException(linked_bundle_path.bundle_path)
    if not linked_bundle_path.is_archive:
        # Single file
        raise PathException(
            "Single files on Blob Storage are not supported; only a path within an archive file is supported."
        )

    # process_contents is used to process the value of the 'contents' key (which is a generator) before it is returned.
    # If return_generators is False, it resolves the given generator into a list; otherwise, it just returns
    # the generator unchanged.
    process_contents = list if return_generators is False else lambda x: x

    with OpenIndexedArchiveFile(linked_bundle_path.bundle_path) as tf:
        islink = lambda finfo: stat.S_ISLNK(finfo.mode)
        readlink = lambda finfo: finfo.linkname
        isfile = lambda finfo: not stat.S_ISDIR(finfo.mode)
        isdir = lambda finfo: stat.S_ISDIR(finfo.mode)
        listdir = lambda path: cast(Dict[str, FileInfo], tf.listDir(path) or {})

        def _get_info(path: str, depth: Union[int, float]) -> TargetInfo:
            """This function is called to get the target info of the specified path.
            If the specified path is a directory and additional depth is requested, this
            function is recursively called to retrieve the target info of files within
            the directory, much like _compute_target_info_local.
            """
            if not path.startswith("/"):
                path = "/" + path
            finfo = cast(FileInfo, tf.getFileInfo(path))
            if finfo is None:
                # Not found
                raise PathException("File not found.")
            result: TargetInfo = {
                'name': os.path.basename(path),  # get last part of path
                'size': finfo.size,
                'perm': finfo.mode & 0o777,
                'type': '',
            }
            if islink(finfo):
                result['type'] = 'link'
                result['link'] = readlink(finfo)
            elif isfile(finfo):
                result['type'] = 'file'
            elif isdir(finfo):
                result['type'] = 'directory'
                if depth > 0:
                    result['contents'] = process_contents(
                        _get_info(path + "/" + file_name, depth - 1)
                        for file_name in listdir(path)
                        if file_name != "."
                    )
            return result

        if not linked_bundle_path.is_archive_dir:
            # Return the contents of the single .gz file.
            # The entry returned by ratarmount for a single .gz file is not technically part of a tar archive
            # and has a name hardcoded as "contents," so we modify the type, name, and permissions of
            # the output accordingly.
            return cast(
                TargetInfo,
                dict(
                    _get_info("/contents", depth),
                    type="file",
                    name=linked_bundle_path.bundle_uuid,
                    perm=0o755,
                ),
            )
        if linked_bundle_path.archive_subpath:
            # Return the contents of a subpath within a directory.
            return _get_info(linked_bundle_path.archive_subpath, depth)
        else:
            # No subpath, return the entire directory with the bundle
            # contents in it. The permissions of this directory
            # cannot be set by the user (the user can only set permissions
            # of files *within* this directory that are part of the bundle
            # itself), so we just return a placeholder value of 0o755
            # for this directory's permissions.
            file = FileSystems.match([path])[0].metadata_list[0]
            result: TargetInfo = {
                'name': linked_bundle_path.bundle_uuid,
                'type': 'directory',
                'size': file.size_in_bytes,
                'perm': 0o755,
            }
            if depth > 0:
                result['contents'] = process_contents(
                    _get_info(file_name, depth - 1)
                    for file_name in listdir("/")
                    if file_name != "."
                )
            return result


def compute_target_info_blob_descendants_flat(path: str) -> Generator[TargetInfo, None, None]:
    """Given a path on Blob Storage,
    returns a generator that generates a flat list of all descendants within that directory
    in the format [{name, type, size, perm}], where `name` is equal to the full path of each item.

    Also includes an entry for the specified directory with `name` equal to an empty string.

    This function is used by TarSubdirStream in order to determine the list of descendants
    that exist inside a given subdirectory in an archive file on Blob Storage.
    """
    target_info = _compute_target_info_blob(
        path=path, depth=math.inf, return_generators=True
    )  # We want to return a generator so that we can expand *all* descendants without adding additional overhead.

    def get_results(tinfo: TargetInfo, prefix="") -> Generator[TargetInfo, None, None]:
        yield cast(TargetInfo, dict(tinfo, contents=None, name=prefix + tinfo["name"]))
        for t in tinfo.get('contents') or []:
            yield from get_results(t, prefix + tinfo['name'] + '/')

    yield cast(TargetInfo, dict(target_info, contents=None, name=""))
    for t in target_info.get('contents') or []:
        print("CONTENTT", t)
        yield from get_results(t)
