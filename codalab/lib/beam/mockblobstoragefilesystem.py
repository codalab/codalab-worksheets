"""Mock Azure Blob Storage filesystem that is backed by a local filesystem instead.
This means that while URLs will still start with azfs://, adding / removing files
will actually change files in the local /tmp/codalab/azfs-mock/ directory instead.

This is used only for unit tests for speed, so that unit tests do not need to depend on
a Blob Storage container / Azurite running in the background.
"""

from apache_beam.io.filesystem import FileSystem
from apache_beam.io.localfilesystem import LocalFileSystem
import os
from io import BytesIO

__all__ = ['MockBlobStorageFileSystem']


class MockBlobStorageFileSystem(LocalFileSystem):
  AZFS_MOCK_LOCATION = "/tmp/codalab/azfs-mock/"

  def __init__(self, *args, **kwargs):
    os.makedirs(MockBlobStorageFileSystem.AZFS_MOCK_LOCATION, exist_ok=True)
    super().__init__(*args, **kwargs)

  @classmethod
  def scheme(cls):
    """URI scheme for the FileSystem
    """
    return 'azfs'

  def _local_to_azfs(self, path):
      return "azfs://" + path[len(MockBlobStorageFileSystem.AZFS_MOCK_LOCATION):]

  def _azfs_to_local(self, path):
      if not path.startswith("azfs://"):
        return path
      path = MockBlobStorageFileSystem.AZFS_MOCK_LOCATION + path[len("azfs://"):]
      os.makedirs(os.path.dirname(path), exist_ok=True)
      return path

  def join(self, basepath, *paths):
    return super().join(basepath, *paths)

  def split(self, path):
    return super().split(path)

  def mkdirs(self, path):
    return super().mkdirs(self._azfs_to_local(path))

  def has_dirs(self):
    return super().has_dirs()

  def _list(self, dir_or_prefix):
    for file_metadata in super()._list(self._azfs_to_local(dir_or_prefix)):
      file_metadata.path = self._local_to_azfs(file_metadata.path)
      yield file_metadata
  
  def create(
      self,
      path,
      *args, **kwargs):
    return super().create(self._azfs_to_local(path), *args, **kwargs)
  
  def open(
      self,
      path,
      *args, **kwargs):
    return BytesIO(super().open(self._azfs_to_local(path), *args, **kwargs).read())

  def copy(self, source_file_names, destination_file_names):
    return super().copy([self._azfs_to_local(p) for p in source_file_names], [self._azfs_to_local(p) for p in destination_file_names])

  def rename(self, source_file_names, destination_file_names):
    print([self._azfs_to_local(p) for p in source_file_names], [self._azfs_to_local(p) for p in destination_file_names])
    return super().rename([self._azfs_to_local(p) for p in source_file_names], [self._azfs_to_local(p) for p in destination_file_names])
  
  def exists(self, path):
    return super().exists(self._azfs_to_local(path))

  def size(self, path):
    return super().size(self._azfs_to_local(path))

  def last_updated(self, path):
    return super().last_updated(self._azfs_to_local(path))

  def checksum(self, path):
    return super().checksum(self._azfs_to_local(path))

  def delete(self, paths):
    return super().delete([self._azfs_to_local(path) for path in paths])
