"""Modifies Apache Beam so that we use MockBlobStorageFileSystem
instead of BlobStorageFileSystem for azfs:// URLs.

This allows unit tests that test Azure Blob Storage integration to be
lighter, as they can use the filesystem-backed MockBlobStorageFileSystem
rather than having to run Azurite for BlobStorageFileSystem.

To use this in a test, add the following to the top of the imports in that file:

```
import tests.unit.azure_blob_mock  # noqa: F401
```

"""

# Import MockBlobStorageFileSystem, which is associated with the azfs:// URL.
from codalab.lib.beam.mockblobstoragefilesystem import MockBlobStorageFileSystem  # noqa: F401
from apache_beam.io.filesystems import BlobStorageFileSystem


class DummyClass:
    pass


# By overwriting the __bases__ attribute of BlobStorageFileSystem, we effectively
# disable BlobStorageFileSystem (the default Azure Blob Storage file system built into Beam),
# so that Apache Beam does not recognize it when parsing azfs:// URLs.
BlobStorageFileSystem.__bases__ = (DummyClass,)
