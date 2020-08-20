import os
from apache_beam.io.filesystems import FileSystems
from codalab.beam.blobstoragefilesystem import BlobStorageFileSystem

"""
Modifies Beam to add support for Azure Blob Storage filesystems.

Based on work from https://github.com/apache/beam/pull/12581. Once that PR is added into Beam,
we will switch to using a Beam library instead.
"""

os.environ['AZURE_STORAGE_CONNECTION_STRING'] = '...'