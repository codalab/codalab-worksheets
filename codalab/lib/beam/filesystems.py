import os
from apache_beam.io.filesystems import FileSystems
from .blobstoragefilesystem import BlobStorageFileSystem
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

"""
Modifies Beam to add support for Azure Blob Storage filesystems.

Based on work from https://github.com/apache/beam/pull/12581. Once that PR is added into Beam,
we will switch to using a Beam library instead.
"""

# Test key for Azurite (local development)
os.environ[
    'AZURE_STORAGE_CONNECTION_STRING'
] = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;'

client = BlobServiceClient.from_connection_string(os.environ['AZURE_STORAGE_CONNECTION_STRING'])
try:
    client.create_container("bundles")
    print("Created container 'bundles'.")
except ResourceExistsError:
    pass