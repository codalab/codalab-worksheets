import apache_beam.io.azure.blobstorageio
from azure.storage.blob import BlobServiceClient
from apache_beam.io.azure.blobstorageio import AZURE_DEPS_INSTALLED
import os

def new_init(self, client=None):
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    if client is None:
        self.client = BlobServiceClient.from_connection_string(
            connect_str, kwargs={
                "max_single_put_size": 4*1024*1024, # split to 4MB chunks
                "max_single_get_size": 4*1024*1024, # split to 4MB chunks
            }
        )
    else:
        self.client = client
    if not AZURE_DEPS_INSTALLED:
        raise RuntimeError('Azure dependencies are not installed. Unable to run.')

# Monkey patch the init function
apache_beam.io.azure.blobstorageio.BlobStorageIO.__init__ = new_init