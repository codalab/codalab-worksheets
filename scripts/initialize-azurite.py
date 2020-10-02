"""
Script that initializes Azure Blob Storage setup needed,
creating the "bundles" container if necessary.

Only run during local development with Azurite.
"""
import os
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

# Test connection string for Azurite (local development)
TEST_CONN_STR = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;"

client = BlobServiceClient.from_connection_string(os.environ.get('CODALAB_AZURE_BLOB_CONNECTION_STRING', TEST_CONN_STR))
try:
    client.create_container("bundles")
    print("Azure Blob Storage: created container 'bundles'.")
except ResourceExistsError:
    pass
