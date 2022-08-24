import os
from azure.storage.blob import BlobServiceClient

# Monkey-patch BlobStorageUploader
from .blobstorageuploader import BlobStorageUploader
import apache_beam.io.azure.blobstorageio
apache_beam.io.azure.blobstorageio.BlobStorageUploader = BlobStorageUploader

# Test connection string for Azurite (local development)
TEST_CONN_STR = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://azurite:10000/devstoreaccount1;"
)

# The Apache beam BlobStorageFileSystem expects the AZURE_STORAGE_CONNECTION_STRING environment variable
# to be set to the correct Azure Blob Storage connection string.
AZURE_BLOB_CONNECTION_STRING = (
    os.environ.get("CODALAB_AZURE_BLOB_CONNECTION_STRING") or TEST_CONN_STR
)

os.environ['AZURE_STORAGE_CONNECTION_STRING'] = AZURE_BLOB_CONNECTION_STRING

client = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION_STRING)

# This is the account name of the account, which determines the first part of Azure URLs. For example,
# if AZURE_BLOB_ACCOUNT_NAME is equal to "devstoreaccount1", all Azure URLs for objects within that account
# will start with "azfs://devstoreaccount1/"
AZURE_BLOB_ACCOUNT_NAME = client.account_name

# Account key of the account. Used to sign SAS URLs.
AZURE_BLOB_ACCOUNT_KEY = client.credential.account_key

# Container name where bundles are stored.
AZURE_BLOB_CONTAINER_NAME = "bundles"

# Set to True if using Azurite.
LOCAL_USING_AZURITE = "http://azurite" in AZURE_BLOB_CONNECTION_STRING

# HTTP endpoint used to directly access Blob Storage. Used to generate SAS URLs.
AZURE_BLOB_HTTP_ENDPOINT = f"http://localhost:10000/{AZURE_BLOB_ACCOUNT_NAME}" if LOCAL_USING_AZURITE else f"https://{AZURE_BLOB_ACCOUNT_NAME}.blob.core.windows.net"


def get_azure_bypass_conn_str():
    """
    Get current Azure connection string from environment variables.
    Used for bypass server upload to blob storage.
    Returns the Azure connection string without Account key.
    """
    conn_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    all_fields = conn_str.split(";")
    allow_fields = ["DefaultEndpointsProtocol", "AccountName", "BlobEndpoint", "EndpointSuffix"]
    fields = []
    for field in all_fields:
        if field.split('=')[0] in allow_fields:
            fields.append(field)
    return ';'.join(fields)
