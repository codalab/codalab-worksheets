import os
from azure.storage.blob import BlobServiceClient

# Test connection string for Azurite (local development)
TEST_CONN_STR = ("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
"BlobEndpoint=http://azurite:10000/devstoreaccount1;")

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
