"""
Script that initializes Azure Blob Storage and performs
the setup needed, creating the bundles container
if it does not already exist.

Only run during local development with Azurite.
"""
from azure.core.exceptions import ResourceExistsError
from codalab.lib.beam.filesystems import client, AZURE_BLOB_CONTAINER_NAME

try:
    client.create_container(AZURE_BLOB_CONTAINER_NAME)
    print(f"Created initial Azure Blob Storage container \"{AZURE_BLOB_CONTAINER_NAME}\".")
except ResourceExistsError:
    pass
