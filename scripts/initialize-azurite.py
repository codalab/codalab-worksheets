"""
Script that initializes Azure Blob Storage and performs
the setup needed, creating the "bundles" container
if it does not already exist.

Only run during local development with Azurite.
"""
from azure.core.exceptions import ResourceExistsError
from codalab.lib.beam.filesystems import client

try:
    client.create_container("bundles")
    print("Created initial Azure Blob Storage container \"bundles\".")
except ResourceExistsError:
    pass
