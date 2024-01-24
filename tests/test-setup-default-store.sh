#!/bin/bash

# Create a bundle storage. Used for test bypass server upload using CODALAB_DEFAULT_BUNDLE_STORE_NAME.

set -e

cl work localhost::

echo ">> Setup default Azure storage"
cl store add --name azure-store-default --url azfs://devstoreaccount1/bundles