#!./venv/bin/python
"""
Script that goes through and reindexes all bundles on disk to be located by their UUID instead of by their data_hash.

For every bundle, finds its associated data_hash, and copies the data underneath data_hash/
"""

import os
import sys
sys.path.append('.')

from codalab.lib.codalab_manager import CodaLabManager
from codalab.lib import path_util

manager = CodaLabManager()
model = manager.model()

CODALAB_HOME = manager.codalab_home

"""Move data/ directory over to a temp area, and create a staging tree for uuid-based storage"""
DATA_DIR = os.path.join(CODALAB_HOME, 'data')
TEMP_DIR = os.path.join(CODALAB_HOME, 'migration-temp')
STAGING_AREA = os.path.join(CODALAB_HOME, 'migration-staging')

path_util.rename(DATA_DIR, TEMP_DIR)
path_util.make_directory(STAGING_AREA)

"""For each data hash, get a list of all bundles that have that hash, and make a copy of the bundle in the staging
area under the UUID for the bundle."""

data_hashes = reduce(lambda x,y: x+y, path_util.ls(TEMP_DIR))
for data_hash in data_hashes:
    orig_location = os.path.join(TEMP_DIR, data_hash)

    bundles_with_hash = model.batch_get_bundles(data_hash=data_hash)
    for bundle in bundles_with_hash:
        uuid = bundle.uuid
        copy_location = os.path.join(STAGING_AREA, uuid)
        print >> sys.stderr, 'Copying Bundle 0x%s with data_hash 0x%s to %s' % (uuid, data_hash, copy_location)
        path_util.copy(orig_location, copy_location)

"""Move the staging location to DATA_DIR, and tell the user to delete the TEMP_DIR"""
path_util.rename(STAGING_AREA, DATA_DIR)

explain_str = """
Migration complete! The directory %s has been left with your old data, if you believe something is out of place run the
following command to restore your old bundle state:

    mv %s %s

To delete the old data off of disk:

    rm -rf %s
""".lstrip() % (TEMP_DIR, TEMP_DIR, DATA_DIR, TEMP_DIR)
print >> sys.stderr, explain_str
