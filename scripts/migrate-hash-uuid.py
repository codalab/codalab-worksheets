#!./venv/bin/python
"""
Script that goes through and reindexes all bundles on disk to be located by their UUID instead of by their data_hash.

For every bundle, finds its associated data_hash, and copies the data underneath data_hash/

By default, this script runs in dry-run mode, i.e. it prints verbose output but does not make changes to
the file system. When you're ready to perform the migration, run with the '-f' flag.
"""

import os
import sys
import shlex
from subprocess import Popen
sys.path.append('.')

from codalab.lib.codalab_manager import CodaLabManager
from codalab.lib import path_util

dry_run = False if len(sys.argv) > 1 and sys.argv[1] == '-f' else True

manager = CodaLabManager()
model = manager.model()

CODALAB_HOME = manager.codalab_home

"""Move data/ directory over to a temp area, and create a staging tree for uuid-based storage"""
DATA_DIR = os.path.join(CODALAB_HOME, 'data')
FINAL_LOCATION = os.path.join(CODALAB_HOME, 'bundles')

if not dry_run:
    path_util.make_directory(FINAL_LOCATION)

"""For each data hash, get a list of all bundles that have that hash, and make a copy of the bundle in the staging
area under the UUID for the bundle."""
data_hashes = reduce(lambda x,y: x+y, path_util.ls(DATA_DIR))
for data_hash in data_hashes:
    orig_location = os.path.join(DATA_DIR, data_hash)

    bundles_with_hash = model.batch_get_bundles(data_hash=data_hash)
    # We'd prefer renaming bundles to making copies, but because we are converting from deduplicated storage
    # we need to make sure that we only perform renames if we map 1:1 UUID->Hash.
    rename_allowed = len(bundles_with_hash) <= 1
    for bundle in bundles_with_hash:
        # Build the command to be executed in a subshell
        uuid = bundle.uuid
        copy_location = os.path.join(FINAL_LOCATION, uuid)
        command = '%s %s %s' % ('mv' if rename_allowed else 'cp -a', orig_location, copy_location)
        print command
        if not dry_run:
            exec_str = shlex.split(command)
            cmd = Popen(exec_str)
            exit_code = cmd.wait()
            if exit_code != 0:
                print >> sys.stderr, 'command \'%s\' failed(status=%d), aborting...'
                break


dry_run_str = """
This was a dry run, no migration occurred. To perform full migration, run again with `-f':

    %s -f
""".rstrip() % sys.argv[0]

explain_str = "Migration complete!"

print >> sys.stderr, dry_run_str if dry_run else explain_str
