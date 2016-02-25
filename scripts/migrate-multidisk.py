#!/usr/bin/env python

"""
Migrate an existing SingleDiskBundleStore over to a MultiDiskBundleStore
"""

import json
import os
import sys
from subprocess import Popen

CODALAB_HOME = os.environ.get('CODALAB_HOME', None)

if CODALAB_HOME == None:
    print >> sys.stderr, 'Make sure you have the CODALAB_HOME environment variable set and try again.'
    sys.exit(1)

def add_multidisk_config():
    """Adds bundle_store=MultiDiskBundleStore to the configuration file"""
    config_path = os.path.join(CODALAB_HOME, 'config.json')
    with open(config_path, 'r') as config_file:
        config_json = json.loads(config_file.read())
        config_json['bundle_store'] = 'MultiDiskBundleStore'
        updated_config = json.dumps(config_json, indent=4)
        print updated_config

    with open(config_path, 'w') as config_file:
        config_file.write(updated_config)

    print "Updated configuration file."

def add_dummy_partition(user_specified_dir=None):
    """Adds a new partition to MultiDiskBundleStore to hold the existing bundles."""
    dummy_dir = os.path.join(CODALAB_HOME, 'dummy') if user_specified_dir == None else user_specified_dir

    print 'Creating directory %s ...' % dummy_dir
    os.mkdir(dummy_dir)

    print 'Adding default partition %s to bundle store...' % dummy_dir
    exit_code = Popen(['cl', 'bs-add-partition', dummy_dir, 'default']).wait()
    if exit_code < 0:
        print >> sys.stderr, 'Error, adding partition exited with code %d' % exit_code
        sys.exit(exit_code)

    # Move bundles to the new location
    old_bundles_dir = os.path.join(CODALAB_HOME, 'bundles')
    new_bundles_dir = os.path.join(dummy_dir, 'bundles')
    print 'Moving bundles from %s to %s ...' % (old_bundles_dir, new_bundles_dir)
    os.rmdir(new_bundles_dir)
    os.rename(old_bundles_dir, new_bundles_dir)
    return dummy_dir

add_multidisk_config()

if len(sys.argv) > 1:
    """First argument to the script is the name of a directory to create"""
    partition_root = add_dummy_partition(sys.argv[1])
else:
    partition_root = add_dummy_partition()

print 'Successfully setup MultiDiskBundleStore partition in %s!' % partition_root
