#!/usr/bin/python

# Script to upload an MLcomp program or dataset to CodaLab.  An MLcomp
# program/dataset has a metadata file in it, which is used to specify the
# arguments (CodaLab stores metadata exclusively in the database).

# Usage: directories to upload

import sys
import os
import yaml

for path in sys.argv[1:]:
    info = yaml.safe_load(open(os.path.join(path, 'metadata')))
    if 'format' in info:
        bundle_type = 'dataset'
        tags = info['format']
    elif 'task' in info:
        bundle_type = 'program'
        tags = info['task']
    else:
        raise ValueError('Weird info: %s' % info)

    name = info['name'].replace(' ', '_')
    cmd = "cl upload %s `/bin/ls -d %s/*` --name %s --tags mlcomp %s --description \"%s\"" % (
        bundle_type,
        path,
        name,
        tags,
        info['description'].replace('"', '\\"'),
    )
    print(cmd)
    os.system(cmd)
