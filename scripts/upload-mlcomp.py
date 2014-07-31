#!/usr/bin/python

# Script to upload an MLcomp program or dataset to CodaLab.  An MLcomp
# program/dataset has a metadata file in it, which is used to specify the
# arguments (CodaLab stores metadata exclusively in the database).

# Usage: directories to upload

import sys, os, yaml

for path in sys.argv[1:]:
    info = yaml.load(open(os.path.join(path, 'metadata')))
    if 'format' in info:
        bundle_type = 'dataset'
        tags = info['format']
    elif 'task' in info:
        bundle_type = 'program'
        tags = info['task']
    else:
        raise 'Weird info: %s' % info
 
    if not os.getenv('CODALAB_SESSION'):
        os.environ['CODALAB_SESSION'] = str(os.getppid())
    print 'session:', os.getenv('CODALAB_SESSION')
    cmd = "cl upload %s `/bin/ls -d %s/*` --name %s --tags mlcomp %s --description \"%s\" --auto" % \
        (bundle_type, path, info['name'], tags, info['description'].replace('"', '\\"'))
    print cmd
    os.system(cmd)
