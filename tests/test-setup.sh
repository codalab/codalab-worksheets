#!/bin/bash

# When running tests, run this script before starting codalab with codalab_service.py.

mkdir -p /tmp/codalab/link-mounts/test
echo 'hello world!' > /tmp/codalab/link-mounts/test.txt
echo 'hello world!' > /tmp/codalab/link-mounts/test/test.txt
