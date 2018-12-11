#!/bin/sh
# run-tests.sh
# Waits for codalab initialization and runs tests

set -e

until cl work http://rest-server:2900::; do
  >&2 echo "Codalab server not available - waiting"
  sleep 1
done

cl status

>&2 echo "CL up, begin tests"

python /opt/codalab-cli/test-cli.py default
