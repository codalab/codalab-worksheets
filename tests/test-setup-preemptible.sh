#!/bin/bash

# Performs setup for preemptible test (used in preemptible test in test_cli.py).

set -e

cl work localhost::

if [[ $(cl search request_queue=preemptible .count) -ne 0 ]]; then
    echo "Cleaning up old bundles"
    cl rm --force $(cl search request_queue=preemptible -u)
else
    echo "No bundles to clean up"
fi

echo ">> docker kill codalab_worker-preemptible2_1"
docker kill codalab_worker-preemptible2_1

__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CODALAB_USERNAME=$CODALAB_USERNAME CODALAB_PASSWORD=$CODALAB_PASSWORD timeout 5m ${__dir}/test-setup-preemptible-background.sh #&
