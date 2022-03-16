#!/bin/bash

# Performs setup for preemptible test (used in preemptible test in test_cli.py).

if [[ $(cl search request_queue=preemptible .count) -ne 0 ]]; then
    echo "Cleaning up old bundles"
    cl rm --force $(cl search request_queue=preemptible -u)
else
    echo "No bundles to clean up"
fi

__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
timeout 1m ${__dir}/test-setup-preemptible-background.sh # &