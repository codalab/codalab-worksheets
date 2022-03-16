#!/bin/bash

# Called by test-setup-preemptible.sh and runs in the background.
# Waits until preemptible bundle is running, then stops the worker.

while : ; do
    echo ">> cl search request_queue=preemptible state=running .count"
    num_bundles=$(cl search .count)
    echo $num_bundles
    if [[ $num_bundles -ne 0 ]]; then
        echo "Bundle is running! Stopping the worker and bundle container now."
        echo ">> docker kill codalab_worker-preemptible_1"
        docker kill codalab_worker-preemptible_1
        echo ">> docker kill $(docker ps -f name=codalab_run -q)"
        docker kill $(docker ps -f name=codalab_run -q)
        echo "Worker stopped successfully."
        break
    else
        echo "No bundles running"
    fi
    sleep 1
done