#!/bin/bash

# Called by test-setup-preemptible.sh and runs in the background.
# Waits until preemptible bundle is running, then stops the worker,
# and finally starts another preemptible worker where the bundle can finish
# running on.

set -e

cl work localhost::

while : ; do
    echo ">> cl search request_queue=preemptible state=running .count"
    num_bundles=$(cl search request_queue=preemptible state=running .count)
    echo $num_bundles
    if [[ $num_bundles -ne 0 ]]; then
        echo "Bundle is running! Stopping the worker and bundle container now."
        echo ">> docker kill codalab_worker-preemptible_1"
        docker kill codalab_worker-preemptible_1
        run_container=$(docker ps -f name=codalab_run -q)
        echo ">> docker kill $run_container && docker rm $run_container"
        docker kill $run_container && docker rm $run_container
        echo "Worker stopped successfully. Starting another preemptible worker in 1 minute..."
        sleep 60
        echo ">> docker start codalab_worker-preemptible2_1"
        docker start codalab_worker-preemptible2_1
        break
    else
        echo "No bundles running"
    fi
    sleep 1
done