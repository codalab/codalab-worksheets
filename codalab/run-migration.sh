#!/bin/bash

# 5min timeout
timeout_duration=300
# default location migration script writes to
filename='/home/azureuser/codalab-worksheets/var/codalab/home/bundle_ids_0.csv'
while read line
do
# Command to run
    command_to_run="python migration.py -t blob-prod -u $line -p 1"

    timeout -k 20 $timeout_duration $command_to_run
    exit_status=$?
    if [ $exit_status -eq 124 ]; then
        echo "Process took too long. Killing the process for bundle $line..."
    fi
# skips header row
done < <(tail -n +2 $filename)
