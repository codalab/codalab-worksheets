#!/bin/bash
# Wrapper around the worker Python code that restarts the worker when upgrading.

if ! [ -z "$PBS_JOBID" ]; then
  # For jobs running on Torque, we use the ID to figure out which worker should
  # run which bundle. Additionally, we need to save the stdout and stderr
  # somewhere to debug workers that fail to start.
  ID_ARG="--id $PBS_JOBID"
  STDOUT=$LOG_DIR/stdout.$PBS_JOBID
  STDERR=$LOG_DIR/stderr.$PBS_JOBID
  WORKER_ARGS="${WORKER_ARGS//|/ }"
else
  WORKER_CODE_DIR=$(dirname $0)
  STDOUT=/dev/stdout
  STDERR=/dev/stderr
fi

while [ 1 ]; do
    python $WORKER_CODE_DIR/main.py $ID_ARG $WORKER_ARGS "$@" >$STDOUT 2>$STDERR
    if [ "$?" -ne "123" ]; then
        break
    fi
done
