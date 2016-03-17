#!/bin/bash
# Wrapper around the worker Python code that restarts the worker when upgrading.

# TODO: Add comments.
if ! [ -z "$PBS_JOBID" ]; then
  # Running on Torque.
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
