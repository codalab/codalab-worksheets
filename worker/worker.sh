#!/bin/bash
# Wrapper around the worker Python code that restarts the worker when upgrading.

while [ 1 ]; do
    python $(dirname $0)/main.py "$@"
    if [ "$?" -ne "123" ]; then
        break
    fi
done
