#!/bin/bash

# Run this script before you commit.

# TODO: merge into one once we have Python 3.

if ! [ -e venv2.7 ]; then
  virtualenv -p python2.7 venv2.7 || exit 1
  venv2.7/bin/pip install -r requirements-server.txt || exit 1
  venv2.7/bin/pip install -r requirements.docs.txt || exit 1
  rm -rf worker/codalabworker.egg-info  # Need to clear because of different Python versions
  venv2.7/bin/pip install -e worker  # Not sure why this is necessary
  venv2.7/bin/pip install -e .
fi

if ! [ -e venv3.6 ]; then
  virtualenv -p python3.6 venv3.6 || exit 1
  venv3.6/bin/pip install black==18.9b0 || exit 1
fi

# Generate docs
venv2.7/bin/python scripts/gen-rest-docs.py || exit 1  # Outputs to `docs`
venv2.7/bin/python scripts/gen-cli-docs.py || exit 1  # Outputs to `docs`
venv2.7/bin/mkdocs build || exit 1  # Outputs to `site`
# Note: run `venv2.7/bin/mkdocs serve` for a live preview

# Fix style (mutates code!)
venv3.6/bin/black codalab worker scripts *.py || exit
