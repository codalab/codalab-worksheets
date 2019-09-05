#!/bin/bash

# Run this script before you commit.

if ! [ -e venv3.6 ]; then
  virtualenv -p python2.7 venv3.6 || exit 1
  venv3.6/bin/pip install -r requirements-server.txt || exit 1
  venv3.6/bin/pip install -r requirements.docs.txt || exit 1
  rm -rf worker/codalabworker.egg-info  # Need to clear because of different Python versions
  # Install for generating docs.
  venv3.6/bin/pip install -e worker  # Not sure why this is necessary
  venv3.6/bin/pip install -e .
  venv3.6/bin/pip install black==18.9b0 || exit 1
fi

# Generate docs
venv3.6/bin/python scripts/gen-rest-docs.py || exit 1  # Outputs to `docs`
venv3.6/bin/python scripts/gen-cli-docs.py || exit 1  # Outputs to `docs`
venv3.6/bin/mkdocs build || exit 1  # Outputs to `site`
# Note: run `venv3.6/bin/mkdocs serve` for a live preview

# Fix style (mutates code!)
venv3.6/bin/black codalab worker scripts *.py || exit
