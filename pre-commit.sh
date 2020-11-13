#!/bin/bash

# Run this script before you commit.

# Script fails immediately when any command fails.
set -e

if ! [ -e venv ]; then
  python3 -m pip install virtualenv
  python3 -m virtualenv -p python3 venv
fi
venv/bin/pip install -r requirements-server.txt --no-cache
venv/bin/pip install -r requirements.docs.txt --no-cache
venv/bin/pip install -r requirements.dev.txt --no-cache

venv/bin/pip check || let val=$?
if [ $val -ne 0 ] ; then
  echo "Broken version requirements detected";
  # Should I exit with 1 here?
fi

venv/bin/pip install -e .

# Generate docs
venv/bin/python scripts/gen-rest-docs.py  # Outputs to `docs`
venv/bin/python scripts/gen-cli-docs.py  # Outputs to `docs`
venv/bin/mkdocs build  # Outputs to `site`
# Note: run `venv/bin/mkdocs serve` for a live preview

# Fix Python and JavaScript style (mutates code!)
venv/bin/black codalab scripts *.py
npm run --prefix frontend format

# Check if there are any mypy errors
venv/bin/mypy .
venv/bin/flake8 .
