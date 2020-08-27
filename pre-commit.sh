#!/bin/bash

# Run this script before you commit.

# Script fails immediately when any command fails.
set -e

if ! [ -e venv ]; then
  virtualenv -p python3 venv
fi
venv/bin/pip install -r requirements-server.txt
venv/bin/pip install -r requirements.docs.txt
venv/bin/pip install -r requirements.dev.txt

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
mypy .
