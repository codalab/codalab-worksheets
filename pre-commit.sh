#!/bin/bash

# Run this script before you commit.

if ! [ -e venv ]; then
  virtualenv -p python3.6 venv || exit 1
  venv/bin/pip install -r requirements-server.txt || exit 1
  venv/bin/pip install -r requirements.docs.txt || exit 1
fi

venv/bin/pip install -e .

# Generate docs
venv/bin/python scripts/gen-rest-docs.py || exit 1  # Outputs to `docs`
venv/bin/python scripts/gen-cli-docs.py || exit 1  # Outputs to `docs`
venv/bin/mkdocs build || exit 1  # Outputs to `site`
# Note: run `venv/bin/mkdocs serve` for a live preview

# Fix style (mutates code!)
venv/bin/black codalab scripts *.py || exit
