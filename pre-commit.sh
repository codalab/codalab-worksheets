#!/bin/bash

# Run this script before you commit.

if ! [ -e venv ]; then
  virtualenv -p python3 venv || exit 1
  venv/bin/pip install -r requirements-server.txt || exit 1
fi

venv/bin/pip install -e .

# Generate docs
venv/bin/python scripts/gen-rest-docs.py || { rm -rf venv; exit 1; }  # Outputs to `docs`
venv/bin/python scripts/gen-cli-docs.py || { rm -rf venv; exit 1; }  # Outputs to `docs`

# Fix Python and JavaScript style (mutates code!)
venv/bin/black codalab scripts *.py || exit
npm run --prefix frontend format
