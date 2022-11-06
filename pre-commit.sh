#!/bin/bash

# Run this script before you commit.

# Script fails immediately when any command fails.
set -e

if ! [ -e venv ]; then
  python3 -m pip install virtualenv
  python3 -m virtualenv -p python3 venv
fi
venv/bin/pip install -r requirements-server.txt
venv/bin/pip install -r requirements.docs.txt
venv/bin/pip install -r requirements.dev.txt

venv/bin/pip install -e .

venv/bin/pip check

# Generate docs
venv/bin/python scripts/gen-rest-docs.py  # Outputs to `docs`
venv/bin/python scripts/gen-cli-docs.py  # Outputs to `docs`
venv/bin/mkdocs build  # Outputs to `site`
# Note: run `venv/bin/mkdocs serve` for a live preview

# Python style checks and linting
## Fix Python style (mutates code!)
venv/bin/black .
## Check if there are any mypy or flake8 errors
venv/bin/mypy .
venv/bin/flake8 .

# Javascript style checks and linting
## Fix Javascript style (mutates code!)
npm run --prefix frontend format
