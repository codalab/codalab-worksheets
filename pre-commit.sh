#!/bin/bash

# Run this script before you commit.

<<<<<<< HEAD
if ! [ -e venv ]; then
  virtualenv -p python3.6 venv || exit 1
  venv/bin/pip install -r requirements-server.txt || exit 1
  venv/bin/pip install -r requirements.docs.txt || exit 1
  rm -rf worker/codalabworker.egg-info  # Need to clear because of different Python versions
  # Install for generating docs.
  venv/bin/pip install black==18.9b0 || exit 1
=======
if ! [ -e venv3.6 ]; then
  virtualenv -p python2.7 venv3.6 || exit 1
  venv3.6/bin/pip install -r requirements-server.txt || exit 1
  venv3.6/bin/pip install -r requirements.docs.txt || exit 1
  # Install for generating docs.
  venv3.6/bin/pip install -e .
  venv3.6/bin/pip install black==18.9b0 || exit 1
>>>>>>> 404054c1... Remove codalabworker package
fi

venv/bin/pip install -e worker  # Not sure why this is necessary
venv/bin/pip install -e .

# Generate docs
venv/bin/python scripts/gen-rest-docs.py || exit 1  # Outputs to `docs`
venv/bin/python scripts/gen-cli-docs.py || exit 1  # Outputs to `docs`
venv/bin/mkdocs build || exit 1  # Outputs to `site`
# Note: run `venv/bin/mkdocs serve` for a live preview

# Fix style (mutates code!)
<<<<<<< HEAD
venv/bin/black codalab worker scripts *.py || exit
=======
venv3.6/bin/black codalab scripts *.py || exit
>>>>>>> 404054c1... Remove codalabworker package
