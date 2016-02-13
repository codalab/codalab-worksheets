#!/bin/bash

# Setup environment to run tests
cd $(dirname $0)/..
export PATH="$HOME/codalab-cli/codalab/bin:$PATH"


# Run all tests save for resources and write, which depend on Docker.
# TODO: Add support for docker tests
./venv/bin/python test-cli.py unittest basic upload1 upload2 upload3 download refs rm make run worksheet worksheet_search worksheet_tags freeze detach perm search kill mimic status events batch copy

