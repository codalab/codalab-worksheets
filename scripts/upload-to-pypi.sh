#!/bin/bash
# upload-to-pypi.sh
# Builds and uploads Codalab pip packages

usage()
{
  echo "Build pip packags from the codebase (requires TWINE_USERNAME and TWINE_PASSWORD environment variables to be set). [[VERSION: Version to use for packages]]"
}

VER=$1
set -e

echo "==> Packaging codalab"
python3 setup.py bdist_wheel
echo "==> Uploading codalab"
twine upload dist/codalab-$VER-py3-none-any.whl
