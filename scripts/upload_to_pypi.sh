#!/bin/bash
# upload_to_pypi.sh
# Builds and uploads Codalab pip packages

usage()
{
  echo "Build pip packags from the codebase (requires TWINE_USERNAME and TWINE_PASSWORD environment variables to be set). [[VERSION: Version to use for packages]]"
}

VER=$1

echo "==> Packaging codalabworker"
cd ./worker
python setup.py sdist
echo "==> Uploading codalabworker"
twine upload dist/codalabworker-$VER.tar.gz
echo "==> Packaging codalab"
cd ..
python setup.py sdist
echo "==> Uploading codalab"
twine upload dist/codalab-$VER.tar.gz
