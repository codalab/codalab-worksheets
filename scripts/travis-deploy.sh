#!/bin/bash

# Called by Travis CI at the end of a successful build to do necessary
# deployment actions like building and pushing docker images and PyPI
# packages.  The two possibilities are:
#
#   travis-deploy.sh master
#   travis-deploy.sh 0.3.3   (for releases)

tag=$1

python3.6 codalab_service.py build --version $tag --pull --push
if [ "$tag" != "master" ]; then
  python3.6 codalab_service.py build --version latest --pull --push
  ./scripts/upload-to-pypi.sh $tag
fi