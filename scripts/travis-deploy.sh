#!/bin/bash

# Called by Travis CI at the end of a successful build to do necessary
# deployment actions like building and pushing docker images and PyPI
# packages.  The two possibilities are:
#
#   travis-deploy.sh master
#   travis-deploy.sh 0.3.3   (for releases)

tag=$1

# Check if ENV variable CODALAB_DOCKER_USERNAME is set. If not, it means this build was triggered
# by an external user. Then we shouldn't push the docker image to docker hub.
PUSH_FLAG=$([ -z "${CODALAB_DOCKER_USERNAME}" ] || echo "--push")

python3 codalab_service.py build --version $tag --pull $PUSH_FLAG
if [ "$tag" != "master" ]; then
  python3 codalab_service.py build --version latest --pull $PUSH_FLAG
  ./scripts/upload-to-pypi.sh $tag
fi
