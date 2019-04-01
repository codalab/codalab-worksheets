#!/bin/bash
# travis-deploy.sh
# Called by Travis CI at the end of a successful build to do necessary
# deployment actions like building and pushing docker images and PyPI
# packages

TAG=$1
RELEASE=0
if [ "$2" = "release" ]; then
  RELEASE=1
fi

if [ "$RELEASE" = "1" ]; then
  ./docker/build-images.sh $TAG -p
  ./docker/build-images.sh latest -p
  ./scripts/upload-to-pypi.sh $TAG
else
  ./docker/build-images.sh $TAG -p
fi
