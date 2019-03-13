#!/bin/bash
# travis_deploy.sh
# Called by Travis CI at the end of a successful build to do necessary
# deployment actions like building and pushing docker images and PyPI
# packages

TAG=$1
RELEASE=0
if [ "$2" = "release" ]; then
  RELEASE=1
fi

if [ "$RELEASE" = "1" ]; then
  ./docker/build_images.sh $TAG -p
  ./docker/build_images.sh latest -p
  ./scripts/upload_to_pypi.sh $TAG
else
  ./docker/build_images.sh $TAG -p
fi
