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
  python3 codalab_service.py build all -v $TAG --push
  python3 codalab_service.py build all -v latest --push
  python3 scripts/upload-to-pypi.sh $TAG
else
  python3 codalab_service.py build all -v $TAG --push
fi

