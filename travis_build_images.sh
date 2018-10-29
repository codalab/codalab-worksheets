#!/bin/bash

if [ "$BRANCH" == "master" || "$BRANCH" == "staging" ]; then
  docker build . -f docker/Dockerfile.server -t codalab/bundleserver:$BRANCH
  docker build . -f docker/Dockerfile.worker -t codalab/worker:$BRANCH
  docker build . -f docker/Dockerfile.cpu -t codalab/default-cpu:$BRANCH
  docker build . -f docker/Dockerfile.gpu -t codalab/default-gpu:$BRANCH
  docker login -u $DOCKER_USER -p $DOCKER_PWD
  docker push codalab/bundleserver:$BRANCH
  docker push codalab/worker:$BRANCH
  docker push codalab/default-cpu:$BRANCH
  docker push codalab/default-gpu:$BRANCH
fi
