#!/bin/bash
IMAGE_TAG=$1

docker build . -f docker/Dockerfile.server -t codalab/bundleserver:$IMAGE_TAG
docker build . -f docker/Dockerfile.worker -t codalab/worker:$IMAGE_TAG
docker build . -f docker/Dockerfile.cpu -t codalab/default-cpu:$IMAGE_TAG
docker build . -f docker/Dockerfile.gpu -t codalab/default-gpu:$IMAGE_TAG
docker login -u $DOCKER_USER -p $DOCKER_PWD
docker push codalab/bundleserver:$IMAGE_TAG
docker push codalab/worker:$IMAGE_TAG
docker push codalab/default-cpu:$IMAGE_TAG
docker push codalab/default-gpu:$IMAGE_TAG
if [ "$TRAVIS_BRANCH" == 'release' ]; then
docker build . -f docker/Dockerfile.cpu -t codalab/default-cpu:latest
docker build . -f docker/Dockerfile.gpu -t codalab/default-gpu:latest
  docker push codalab/default-cpu:latest
  docker push codalab/default-gpu:latest
fi
