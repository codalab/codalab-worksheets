#!/bin/bash
# build-images.sh
# Builds docker images for the Codalab Worksheets service and optionally
# pushes them to Dockerhub

usage()
{
  echo "Build docker images from the codebase. [
    [TAG: Tag to use for images]
    [-d --dev: If specified build the dev images instead]
    [-p --push: If specified push the images to Dockerhub (requires DOCKER_USERNAME and DOCKER_PWD environment variables to be set)]
  ]"
}

TAG=$1
shift

PUSH=0
DEV=0

for arg in "$@"; do
  case $arg in
    -p | --push )       PUSH=1
                        ;;
    -d | --dev )        DEV=1
                        ;;
    -h | --help )       usage
                        exit
  esac
done

echo "==> Building the bundleserver Docker image"
docker pull codalab/bundleserver:$TAG
docker build --cache-from codalab/bundleserver:$TAG -t codalab/bundleserver:$TAG -f docker/Dockerfile.server .
echo "==> Building the frontend Docker image"

if [ "$DEV" = "1" ]; then
  docker build -t codalab/frontend-dev:$TAG -f docker/Dockerfile.frontend.dev .
else
  docker pull codalab/frontend:$TAG
  docker build --cache-from codalab/frontend:$TAG -t codalab/frontend:$TAG -f docker/Dockerfile.frontend .
fi

echo "==> Building the worker Docker image"
docker pull codalab/worker:$TAG
docker build --cache-from codalab/worker:$TAG -t codalab/worker:$TAG -f docker/Dockerfile.worker .
echo "==> Building the default-cpu Docker image"
docker pull codalab/default-cpu:$TAG
docker build --cache-from codalab/default-cpu:$TAG -t codalab/default-cpu:$TAG -f docker/Dockerfile.cpu .
echo "==> Building the default-gpu Docker image"
docker pull codalab/default-gpu:$TAG
docker build --cache-from codalab/default-gpu:$TAG -t codalab/default-gpu:$TAG -f docker/Dockerfile.gpu .

if [ "$PUSH" = "1" ]; then
  docker login -u $DOCKER_USER -p $DOCKER_PWD
  docker push codalab/bundleserver:$TAG
  docker push codalab/frontend:$TAG
  docker push codalab/worker:$TAG
  docker push codalab/default-cpu:$TAG
  docker push codalab/default-gpu:$TAG
fi
