#!/bin/bash
# build_images.sh
# Builds docker images for the Codalab Worksheets service and optionally
# pushes them to Dockerhub

usage()
{
  echo "Build docker images from the codebase. [[TAG: Tag to use for images] [-p --push: If specified push the images to Dockerhub (requires DOCKER_USERNAME and DOCKER_PWD environment variables to be set)]]"
}

TAG=$1
shift
if [ "$1" = '-p' ] || [ "$1" = '--push' ]; then
  PUSH=1
else
  PUSH=0
fi

echo "==> Building the bundleserver Docker image"
docker build -t codalab/bundleserver:$TAG -f docker/Dockerfile.server .
echo "==> Building the frontend Docker image"
docker build -t codalab/frontend:$TAG -f docker/Dockerfile.frontend .
echo "==> Building the worker Docker image"
docker build -t codalab/worker:$TAG -f docker/Dockerfile.worker .
#echo "==> Building the default-cpu Docker image"
#docker build -t codalab/default-cpu:$TAG -f docker/Dockerfile.cpu .
#echo "==> Building the default-gpu Docker image"
#docker build -t codalab/default-gpu:$TAG -f docker/Dockerfile.gpu .

if [ "$PUSH" = "1" ]; then
  docker login -u $DOCKER_USER -p $DOCKER_PWD
  docker push codalab/bundleserver:$TAG
  docker push codalab/frontend:$TAG
  docker push codalab/worker:$TAG
#  docker push codalab/default-cpu:$TAG
#  docker push codalab/default-gpu:$TAG
fi
