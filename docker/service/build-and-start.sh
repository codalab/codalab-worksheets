#!/bin/bash
# build-and-start.sh
# Builds local docker images and runs the service

usage()
{
  echo "Build docker images from current codebase and run a full Codalab service on docker. [[-t --test : runs integration tests against the service] [-f --frontend : also builds and runs docker containers for the frontend portion]]"
}

TEST=0
FRONTEND=0

while [ "$1" != "" ]; do
    case $1 in
        -t | --test )           shift
                                TEST=1
                                ;;
        -w | --frontend )      FRONTEND=1
                                ;;
        -h | --help )           usage
                                exit
    esac
    shift
done

echo "==> Bringing down service"
docker-compose down --remove-orphans

cd ../..
echo "==> Building the bundleserver Docker image"
docker build -t codalab/bundleserver:local-dev -f docker/Dockerfile.server .
echo "==> Building the worker Docker image"
docker build -t codalab/worker:local-dev -f docker/Dockerfile.worker .

COMPOSE_FILES="-f docker-compose.yml"
COMPOSE_FLAGS="-d"

if [ "$FRONTEND" = "1" ]; then
  docker build -t codalab/frontend:local-dev -f docker/Dockerfile.frontend .
  COMPOSE_FILES="$COMPOSE_FILES -f docker-compose.frontend.yml"
fi

if [ "$TEST" = "1" ]; then
  COMPOSE_FILES="$COMPOSE_FILES -f docker-compose.test.yml"
  COMPOSE_FLAGS="--exit-code-from tests"
fi

cd docker/service
echo "==> Bringing service up with 'docker-compose $COMPOSE_FILES up $COMPOSE_FLAGS'"
docker-compose $COMPOSE_FILES up $COMPOSE_FLAGS
