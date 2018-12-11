#!/bin/bash
# build-and-start.sh
# Builds local docker images and runs the service

usage()
{
  echo "Build docker images from current codebase and run a full Codalab service on docker. [[-t --test : runs integration tests against the service] [-w --webserver : also builds and runs docker containers for the webserver portion, assumes webserver repo can be found in the same directory as the CLI repo]]"
}

TEST=0
WEBSERVER=0

while [ "$1" != "" ]; do
    case $1 in
        -t | --test )           shift
                                TEST=1
                                ;;
        -w | --webserver )      WEBSERVER=1
                                ;;
        -h | --help )           usage
                                exit
    esac
    shift
done

PARENT_DIR=$(cd "$(dirname "$0")/../../.."; pwd)
echo $PARENT_DIR
if [ -z "$CODALAB_DIR" ]; then CODALAB_DIR="$PARENT_DIR"; fi

docker-compose down

cd $CODALAB_DIR/codalab-cli
docker build -t codalab/bundleserver:local-dev -f docker/Dockerfile.server .
docker build -t codalab/worker:local-dev -f docker/Dockerfile.worker .

COMPOSE_FILES="-f docker-compose.yml"
COMPOSE_FLAGS="-d"

if [ "$WEBSERVER" = "1" ]; then
  cd $CODALAB_DIR/codalab-worksheets
  docker build -t codalab/webserver:local-dev -f Dockerfile .
  COMPOSE_FILES="$COMPOSE_FILES -f docker-compose.webserver.yml"
fi

if [ "$TEST" = "1" ]; then
  COMPOSE_FILES="$COMPOSE_FILES -f docker-compose.test.yml"
  COMPOSE_FLAGS="--exit-code-from tests"
fi

cd $CODALAB_DIR/codalab-cli/docker/service
docker-compose $COMPOSE_FILES up $COMPOSE_FLAGS
