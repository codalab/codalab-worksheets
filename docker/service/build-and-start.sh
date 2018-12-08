#!/bin/bash
# build-and-start.sh
# Builds local docker images and runs the service

PARENT_DIR=$(cd "$(dirname "$0")/../../.."; pwd)
echo $PARENT_DIR
if [ -z "$CODALAB_DIR" ]; then CODALAB_DIR="$PARENT_DIR"; fi

cd $CODALAB_DIR/codalab-cli
docker build -t codalab/bundleserver:local-dev -f docker/Dockerfile.server .
docker build -t codalab/worker:local-dev -f docker/Dockerfile.worker .

cd $CODALAB_DIR/codalab-worksheets
docker build -t codalab/webserver:local-dev -f Dockerfile .

cd $CODALAB_DIR/codalab-cli/docker/service
docker-compose up -d
