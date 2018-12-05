#!/bin/bash
IMAGE_TAG=$1

docker build . -f docker/Dockerfile.server -t codalab/bundleserver:$IMAGE_TAG
docker build . -f docker/Dockerfile.worker -t codalab/worker:$IMAGE_TAG
docker build . -f docker/Dockerfile.cpu -t codalab/default-cpu:$IMAGE_TAG
docker build . -f docker/Dockerfile.gpu -t codalab/default-gpu:$IMAGE_TAG
