#!/bin/bash
# start_service.sh
# Start a full Codalab Worksheets Server service

set -a

usage()
{
  echo "Starts a full Codalab Worksheets Server service. Optionally builds docker images for it. If not building local images, 'latest' tags used, otherwise images are built with the 'local-dev' tag and these images are used. [[-b --build: Build docker images first] [-h --help: get usage help]]"
}

BUILD=0
INIT=0

CODALAB_MYSQL_ROOT_PWD=${CODALAB_MYSQL_ROOT_PWD:-mysql_root_pwd}

CODALAB_MYSQL_USER=${CODALAB_MYSQL_USER:-bundles_user}
CODALAB_MYSQL_PWD=${CODALAB_MYSQL_PWD:-mysql_pwd}

CODALAB_ROOT_USER=${CODALAB_ROOT_USER:-codalab}
CODALAB_ROOT_PWD=${CODALAB_ROOT_PWD:-testpassword}

CODALAB_SERVICE_HOME=${CODALAB_SERVICE_HOME:-/var/lib/codalab/home/}
CODALAB_BUNDLE_STORE=${CODALAB_BUNDLE_STORE:-/var/lib/codalab/bundles/}
CODALAB_MYSQL_MOUNT=${CODALAB_MYSQL_MOUNT:-/var/lib/codalab/mysql/}


for arg in "$@"; do
  case $arg in
    -b | --build )      BUILD=1
                        ;;
    -i | --init )       INIT=1
                        ;;
    -h | --help )       usage
                        exit
  esac
done

if [ "$BUILD" = "1" ]; then
  CODALAB_VERSION=local-dev
  echo "==> Building 'local-dev' Docker images"
  ./docker/build_images.sh local-dev
else
  CODALAB_VERSION=${CODALAB_VERSION:-latest}
fi

cd docker/service

echo "==> Bringing down old instance of service"
docker-compose down --remove-orphans

mkdir -p $CODALAB_SERVICE_HOME
mkdir -p $CODALAB_BUNDLE_STORE
mkdir -p $CODALAB_MYSQL_MOUNT

docker-compose up -d mysql
docker-compose run --entrypoint='' rest-server bash -c "/opt/codalab-worksheets/venv/bin/pip install /opt/codalab-worksheets && data/bin/wait-for-it.sh mysql:3306 -- opt/codalab-worksheets/codalab/bin/cl config server/engine_url mysql://$CODALAB_MYSQL_USER:$CODALAB_MYSQL_PWD@mysql:3306/codalab_bundles && /opt/codalab-worksheets/codalab/bin/cl config cli/default_address http://rest-server:2900 && /opt/codalab-worksheets/codalab/bin/cl config server/rest_host 0.0.0.0"

if [ "$INIT" = "1" ]; then
  docker-compose run --entrypoint='' rest-server bash -c "/opt/codalab-worksheets/venv/bin/pip install /opt/codalab-worksheets && data/bin/wait-for-it.sh mysql:3306 -- opt/codalab-worksheets/venv/bin/python /opt/codalab-worksheets/scripts/create-root-user.py $CODALAB_ROOT_PWD"
fi

docker-compose up -d rest-server

if [ "$INIT" = "1" ]; then
  docker-compose run --entrypoint='' bundle-manager bash -c "data/bin/wait-for-it.sh rest-server:2900 -- opt/codalab-worksheets/codalab/bin/cl logout && /opt/codalab-worksheets/codalab/bin/cl new home && /opt/codalab-worksheets/codalab/bin/cl new dashboard"
fi

docker-compose up -d bundle-manager
docker-compose up -d frontend
docker-compose up -d nginx
