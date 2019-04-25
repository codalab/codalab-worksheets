#!/bin/bash
# start-service.sh
# Start a full Codalab Worksheets service

set -a
set -e
set -o pipefail

usage()
{
  echo "Starts a full Codalab Worksheets service. Optionally builds docker images for it.
If not building local images, images with '$CODALAB_VERSION'[\$CODALAB_VERSION] tags are pulled
from DockerHub, otherwise images are built with the '$CODALAB_VERSION'[\$CODALAB_VERSION] tag
and these images are used.

Uses environment variables to configure where to mount persistent directories needed for
the service (MYSQL data directory, service home directory, bundle store, worker working directory),
which version of codalab to use, and root CodaLab user information.

Here's a comprehensive list of environment variables you can set, their explanation,
and default values:
  [
    [ CODALAB_UID: Linux UID that owns the files created by Codalab (ID of the user running this script) ]

    [ CODALAB_MYSQL_ROOT_PWD: Root password for the database (mysql_root_pwd) ]
    [ CODALAB_MYSQL_USER: MYSQL username for the Codalab MYSQL client (bundles_user) ]
    [ CODALAB_MYSQL_PWD: MYSQL password for the Codalab MYSQL client (mysql_pwd) ]

    [ CODALAB_ROOT_USER: Codalab username for the Codalab admin user (codalab) ]
    [ CODALAB_ROOT_PWD: Codalab password for the Codalab admin user (testpassword) ]

    [ CODALAB_SERVICE_HOME: Path on the host machine to store home directory of the Codalab server (/var/lib/codalab/home/) ]
    [ CODALAB_BUNDLE_STORE: Path on the host machine to store Codalab bundle contents (/var/lib/codalab/bundles/) ]
    [ CODALAB_MYSQL_MOUNT: Path on the host machine to store MYSQL data files of the Codalab database (/var/lib/codalab/mysql/) ]
    [ CODALAB_WORKER_DIR: Path on the host machine to store Codalab worker working files, used if worker is specified (/var/lib/codalab/worker-dir/) ]

    [ CODALAB_WORKER_NETWORK_NAME: Name for the docker network that includes the worker and all the container docker networks (codalab_worker_network) ]

    [ CODALAB_HTTP_PORT: Port for nginx to listen on, this is the general use port that redirects to both frontend and backend (80) ]
    [ CODALAB_REST_PORT: Port for the REST server to listen on (2900) ]
    [ CODALAB_FRONTEND_PORT: Port for the frontend server to listen on (2700) ]
    [ CODALAB_MYSQL_PORT: Port for the MYSQL database to bind on the host (3306) ]

    [ CODALAB_VERSION: Version of Codalab to bring up and tag to use if --build option given (latest) ]
  ]

Here's a list of arguments you can pass to control which services are brought up:
  [
    [ -b --build: Build docker images first ]
    [ -d --dev: Development setup from local files ]
    [ -w --worker: Start a CodaLab worker as well ]
    [ -t --test: Run tests as well, fail if tests fail ]
    [ -h --help: get usage help ]
    [ -s --stop: Just stop the service ]
    [ -l --logs: Stream logs until force quit by user ]
  ]"
}

BUILD=0
DEV=0
INIT=0
WORKER=0
TEST=0


CURRENT_UID=$(id -u):$(id -g)
CODALAB_UID=${CODALAB_UID:-$CURRENT_UID}

CODALAB_MYSQL_ROOT_PWD=${CODALAB_MYSQL_ROOT_PWD:-mysql_root_pwd}

CODALAB_MYSQL_USER=${CODALAB_MYSQL_USER:-bundles_user}
CODALAB_MYSQL_PWD=${CODALAB_MYSQL_PWD:-mysql_pwd}

CODALAB_ROOT_USER=${CODALAB_ROOT_USER:-codalab}
CODALAB_ROOT_PWD=${CODALAB_ROOT_PWD:-testpassword}

CODALAB_SERVICE_HOME=${CODALAB_SERVICE_HOME:-/var/lib/codalab/home/}
CODALAB_BUNDLE_STORE=${CODALAB_BUNDLE_STORE:-/var/lib/codalab/bundles/}
CODALAB_MYSQL_MOUNT=${CODALAB_MYSQL_MOUNT:-/var/lib/codalab/mysql/}
CODALAB_WORKER_DIR=${CODALAB_WORKER_DIR:-/var/lib/codalab/worker-dir/}

CODALAB_WORKER_NETWORK_NAME=${CODALAB_WORKER_NETWORK_NAME:-codalab_worker_network}

CODALAB_HTTP_PORT=${CODALAB_HTTP_PORT:-80}
CODALAB_REST_PORT=${CODALAB_REST_PORT:-2900}
CODALAB_FRONTEND_PORT=${CODALAB_FRONTEND_PORT:-2700}
CODALAB_MYSQL_PORT=${CODALAB_MYSQL_PORT:-3306}
CODALAB_VERSION=${CODALAB_VERSION:-latest}

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
# cd to project root
cd $SCRIPT_DIR/../..
echo $(pwd)

for arg in "$@"; do
  case $arg in
    -b | --build )      BUILD=1
                        ;;
    -d | --dev )        DEV=1
                        ;;
    -i | --init )       INIT=1
                        ;;
    -t | --test )       TEST=1
                        ;;
    -w | --worker )     WORKER=1
                        ;;
    -s | --stop )       cd docker/codalab-service
                        echo "==> Bringing down Codalab service"
                        docker-compose down --remove-orphans
                        exit
                        ;;
    -l | --logs )       cd docker/codalab-service
                        docker-compose logs -f
                        exit
                        ;;
    -h | --help )       usage
                        exit
  esac
done

if [ "$BUILD" = "1" ]; then
  echo "==> Building Docker images"
  ./docker/build-images.sh $CODALAB_VERSION
fi

if [ "$DEV" = "1" ]; then
  COMPOSE_FILES='-f docker-compose.yml -f docker-compose.dev.yml'
else
  COMPOSE_FILES='-f docker-compose.yml'
fi

cd docker/codalab-service

echo "==> Bringing down old instance of service"
docker-compose $COMPOSE_FILES down --remove-orphans

mkdir -p $CODALAB_SERVICE_HOME
mkdir -p $CODALAB_BUNDLE_STORE
mkdir -p $CODALAB_MYSQL_MOUNT

echo "===> Starting mysql"
docker-compose $COMPOSE_FILES up -d mysql
echo "===> Configuring Codalab server"
docker-compose $COMPOSE_FILES run --no-deps --rm --entrypoint='' --user=$CODALAB_UID rest-server bash -c "data/bin/wait-for-it.sh mysql:3306 -- /opt/codalab-worksheets/codalab/bin/cl config server/engine_url mysql://$CODALAB_MYSQL_USER:$CODALAB_MYSQL_PWD@mysql:3306/codalab_bundles && /opt/codalab-worksheets/codalab/bin/cl config cli/default_address http://rest-server:$CODALAB_REST_PORT && /opt/codalab-worksheets/codalab/bin/cl config server/rest_host 0.0.0.0"

if [ "$INIT" = "1" ]; then
  echo "===> Creating root user"
  docker-compose $COMPOSE_FILES run --no-deps --rm --entrypoint='' --user='0:0' rest-server bash -c "/opt/codalab-worksheets/venv/bin/pip install /opt/codalab-worksheets && data/bin/wait-for-it.sh mysql:3306 -- opt/codalab-worksheets/venv/bin/python /opt/codalab-worksheets/scripts/create-root-user.py $CODALAB_ROOT_PWD"
fi

echo "===> Bringing up rest server"
docker-compose $COMPOSE_FILES up -d --no-deps --no-recreate rest-server

if [ "$INIT" = "1" ]; then
  echo "===> Creating initial worksheets"
  docker-compose $COMPOSE_FILES run --no-deps --rm --entrypoint='' --user=$CODALAB_UID bundle-manager bash -c "data/bin/wait-for-it.sh rest-server:$CODALAB_REST_PORT -- opt/codalab-worksheets/codalab/bin/cl logout && /opt/codalab-worksheets/codalab/bin/cl new home && /opt/codalab-worksheets/codalab/bin/cl new dashboard"
fi

echo "===> Bringing up bundle manager"
docker-compose $COMPOSE_FILES up -d --no-deps --no-recreate bundle-manager
echo "===> Bringing up frontend"
docker-compose $COMPOSE_FILES up -d --no-deps --no-recreate frontend
echo "===> Bringing up nginx"
docker-compose $COMPOSE_FILES up -d --no-deps --no-recreate nginx

if [ "$WORKER" = "1" ]; then
  echo "===> Bringing up worker"
  mkdir -p $CODALAB_WORKER_DIR
  docker-compose $COMPOSE_FILES up -d --no-deps --no-recreate worker
fi

if [ "$TEST" = "1" ]; then
  cd ../..
  pip install -e ./worker/
  pip install -e ./
  cl config server/engine_url mysql://$CODALAB_MYSQL_USER:$CODALAB_MYSQL_PWD@127.0.0.1:$CODALAB_MYSQL_PORT/codalab_bundles
  python test-cli.py --instance http://localhost:$CODALAB_REST_PORT all
fi
