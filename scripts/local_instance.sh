#!/bin/bash

set -e

# Setup the database:
service mysql start
mysql -u root -prootpw -e "CREATE USER 'codalab'@'*' IDENTIFIED BY 'testpwd';"
mysql -u root -prootpw -e "CREATE DATABASE codalab_bundles;"
mysql -u root -prootpw -e "GRANT ALL ON codalab_bundles.* TO 'codalab'@'*';"

pip install ${CODALAB_DIR}/codalab-cli/worker
pip install ${CODALAB_DIR}/codalab-cli
python -c "import codalab"

cl config server/engine_url mysql://root:rootpw@localhost:3306/codalab_bundles
cl config cli/default_address http://localhost:2900

python ${CODALAB_DIR}/codalab-cli/scripts/create-root-user.py $CODALAB_PASSWORD

# Start the server
cl server &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start rest server: $status"
  exit $status
fi

# Start the bundle manager
cl bundle-manager &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start bundle manager: $status"
  exit $status
fi

cl status

#cl new home
#cl new dashboard

# Naive check runs checks once a minute to see if either of the processes exited.
# This illustrates part of the heavy lifting you need to do if you want to run
# more than one service in a container. The container exits with an error
# if it detects that either of the processes has exited.
# Otherwise it loops forever, waking up every 60 seconds

while sleep 60; do
  ps aux |grep "cl server" |grep -q -v grep
  SERVER_STATUS=$?
  ps aux |grep "cl bundle-manager" |grep -q -v grep
  BUNDLE_MANAGER_STATUS=$?
  ps aux |grep "cl-worker" |grep -q -v grep
  WORKER_STATUS=0
  # If the greps above find anything, they exit with 0 status
  # If they are not both 0, then something is wrong
  if [ $SERVER_STATUS -ne 0 -o $BUNDLE_MANAGER_STATUS -ne 0 -o $WORKER_STATUS -ne 0 ]; then
    echo "One of the processes has already exited."
    exit 1
  fi
done


