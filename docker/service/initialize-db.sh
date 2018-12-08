#!/bin/sh
# initialize-db.sh
# Creates the bundles_user for the codalab database and adds the root codalab user and home worksheets

set -e

until mysql -h mysql -u root -pROOTPWD -e exit; do
  >&2 echo "Mysql server not available - waiting"
  sleep 1
done

>&2 echo "Mysql server responding - initializing database"

mysql -h mysql -u root -pROOTPWD -e "GRANT ALL PRIVILEGES ON codalab_bundles.* TO 'codalab'@'%' IDENTIFIED BY 'TESTPASSWORD'"

>&2 echo "Created user codalab"

mysql -h mysql -u root -pROOTPWD -e "CREATE DATABASE codalab_bundles"

>&2 echo "Created database codalab_bundles"

. /opt/codalab-cli/venv/bin/activate
pip install /opt/codalab-cli/worker
pip install /opt/codalab-cli

python /opt/codalab-cli/scripts/create-root-user.py TESTPASSWORD

>&2 echo "Created CodaLab root user codalab"

cl new home

>&2 echo "Created home"

cl new dashboard

>&2 echo "Created dashboard"
