#!/bin/bash

# Configures use of MultiDiskBundleStore for CircleCI tests.
# Modifies the configuration file, and adds several disks for testing.

CONFIG="
{
    \"aliases\": {
        \"localhost\": \"http://localhost:2800\",
        \"main\": \"https://worksheets.codalab.org/bundleservice\"
    },
    \"bundle_store\": \"MultiDiskBundleStore\",
    \"cli\": {
        \"default_address\": \"local\",
        \"verbose\": 1
    },
    \"server\": {
        \"auth\": {
            \"class\": \"MockAuthHandler\"
        },
        \"class\": \"SQLiteModel\",
        \"engine_url\": \"sqlite:///${HOME}/.codalab/bundle.db\",
        \"host\": \"localhost\",
        \"port\": 2800,
        \"rest_host\": \"localhost\",
        \"rest_port\": 2900,
        \"verbose\": 1
    },
    \"workers\": {
        \"q\": {
            \"dispatch_command\": \"python \$CODALAB_CLI/scripts/dispatch-q.py\",
            \"verbose\": 1
        }
    }
}
"
echo $CONFIG > $HOME/.codalab/config.json
# Add two disks
mkdir -p /tmp/A /tmp/B
./codalab/bin/cl bs-add-partition /tmp/A A
./codalab/bin/cl bs-add-partition /tmp/B B

