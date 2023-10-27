declare -a transaction_rates=(0 1)
for rate in "${transaction_rates[@]}"
do
    export CODALAB_SENTRY_TRANSACTION_RATE=$rate
    export CODALAB_SENTRY_PROFILES_RATE=$rate
    echo "transaction rate $rate"
    echo "env variable: $CODALAB_SENTRY_TRANSACTION_RATE"
    python3 codalab_service.py start -s rest-server worker
    docker exec codalab_rest-server_1 /bin/bash -c "mkdir -p /home/azureuser/codalab-worksheets/var/codalab/home/partitions"
    docker exec -it codalab_rest-server_1 /bin/bash -c "python3 tests/timing/timing_test.py"
done