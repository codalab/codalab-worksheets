
declare -a transaction_rates=(0 0.01 0.025 0.05 0.075 0.1 0.125 0.15 0.2 0.3 0.4)
for rate in "${transaction_rates[@]}"
do
    export CODALAB_SENTRY_TRANSACTION_RATE=$rate
    echo "transaction rate $rate"
    echo "env variable: $CODALAB_SENTRY_TRANSACTION_RATE"
    python3 codalab_service.py start -s rest-server worker
    docker exec codalab_rest-server_1 /bin/bash -c "mkdir -p /home/azureuser/codalab-worksheets/var/codalab/home/partitions"
    docker exec -it codalab_rest-server_1 /bin/bash -c "python3 tests/timing/timing_test.py"
done

