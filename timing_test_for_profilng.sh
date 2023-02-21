
declare -a transaction_rates=(0 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 1)
for rate in "${transaction_rates[@]}"
do
    export CODALAB_SENTRY_TRANSACTION_RATE=$rate
    echo "transaction rate $rate"
    ./codalab_service.py start
    docker exec -it codalab_rest-server_1 /bin/bash -c "python3 tests/timing/timing_test.py"
done

