```
pip install -e .

cl work https://worksheets.codalab.org::

# start up your local machine
codalab-service start -bd
codalab-service start -bds default worker2

# just start one instance (ex., if you only changed rest server code)
codalab-service start -bds rest-server

# connect to your local machine
cl work http://localhost::


docker ps

```





todo

codalab-service build -s worker && kind load docker-image codalab/worker:k8s_runtime --name codalab && codalab-service start -bds worker-manager-cpu && docker logs codalab_kubernetes-worker-manager-cpu_1 --follow

## ws

codalab-service start -bds ws-server && docker logs codalab_ws-server_1 --follow

codalab-service start -bds rest-server && docker logs codalab_rest-server_1 --follow

codalab-service start -bds rest-server init
docker exec -it codalab_rest-server_1 /bin/bash

python3 scripts/create-root-user.py pwd

```
process: Fatal Python error: Segmentation fault
process: 
process: Current thread 0x00007f95614a2740 (most recent call first):
process:   File "/opt/conda/lib/python3.7/site-packages/MySQLdb/connections.py", line 164 in __init__
process:   File "/opt/conda/lib/python3.7/site-packages/MySQLdb/__init__.py", line 84 in Connect
process:   File "/opt/conda/lib/python3.7/site-packages/sqlalchemy/engine/default.py", line 493 in connect
process:   File "/opt/conda/lib/python3.7/site-packages/sqlalchemy/engine/strategies.py", line 114 in connect
process:   File "/opt/conda/lib/python3.7/site-packages/sqlalchemy/pool/base.py", line 656 in __connect
process:   File "/opt/conda/lib/python3.7/site-packages/sqlalchemy/pool/base.py", line 440 in __init__
process:   File "/opt/conda/lib/python3.7/site-packages/sqlalchemy/pool/base.py", line 309 in _create_connection
process:   File "/opt/conda/lib/python3.7/site-packages/sqlalchemy/pool/impl.py", line 137 in _do_get
process:   File "/opt/conda/lib/python3.7/site-packages/sqlalchemy/pool/base.py", line 495 in checkout
process:   File "/opt/conda/lib/python3.7/site-packages/sqlalchemy/pool/base.py", line 778 in _checkout
process:   File "/opt/conda/lib/python3.7/site-packages/sqlalchemy/pool/base.py", line 364 in connect
process:   File "/opt/conda/lib/python3.7/site-packages/sqlalchemy/engine/base.py", line 2338 in _wrap_pool_connect
process:   File "/opt/conda/lib/python3.7/site-packages/sqlalchemy/engine/threadlocal.py", line 76 in _contextual_connect
process:   File "/opt/conda/lib/python3.7/site-packages/sqlalchemy/engine/base.py", line 2088 in _optional_conn_ctx_manager
process:   File "/opt/conda/lib/python3.7/contextlib.py", line 112 in __enter__
process:   File "/opt/conda/lib/python3.7/site-packages/sqlalchemy/engine/base.py", line 2096 in _run_visitor
process:   File "/opt/conda/lib/python3.7/site-packages/sqlalchemy/sql/schema.py", line 4556 in create_all
process:   File "/opt/codalab-worksheets/codalab/model/bundle_model.py", line 120 in create_tables
process:   File "/opt/codalab-worksheets/codalab/model/bundle_model.py", line 92 in __init__
process:   File "/opt/codalab-worksheets/codalab/model/mysql_model.py", line 54 in __init__
process:   File "/opt/codalab-worksheets/codalab/lib/codalab_manager.py", line 359 in model
process:   File "scripts/create-root-user.py", line 14 in <module>
process: 139
```





/home/ubuntu/environment/codalab/codalab-worksheets/venv/lib/python3.8/site-packages/ratarmountcore/SQLiteIndexedTar.py

/home/ubuntu/environment/codalab/codalab-worksheets/venv/lib/python3.8/site-packages/ratarmountcore/compressions.py



codalab-service start -s azurite
CODALAB_DEFAULT_BUNDLE_STORE_NAME=azure-store-default codalab-service start -s default azurite
sh ./tests/test-setup-default-store.sh
codalab-service start -bs rest-server && python test_runner.py make

cl uedit codalab -d 8m
codalab-service start -bs rest-server && cl upload  --store azure-store-default venv/lib/python3.8/site-packages/botocore


CODALAB_DEFAULT_BUNDLE_STORE_NAME=azure-store-default codalab-service start -bs rest-server && python test_runner.py make

cl make $(cl upload -c "hello")

cl make 0xe7a19c5b2f074b2c9582333f40febee8 --store test

----

k8s test

codalab-service build --pull
VERSION=kstats sh ./scripts/local-k8s/setup-ci.sh
codalab-service build -s worker && kind load docker-image "codalab/worker:kstats" --name codalab
export CODALAB_SERVER=http://nginx
export CODALAB_WORKER_MANAGER_CPU_BUNDLE_RUNTIME=kubernetes
export CODALAB_WORKER_MANAGER_CPU_KUBERNETES_CLUSTER_HOST=https://codalab-control-plane:6443
export CODALAB_WORKER_MANAGER_TYPE=kubernetes
export CODALAB_WORKER_MANAGER_CPU_KUBERNETES_CERT_PATH=/dev/null
export CODALAB_WORKER_MANAGER_CPU_KUBERNETES_AUTH_TOKEN=/dev/null
export CODALAB_WORKER_MANAGER_CPU_DEFAULT_CPUS=1
export CODALAB_WORKER_MANAGER_CPU_DEFAULT_MEMORY_MB=100
export CODALAB_WORKER_MANAGER_MIN_CPU_WORKERS=0
export CODALAB_WORKER_MANAGER_MAX_CPU_WORKERS=1
codalab-service start --services worker-manager-cpu

python3 test_runner.py resources

kubectl exec --stdin --tty cl-worker-f628165974b1456ba73c2d1e6408ab7a -- /bin/bash