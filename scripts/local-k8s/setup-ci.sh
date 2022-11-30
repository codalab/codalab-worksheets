# Setup local Kubernetes for CI tests
set -e

# First, start codalab without a worker:
python3 codalab_service.py start --services default no-worker --version ${VERSION}

# Install initial dependencies
go install sigs.k8s.io/kind@v0.12.0
go install github.com/cloudflare/cfssl/cmd/...@latest
kind version # kind should be installed
cfssl version # cfssl should be installed

# Set up local kind cluster.
./scripts/local-k8s/setup.sh

# Run worker manager
export CODALAB_SERVER=http://nginx
export CODALAB_WORKER_MANAGER_CPU_KUBERNETES_CLUSTER_HOST=https://codalab-control-plane:6443
export CODALAB_WORKER_MANAGER_TYPE=kubernetes
export CODALAB_WORKER_MANAGER_CPU_KUBERNETES_CERT_PATH=/dev/null
export CODALAB_WORKER_MANAGER_CPU_KUBERNETES_AUTH_TOKEN=/dev/null
export CODALAB_WORKER_MANAGER_CPU_DEFAULT_CPUS=1
export CODALAB_WORKER_MANAGER_CPU_DEFAULT_MEMORY_MB=100
export CODALAB_WORKER_MANAGER_MIN_CPU_WORKERS=0
export CODALAB_WORKER_MANAGER_MAX_CPU_WORKERS=1
python3 codalab_service.py start --services worker-manager-cpu --version ${VERSION}
