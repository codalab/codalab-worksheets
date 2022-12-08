
## Start a local Kubernetes Batch Worker Manager (with kind, for testing / development only)

If you want to test or develop with kubernetes locally, follow these steps to do so:

### Starting a new cluster

First, install `go` locally.

Then, if a cluster already exists, delete it:

```bash
kind delete cluster --name codalab
```

Build CodaLab images locally:

```bash
python3 codalab_service.py build
```

Then start up a new cluster:

```bash
DEV=1 VERSION=$(python3 codalab_service.py version) sh ./scripts/local-k8s/setup-ci.sh
```

### Setting up web dashboard
Here is how to set up the web dashboard for your local cluster:

```bash
kubectl config use-context kind-codalab # makes sure kubectl is connected to local cluster
kubectl -n kubernetes-dashboard get secret $(kubectl -n kubernetes-dashboard get sa/admin-user -o jsonpath="{.secrets[0].name}") -o go-template="{{.data.token | base64decode}}" # copy this token and use it for web ui auth in the next step
# To view the dashboard, run \"kubectl proxy\" in a terminal and open up: http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/#/workloads?namespace=default"
```

If all is successful, you should be able to log into your dashboard. You should have one node running (codalab-control-plane). After you follow the steps below, you should also be able to view each pod (which corresponds to each worker) and then check their logs by clicking on the icon in the top-right.

![Local Kubernetes Dashboard](./images/local-k8s-dashboard.png)

### Build worker docker image

You should repeat this step each time you change the worker docker image and want the local kind cluster to load it:

```bash
codalab-service build -s worker && kind load docker-image "codalab/worker:$(python3 codalab_service.py version --version $VERSION)" --name codalab
```

### Teardown

You can remove the kind cluster by running:

```
kind delete cluster --name codalab
```
