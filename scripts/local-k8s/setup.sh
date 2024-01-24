# Sets up a local kubernetes cluster.

set -e

docker container prune -f # remove all stopped containers
kind create cluster --wait 30s --config scripts/local-k8s/kind-config.yaml # create cluster
kubectl config use-context kind-codalab # makes sure kubectl is connected to local cluster
kubectl get nodes -o=name | sed "s/^node\///" | xargs -L1 docker network connect rest-server # connects all kind nodes (which are Docker containers) to codalab docker network, so they can communicate.
kubectl apply -f scripts/local-k8s/anonymous-users.yaml # gives anonymous users access to the local k8s cluster. Worker managers currently use anonymous authentication to access local k8s clusters.
kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.5.0/aio/deploy/recommended.yaml # create web ui dashboard. full instructions from tutorial here: https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/
kubectl apply -f scripts/local-k8s/dashboard-user.yaml # create dashboard user