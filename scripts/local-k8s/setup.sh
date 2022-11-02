# Sets up a local kubernetes cluster.

set -e

kubectl get nodes -o=name | sed "s/^node\///" | xargs -L1 docker network connect rest-server # connects all kind nodes (which are Docker containers) to codalab docker network, so they can communicate.
kubectl apply -f scripts/local-k8s/anonymous-users.yaml # gives anonymous users access to the local k8s cluster. Worker managers currently use anonymous authentication to access local k8s clusters.
