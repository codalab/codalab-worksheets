# Worker Managers

[Worker Managers](https://github.com/codalab/codalab-worksheets/blob/master/codalab/worker_manager/main.py) 
allow you to automatically spin up VMs and start CodaLab workers on them to run your staged jobs.

We support the following Worker Managers:

| Name             | Description                                                  |
|------------------|--------------------------------------------------------------|
| aws-batch        | Worker manager for submitting jobs to AWS Batch.             | 
| azure-batch      | Worker manager for submitting jobs to Azure Batch.           | 
| slurm-batch      | Worker manager for submitting jobs using Slurm Batch.        | 
| kubernetes       | Worker manager for submitting jobs to a Kubernetes cluster.  |


## AWS Batch Worker Manager

### Configure AWS Batch (one-time setup)

1. Authenticate AWS on the command-line:
    1. Install the CLI: `pip install awscli`.
    1. Authenticate by running `aws configure` and fill out the form.
1. Create a [launch template](https://docs.aws.amazon.com/batch/latest/userguide/launch-templates.html) 
   for EC2 instances by running:
   
   ```commandline
    aws ec2 --region <region> create-launch-template --cli-input-json file://lt.json
   ```
    
   Your launch template `lt.json` should look something like this:

   ```json
    {
        "LaunchTemplateName": "increase-root-volume",
        "LaunchTemplateData": {
            "BlockDeviceMappings": [
                {
                    "DeviceName": "/dev/xvda",
                    "Ebs": {
                        "Encrypted": true,
                        "VolumeSize": <Desired volume size in GB as an integer>,
                        "VolumeType": "gp2"
                    }
                }
            ]
        }
    }
   ```
   
1. Log on to the [AWS console](https://aws.amazon.com/console). 
1. In the upper right corner, select the region.
1. Type `Batch` in the search bar and click `Batch` under `Services`.
1. Create a compute environment:
    1. Click `Compute environments` and then `Create`.
    1. Specify a name for `Compute Environment Name`.
    1. Under `Instance Configuration`, select `On-Demand` or `Spot`.
    1. Specify the [type of EC2 instances](https://aws.amazon.com/ec2/instance-types/) under the 
       `Allowed Instance Types` dropdown menu.
    1. Under `Additional Settings`, select the launch template you created.
    1. Click `Create compute environment`.
1. Configure a job queue:
    1. Click `Job queues` and then `Create`.
    1. Give your job queue a name.
    1. Under `Connected compute environments`, select the compute environment from the previous step.
    1. Click `Create`.
1. Wait for the job queue and compute environment to have a status of `VALID`.

### Start a AWS Batch Worker Manager

Use the [AWS Batch Worker Manager](https://github.com/codalab/codalab-worksheets/blob/master/codalab/worker_manager/aws_batch_worker_manager.py),
to start the worker manager. Pass in the name of the job queue for `--job-queue`.

## Azure Batch Worker Manager

### Configure Azure Batch (one-time setup)

1. Log on to the Azure portal using your credentials.

1. Go to the Batch account where you want to start your workers. If you don't have a Batch account
create one through the Azure portal.
   
1. Click `Keys` and take note of `Batch account`, `URL`, and `Primary access key` as you will need this information
to start the worker manager.

1. Next create and configure a Batch Pool and a Batch Job. 

#### How to create a Batch Pool

1. Go to `Pools` and select `Add`.

1. Under `Pool Detail`:
    1. For `Pool ID`, give your Pool a unique name.
    1. Skip `Display Name`.

1. Under `Operating System`:
    1. Keep `Image Type` as `Marketplace`.
    1. For `Publisher`, select `microsoft-azure-batch`.
    1. For `Offer`, select `ubuntu-server-container`.
    1. For `Sku`, select `20-04-lts`.
    1. Toggle `Container configuration` to `Custom`.
    1. Make sure `Container type` is `Docker compatible`.

1. Under `Node Size`, select the appropriate VM size. If you want a CPU-only pool
you would select `Standard D3_v2 (4 vCPUs, 14 GB Memory)` for example. For a gpu pool, select
`Standard NC6 (6 vCPUs, 56 GB Memory)` for example.

1. Under `Scale`:
    1. Toggle `Mode` to `Auto scale`.
    1. Set `AutoScale Evaluation Interval` to an appropriate time. This controls how often the pool autoscales.
    1. For `Formula`, create your custom autoscale formula based on your compute needs. The following is an
       example:
       
   ```text
   // The pool size is adjusted based on the number of tasks in the queue
   // The variables prepended with '$' in this formula are Azure service-defined variables
   
   // Adjust the min and max number of VMs accordingly
   minNumberOfVMs = 1;
   maxNumberOfVMs = 15;
   
    // Samples are obtained every 30 seconds over a 5 minute interval
   pendingTaskSamplePercent = $PendingTasks.GetSamplePercent(5 * TimeInterval_Minute);
   
   // If we have more than 50 percent data points, we use the history average of number of tasks.
   // It is bad practice to simply use the last sample, as it can be stale and not indicative of the current situation.
   pendingTaskSamples = pendingTaskSamplePercent < 50 ? minNumberOfVMs : avg($PendingTasks.GetSample(5 * TimeInterval_Minute));
   pendingTaskSamples = max(pendingTaskSamples, minNumberOfVMs);
   
   $TargetDedicatedNodes=min(pendingTaskSamples, maxNumberOfVMs);
   
   // Set node deallocation mode - keep nodes active only until tasks finish
   $NodeDeallocationOption = taskcompletion;
   ```

1. Create the Batch Pool by clicking OK.

1. Select `Pools` and ensure that the Batch Pool you just created shows up on the page.

#### How to create a Batch Job

1. Go to `Jobs` and select `Add`.

1. For `Job ID`, give your Job a unique ID. For example, you can give it name with the format 
   `{environment}-{resource type}`, where `environment` is either `prod` or `dev` and resource type 
   is either `gpu` or `cpu` (e.g. `prod-cpu`).

1. For `Pool`, select the corresponding Batch Pool.

1. Create the Batch Job by clicking OK.

1. Select `Jobs` and ensure that your Batch Job shows up on the page.

### Start a Azure Batch Worker Manager

Use the [Azure Batch Worker Manager](https://github.com/codalab/codalab-worksheets/blob/master/codalab/worker_manager/azure_batch_worker_manager.py#L44)
to start the worker manager by passing in `azure-batch` for the 
[type of worker manager](https://github.com/codalab/codalab-worksheets/blob/master/codalab/worker_manager/main.py#L117).

Below is an example of how to start a worker manager:

```commandline
cl-worker-manager --server https://worksheets.codalab.org --min-workers 0 --max-workers 8 
--min-seconds-between-workers 300 --sleep-time 120 --worker-pass-down-termination 
--worker-exit-on-exception --worker-exit-after-num-runs 1 azure-batch  
--account-name <Azure Batch account name> --account-key <Azure Batch account key> 
--service-url <Azure Batch service URL>  --log-container-url <URL of the Azure Storage container to store the worker logs>  
--job-id <Name of the Batch Job> --cpus <Number of CPUs on VM> --gpus <Number of GPUS on VM>  --memory-mb <Amount of memory on VM in MB>
```

### Checking worker logs in Azure

##### For a running bundle

1. Go to the bundle view page of the bundle and get the worker ID from the `remote` field.
The remote field is in form `<hostname>-<worker ID>`.

1. Login to `portal.azure.com` and go to your Batch account.
2. Under `Features`, select `Jobs`.
3. Select the Batch Job your worker was running on.
4. Search for the task by typing `cl_worker_<worker ID>` into the search bar.
5. Open the task for the log files of the running worker.


##### For a failed bundle

1. Go to the bundle view page of the bundle and get the worker ID from the `remote` field.
The remote field is in form `<hostname>-<worker ID>`.

1. Login to `portal.azure.com` and go to the storage account.
2. Under `Blob service`, select `Containers`.
3. Select the Batch Job your worker was running on.
4. Search for the blob by typing `cl_worker_<worker ID>` into the search bar.
5. Open the blob and select `Edit` to view the logs in the browser. Select `Download` to
download the file.

### Force kill an Azure Batch worker

Sometimes, if a bundle cannot be killed, you may want to force kill the Azure Batch worker. 
Note: this will kill all other bundles that are running on this worker, so only do this if you absolutely need to 
(if the bundle cannot be stopped otherwise). To do so,

1. Follow the steps in the previous section to get the worker ID of the running bundle, then navigate to 
   the corresponding task on the Azure Console.
2. Click "Terminate" to terminate the worker.
3. Look through the logs, if useful, and file an issue related to the problem that this particular worker was having.

## Kubernetes Batch Worker Manager

### Configure GKE (one-time setup)

#### Setting up gcloud and kubectl

1. The Cloud SDK is needed to manage GKE clusters. Follow
   [these instructions](https://cloud.google.com/sdk/docs/install) to install the Cloud SDK and GKE.
2. Login to gcloud by running: `gcloud auth login`.
3. Set the project by running `gcloud config set project hai-gcp-natural-language`.
4. Install kubectl by running `gcloud components install kubectl`.

#### Creating a cluster <a name="gke"></a>

The CodaLab Kubernetes worker manager creates pods in the GKE cluster to run jobs.
Create a cluster by following these documentations:

- [Quickstart guide](https://cloud.google.com/kubernetes-engine/docs/quickstart)
- [How to create a GKE cluster](https://cloud.google.com/sdk/gcloud/reference/beta/container/clusters/create)

Here are some additional links to help determine the parameter values of  
`gcloud container clusters create`:

- [Types of VMs](https://cloud.google.com/compute/docs/machine-types)
- [Types of GPUs](https://cloud.google.com/compute/docs/gpus)
- [GPU availability by region](https://cloud.google.com/compute/docs/gpus/gpu-regions-zones)

We will create two pools when starting a GKE cluster:

- The default pool comprises a single E2-standard machine that runs essential, non-GPU jobs 
  (e.g., running the NFS-server). 
- The GPU pool runs CodaLab workers.

Creating separate pools for GPU vs. non-GPU jobs allows the GPU pool to scale down to 0 
when there aren't any running CodaLab jobs.

Below is an example of how to create a GKE cluster with a separate GPU pool:

```commandline    
gcloud container clusters create codalab-worker-manager-cluster  \
    --zone us-west1-a \
    --machine-type e2-standard-4 \
    --disk-type=pd-ssd \
    --disk-size 100GB \
    --num-nodes 1  \
    --image-type UBUNTU \
    --scopes=cloud-platform,gke-default \

gcloud beta container node-pools create gpu-pool \
    --cluster codalab-worker-manager-cluster \
    --zone us-west1-a \
    --machine-type n1-standard-8 \
    --disk-type=pd-ssd \
    --disk-size 256GB \
    --num-nodes 0  \
    --min-nodes 0  \
    --max-nodes 8  \
    --enable-autoscaling \
    --image-type UBUNTU \
    --accelerator type=nvidia-tesla-p100,count=1  \
    --scopes=cloud-platform,gke-default \
    --spot
```

The commands above will create an auto-scaling cluster in `us-west1-a` with no
n1-standard machine at initialization with the option to auto-scale up to 8 
[Spot nodes](https://cloud.google.com/spot-vms) with P100 GPUs.
By not specifying a cluster version with the `--cluster-version` argument, GCP will create a cluster 
with the default version in the
[Stable channel](https://cloud.google.com/kubernetes-engine/docs/release-notes-stable).

Setting `--scopes=cloud-platform,gke-default` is required to configure Nvidia drivers and dependencies for the nodes.
Also, note that only 74% of memory is available to a CodaLab worker on the VM 
(see [the following documentation](https://learnk8s.io/allocatable-resources) for more information).

Next, run `yes Y | gcloud beta container clusters update codalab-worker-manager-cluster 
--autoscaling-profile optimize-utilization --region us-west1-a` to ensure that
[the cluster scales down more aggressively](https://cloud.google.com/kubernetes-engine/docs/concepts/cluster-autoscaler).


#### Deleting a GKE cluster

To delete a cluster, simply run:

`yes Y | gcloud container clusters delete  codalab-worker-manager-cluster --region us-west1-a`


#### Managing a GKE cluster

Use kubectl to manage the cluster and pods. Here is a list of common kubectl commands:

```commandline
kubectl describe nodes
kubectl describe pods
kubectl delete pods <pod>
kubectl get pods
kubectl get pods -A
kubectl logs <pod> -c <container>
ubectl get configmap cluster-autoscaler-status -n kube-system -o yaml
```

You can also manage a GKE cluster in the GCP console. To manage the GKE cluster in the console, go to the
[GCP Kubernetes console](https://console.cloud.google.com/kubernetes).


#### Installing Nvidia Driver and Dependencies <a name="gkenvidia"></a>

Nvidia drivers and the nvidia-container-runtime tool by default are not installed in most GCP virtual machines. 
Therefore, we need to create node initializers that download and install these dependencies on
existing nodes and future nodes that are brought up by auto-scaling.

Additionally, [the NVIDIA Device Plugin](https://github.com/NVIDIA/k8s-device-plugin) is required to expose the 
GPUs of the nodes in your cluster. The NVIDIA Device Plugin is a daemonset that automatically enumerates the 
number of GPUs on each node of the cluster and allows pods to be run on GPUs.

For more information on bootstrapping GKE nodes with DaemonSets,
see the following
[documentation](https://cloud.google.com/solutions/automatically-bootstrapping-gke-nodes-with-daemonsets).

To set this up:

1. Go to the `gcp` directory of this repository: `cd docs/gcp`.
2. Run `kubectl apply -f cm-entrypoint.yaml && kubectl apply -f daemon-set.yaml`.
3. Create the Nvidia Device Plugin by running `kubectl create -f nvidia-device-plugin.yaml`.   
4. To verify that the Nvidia drivers are installed correctly
   1. Go to the [GCP Compute Engine console](https://console.cloud.google.com/compute/instances). 
   2. Find a virtual machine with a GPU that belongs to your GKE cluster and connect to the
      machine by clicking the `SSH` button. 
   3. In the terminal session, run `sudo nvidia-smi` to see if the driver can communicate with the GPU. 
   4. Run `sudo docker run --runtime=nvidia --rm nvidia/cuda:11.0-base nvidia-smi`
      and check that the output is the same as the output in the previous step.


#### Setting up a Network File System (NFS) server <a name="gkenfs"></a>

Optionally, you can attach additional storage by creating a NFS server.

To set this up:

1. Create a compute disk named `pd` in GCP by running: 
   `gcloud compute disks create --size=<Size of disk in GB>GB --zone=us-west1-a --type pd-ssd pd`.
2. Go to the `gcp/nfs` directory of this repository: `cd gcp/nfs`.
3. Run `kubectl apply -f nfs-server.yaml && kubectl apply -f nfs-service.yaml && kubectl get svc nfs-server`.
   This will output the IP address of the cluster.
4. Update `nfs-pv.yaml` with the IP address from step 3.
5. Run `kubectl apply -f nfs-pv.yaml`.


#### Authentication and setting up a service account <a name="gkeauthenticate"></a>

A GCP service account and cluster certificate are required to authenticate and run Kubernetes commands 
through the worker manager.

To create a service account:

1. Run `kubectl create -f service-account.yaml --namespace default`
2. Then, run `kubectl get secrets --namespace default`
3. Get the auth token by first find the name of the secret (in the form `codalab-token-<random string>`) and 
   then use the name to get the token by running: `kubectl describe secret/codalab-token-<random string>`.
   
To get the cluster certificate:

1. Go to [GKE console](https://console.cloud.google.com/kubernetes/list) and click on the newly created 
   GKE cluster.
2. Under `Cluster Basics`, find the `Endpoint` field.
3. Take note of the endpoint URL as this needed to start the worker manager.
4. Next, click on `Show Cluster Certificate` and copy the entire contents and place it in a file called `gke.crt`.

### Start a Kubernetes Batch Worker Manager

Use the [Kubernetes Batch Worker Manager](https://github.com/codalab/codalab-worksheets/blob/master/codalab/worker_manager/kubernetes_worker_manager.py#L31)
to start the worker manager by passing in `kubernetes` for the 
[type of worker manager](https://github.com/codalab/codalab-worksheets/blob/master/codalab/worker_manager/main.py#L117).

At this point, four things are required to start a Kubernetes worker manager:

- [A running Kubernetes cluster with Nvidia drivers installed](#gke)
- [An auth token](#gkeauthenticate)
- [Path to the GKE cluster certificate](#gkeauthenticate)
- [Endpoint URL of the cluster host](#gkeauthenticate)

You can start a Kubernetes worker manager manually, by using the `cl-worker-manager` command. 
Below is an example of how to start a worker manager:

```commandline
cl-worker-manager --server https://worksheets.codalab.org --min-workers 0 --max-workers 8 
--min-seconds-between-workers 300 --sleep-time 120 --worker-pass-down-termination 
--worker-exit-on-exception --worker-exit-after-num-runs 1  kubernetes  
--cert-path <Path to gke.crt> --auth-token <Auth token> --cluster-host <Endpoint URL of cluster host>
--cpus <Number of CPUs on VM> --gpus <Number of GPUS on VM>  --memory-mb <Amount of memory on VM in MB>
```

### Checking worker logs in GCP

1. Go to the bundle view page of the bundle and get the worker ID from the `remote` field.
2. Go to  [GKE console](https://console.cloud.google.com/kubernetes/workload).
3. In the `Cluster` dropdown menu, specify the GKE cluster the worker is running on.
4. Click on the pod with the name `cl-worker-<ID of worker from step 1> `. 
5. Click on the `Logs` tab to view the worker logs.
