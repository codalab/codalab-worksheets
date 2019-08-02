This page describes how CodaLab executes run bundles and
manages the environment and hardware of those executions.

## Overview: How the worker system works

CodaLab's distributed worker system
executes the run bundles in CodaLab. To begin, a worker machine connects to 
the CodaLab server and asks for run bundles to run. The CodaLab server finds 
a run that hasn't been executed yet, and assigns the worker to it. 
The worker then downloads (if not downloaded already) all the relevant
bundle dependencies from the CodaLab server and the
Docker image from Docker Hub.

Once the worker has all of the dependencies installed, the worker then
executes the run in the Docker container, sending back status updates to the
CodaLab server (e.g., memory usage, etc.), and sees if there are any requests
to kill the run bundle. Any requests to download files in the bundle are forwarded
from the CodaLab server to the worker. At the end of the run, the worker sends
back all the bundle contents. See the [worker system design
doc](worker-design.pdf) for more detailed information.

## Specifying Environments with Docker

CodaLab uses Docker containers to define the 
environment of a run bundle. Each Docker container is based on a Docker image,
which specifies the full environment, including which Linux kernel
version, which libraries, etc.

The default Docker image is `codalab/ubuntu:1.9`, which consists of
Ubuntu 14.04 plus some standard packages (e.g., Python, Ruby, R, Java, Scala, g++).
See the entry in the [CodaLab Docker
registery](https://registry.hub.docker.com/u/codalab/ubuntu/) for more
information.

In general, when you create a run, you can specify which Docker container you want to use.

    cl run <command> --request-docker-image codalab/ubuntu:1.9

To see what Docker images are available, you can do a search on [Docker
hub](https://hub.docker.com). If nothing satisfies your needs, you can
[install Docker](Installing-Docker) and [create your own
image](Creating-Docker-Images). If you're creating a Docker image in Python,
we recommend using the [Codalab Python](https://hub.docker.com/r/codalab/python/)
image as your base image because it comes pre-installed with `python` and `pip`.

Here are some other commonly used docker images with machine learning libraries:

- TensorFlow:

        cl run 'python -c "import tensorflow"' --request-docker-image tensorflow/tensorflow:0.8.0

- Theano: [![](https://images.microbadger.com/badges/image/codalab/ubuntu.svg)](https://microbadger.com/images/codalab/ubuntu "Get your own image badge on microbadger.com")

        # Defaults to standard CodaLab Ubuntu image (codalab/ubuntu:1.9)
        cl run 'python -c "import theano"' 

- Torch: [![](https://images.microbadger.com/badges/image/codalab/torch.svg)](https://microbadger.com/images/codalab/torch "Get your own image badge on microbadger.com")

        cl run 'th' --request-docker-image codalab/torch:1.1

## Running jobs that use GPUs

CodaLab has publicly available GPUs! To use them, you'll need to 1) include the 
`--request-gpus` flag, and 2) specify a Docker image that has `nvidia-smi` installed using the `--request-docker-image` flag. For example:

    cl run --request-docker-image nvidia/cuda:8.0-runtime --request-gpus 1 "nvidia-smi"

And that's all it takes!

### GPU Docker images

* Tensorflow GPU users: check out the [official Tensorflow GPU Docker image](https://hub.docker.com/r/tensorflow/tensorflow/). For example:

        cl run 'python -c "import tensorflow"' --request-docker-image tensorflow/tensorflow:0.8.0-gpu --request-gpus 1

* We have instructions for [creating your own Docker image with GPU support](https://github.com/codalab/codalab-worksheets/wiki/Creating-Docker-Images#building-docker-images-with-cuda-support), though we would recommend searching on [Docker Hub](dockerhub.com) first before creating your own image.

## Default workers

On the `worksheets.codalab.org` CodaLab server, the workers are running on Microsoft
Azure.  Currently, each non-GPU machine has 4 cores and 14 GB of memory, and 
each GPU machine has 6 cores and 56 GB of memory (but this
is subject to change).  You can always find out the exact specs by executing the command:

    cl run 'cat /proc/cpuinfo; free; df'

## Running your own worker

If the default workers are full or do not satisfy your needs, one of the advantages of the CodaLab worker system is that you can run a worker on your own machines.

### Setup Instructions

**Step 0**. [Install the CLI](https://github.com/codalab/codalab-worksheets/wiki/CLI-Basics).

**Step 1**. [Install Docker](https://github.com/codalab/codalab-worksheets/wiki/Installing-Docker), which will be used to run your bundles in an isolated environment. 

**Step 2**. Start the worker:

    cl-worker

**Step 3**. To test your worker, simply start any run:

    cl run date

You should see that the run finished, and if you look at the `remote` metadata field (e.g., via `cl info -f remote ^` on the CLI or on the side panel in the web interface), you should see your hostname.

Note that only your runs will be run on your workers, so you don't have to worry about interference with other users.

**Controlling where runs happen with tags**.

You can tag workers and run jobs on workers with those tags.  To tag a worker, start the worker as follows:

    cl-worker --server https://worksheets.codalab.org --tag <worker_tag> 

To run a job, simply pass the tag in:

    cl run date --request-queue tag=<worker_tag>

**Other flags**. Run `cl-worker --help` for information on all the supported flags. Aside
from the `--server`, other important flags include `--work-dir`
specifying where to store intermediate data and `--cpuset` and `--gpuset` controlling which CPUs and GPUs the system has access to.

### Setting up workers to use GPUs

If your machine has GPUs and would like to hook them up to CodaLab, then follow these instructions.

**Step 0**: Complete the worker setup instructions in the previous section.

**Step 1**: Check that the appropriate drivers are installed by running `nvidia-smi` on your machine. Check for an output similar to this one:

    Thu May 25 09:39:22 2017       
    +-----------------------------------------------------------------------------+
    | NVIDIA-SMI 375.51                 Driver Version: 375.51                    |
    ...

If you have not installed the drivers, here are some links that may help:

* For [Azure N Series GPUs](https://docs.microsoft.com/en-us/azure/virtual-machines/linux/n-series-driver-setup)
* For [AWS P2 GPUs](https://aws.amazon.com/blogs/aws/new-p2-instance-type-for-amazon-ec2-up-to-16-gpus/2)
* For [Google Cloud GPUs](https://cloud.google.com/compute/docs/gpus/add-gpus)

**Step 2**: For Debian/Ubuntu users, install `nvidia-docker` by following the instructions 
[here](https://github.com/NVIDIA/nvidia-docker).

**Step 3**: Test your setup. Run:

    sudo nvidia-docker run --rm nvidia/cuda nvidia-smi

**Step 4**: Run this command, which tests that nvidia-smi is working inside of Docker:

    cl run --request-docker-image nvidia/cuda:8.0-runtime --request-gpus 1 "nvidia-smi"

Check the bundle's `stdout`, and you should see something similar to before:

    Wed May 24 19:03:55 2017``
    +-----------------------------------------------------------------------------+
    | NVIDIA-SMI 367.48                 Driver Version: 367.48                    |
    ...

And that's all.  Congrats!