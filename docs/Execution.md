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
doc](worker-design.pdf) for more detailed information (this document
is a bit outdated).

## Specifying Environments with Docker

CodaLab uses Docker containers to define the
environment of a run bundle. Each Docker container is based on a Docker image,
which specifies the full environment, including which Linux kernel
version, which libraries, etc.

The default Docker image is `codalab/default-cpu` and `codalab/default-gpu`, which consists of
Ubuntu 16.04 plus some standard packages (e.g., Python, Ruby, R, Java, Scala, g++, Tensorflow, Pytorch).
See the
[Dockerfile](https://github.com/codalab/codalab-worksheets/blob/master/docker/dockerfiles/Dockerfile.default-cpu)
for the complete list of packages installed.

In general, when you create a run, you can specify which Docker container you want to use.

    cl run <command> --request-docker-image codalab/default-cpu

To see what Docker images are available, you can do a search on [Docker
Hub](https://hub.docker.com). If nothing satisfies your needs, you can
[install Docker](https://docs.docker.com/install/) and create your own image
using a `Dockerfile`.

## Running jobs that use GPUs

CodaLab has publicly available GPUs! To use them, you'll need to 1) include the
`--request-gpus` flag, and 2) specify a Docker image that has `nvidia-smi` installed using the `--request-docker-image` flag. For example:

    cl run --request-docker-image nvidia/cuda:8.0-runtime --request-gpus 1 "nvidia-smi"

If no Docker image is specified, `codalab/default-gpu` will be used.

## Default workers

On the `worksheets.codalab.org` CodaLab server, the workers are running on Microsoft
Azure.  Currently, each non-GPU machine has 4 cores and 14 GB of memory, and
each GPU machine has 6 cores and 56 GB of memory (but this
is subject to change).  You can always find out the exact specs by executing the command:

    cl run 'cat /proc/cpuinfo; free; df'

## Running your own worker

If the default workers are full or do not satisfy your needs, one of the advantages of the CodaLab worker system is that you can run a worker on your own machines.

### Setup Instructions

**Step 0**. Install the CodaLab CLI (`pip install codalab`).

**Step 1**. Install Docker, which will be used to run your bundles in an isolated environment. Currently, to use GPUs in workers, CodaLab requires a version of Docker < 19.03 .

**Step 2**. Start the worker, which will prompt you for your username and password:

    cl-worker --verbose

**Step 3**. To test your worker, simply start any run:

    cl run date

You should see that the run finished, and if you look at the `remote` metadata field, you should see your hostname.

**Sharing a worker with a group** 

You can also share your worker with other users within a group. When shared, your worker will pick up staged bundles that belong
to you or your group members, so long as you have admin permissions for the bundle.
  
To share a worker, simply start a worker with a valid group name specified: 

    cl-worker --group <group name>
    
**Controlling where runs happen with tags**.

You can tag workers and run jobs on workers with those tags.  To tag a worker, start the worker as follows:

    cl-worker --tag <worker_tag>

To run a job, simply pass the tag in:

    cl run date --request-queue <worker_tag>

**Other flags**. Run `cl-worker --help` for information on all the supported flags. Aside
from the `--server`, other important flags include `--work-dir`
specifying where to store intermediate data and `--cpuset` and `--gpuset`
controlling which CPUs and GPUs the system has access to.

### Setting up workers to use GPUs

If your machine has GPUs and would like to hook them up to CodaLab, then follow these instructions.

**Step 0**: Complete the worker setup instructions in the previous section. Make sure that your version of Docker is < 19.03 .

**Step 1**: Check that the appropriate drivers are installed by running `nvidia-smi` on your machine. Check for an output similar to this one:

    Thu May 25 09:39:22 2017       
    +-----------------------------------------------------------------------------+
    | NVIDIA-SMI 375.51                 Driver Version: 375.51                    |
    ...

If you have not installed the drivers, here are some links that may help:

* For [Azure N Series GPUs](https://docs.microsoft.com/en-us/azure/virtual-machines/linux/n-series-driver-setup)
* For [AWS P2 GPUs](https://aws.amazon.com/blogs/aws/new-p2-instance-type-for-amazon-ec2-up-to-16-gpus/2)
* For [Google Cloud GPUs](https://cloud.google.com/compute/docs/gpus/add-gpus)

**Step 2**: For Debian/Ubuntu users, install `nvidia-docker` for your version of Docker. For instance, on Ubuntu, the following commands install Docker 18.03.1 and the appropriate version of `nvidia-docker`:

    sudo apt-get install docker-ce=18.03.1~ce-0~ubuntu
    sudo apt-get install nvidia-docker2=2.0.3+docker18.03.1-1 nvidia-container-runtime=2.0.0+docker18.03.1-1
    sudo systemctl daemon-reload
    sudo systemctl restart docker

**Step 3**: Test your setup by checking if Docker can find GPUs. Run:

    sudo docker run --runtime=nvidia --rm nvidia/cuda:8.0-runtime nvidia-smi
    
You should see something similar to before:

    Wed May 24 19:03:55 2017``
    +-----------------------------------------------------------------------------+
    | NVIDIA-SMI 367.48                 Driver Version: 367.48                    |
    ...

**Step 4**: Run this command, which tests that `nvidia-smi` is working inside of Docker through CodaLab:

    cl run --request-docker-image nvidia/cuda:8.0-runtime --request-gpus 1 "nvidia-smi"

Check the bundle's `stdout`, and you should see something similar to before:

    Wed May 24 19:03:55 2017``
    +-----------------------------------------------------------------------------+
    | NVIDIA-SMI 367.48                 Driver Version: 367.48                    |
    ...

And that's all.  Congrats!

## Frequently asked questions

### Can I reserve a worker for a particular tag?

Yes, you can use the `--tag-exclusive` flag of `cl worker` (along with a
`--tag`) to prevent your worker from running untagged bundles, since
tag-exclusive workers only run bundles that match their tag.

To be concrete, a worker started with `cl worker --tag debug --tag-exclusive`
will _only_ run bundles with `--request-queue debug`. This is especially
useful for development, since it lets you set off a machine for fast-turnaround
debugging of CodaLab bundles, even if you have many untagged jobs queued---just
make sure to set `--request-queue debug`.
