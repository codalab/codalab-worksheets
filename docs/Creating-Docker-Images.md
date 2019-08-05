Bundles are run inside [Docker](https://www.docker.com) containers. Containers provide an isolated Linux environment for your code containing various libraries and software packages. CodaLab uses this [Ubuntu Linux 16.04 image for bundles that do not require a GPU](https://hub.docker.com/r/codalab/default-cpu/) and [this GPU-enabled image for bundles that do](https://hub.docker.com/r/codalab/default-gpu/) by default. [You can check out the dockerfiles used to create these images here](https://github.com/codalab/codalab-cli/tree/master/docker). You can specify which image to use when you are creating a run bundle using the `--request-docker-image <image>` flag. If the default image doesn't have the package you need, your options are:

1. Find an image that someone else has built. Package maintainers often release Docker images containing their packages.
[DockerHub](https://hub.docker.com) is a good place to look.

2. Build your own image and upload it to Docker Hub. Instructions are given below.

## Building your own images
Detailed instructions for building images are available on the Docker website [here](https://docs.docker.com/engine/userguide/containers/dockerimages/). In the spirit of reproducibility we recommend building images using a Dockerfile so that how the image is built is documented. The steps are as follows:

1. [Download and install Docker](Installing-Docker.md).
2. Create a directory for your image and `cd` into it. Then start editing a file with the name `Dockerfile`.

    ```
    mkdir myimage
    cd myimage
    vim Dockerfile
    ```

3. Your image will contain everything from a base image that you will add to by running Linux commands. Good images to start from include `ubuntu:14.04`, `codalab/ubuntu:1.9` and `nvidia/cuda:7.5-cudnn4-devel` (which sets up NVIDIA CUDA in a way compatible to CodaLab, more below). Specify the image in the `Dockerfile`:

    ```
    FROM ubuntu:14.04
    ```

4. Specify a maintainer documenting who maintains the image:

    ```
    MAINTAINER My Humble Self <me@humblepeople.com>
    ```

5. Add Linux commands to run that install the packages you need and do any other setup.

    ```
    RUN apt-get -y update
    RUN apt-get -y install python2.7
    ```

6. (Optional) You can set up environment variables and so on with a `.bashrc` file. This file should go into a working directory and will be sourced when your container starts executing on CodaLab. Note that your base image may already have a working directory and `.bashrc` file that you should keep and add to. See what is inside your base image by running it: `docker run -it --rm codalab/ubuntu:1.9 /bin/bash`

    ```
    RUN mkdir -m 777 /user
    RUN printf "export PYTHONPATH=src\n" > /user/.bashrc
    WORKDIR /user
    ```

7. Create an account on [Docker Hub](https://hub.docker.com/) where you will upload the image. Note your Docker Hub ID which you will use below (for our example, we use the ID `humblepeople`).
8. Finish editing the `Dockerfile` and build your image, specifying your Docker Hub ID, a name and a tag:

   ```
   docker build -t humblepeople/python:1.0 .
   ```

9. (Optional) Verify your image looks good to you by running a few test commands. If it doesn't, update your Dockerfile and rerun the build command.

   ```
   docker run -it --rm humblepeople/python:1.0 /bin/bash
   root@fb586e56ac91:/user# python2.7
   >>> exit()
   root@fb586e56ac91:/user# exit
   ```

10. Upload your image to Docker Hub:

   ```
   docker push humblepeople/python:1.0
   ```

11. Use your image on Codalab by specifying the `--request-docker-image humblepeople/python:1.0` flag with the `cl run` command.

12. Make your Dockerfile available when your share your worksheets. Either upload it to your worksheet, add a link to it from the worksheet, or set up [automated builds](https://docs.docker.com/docker-hub/builds/).

### Building Docker Images with CUDA support

This section describes how to build Docker images with CUDA support. 
We also have instructions for [running your own worker](Execution.md#running-your-own-worker), which discusses
GPU workers, and for using [GPUs on CodaLab](Execution.md#running-jobs-that-use-gpus).

CUDA consists of several components:

1. The NVIDIA driver, a kernel module, working with the /dev/nvidia device files.
2. CUDA driver, a shared library, working with the NVIDIA driver. This driver is different
for different versions of the NVIDIA driver and thus the two need to agree.
3. CUDA Toolkit containing code for using CUDA.

CodaLab makes 1 and 2 available inside the Docker container, as long as they are installed on the machine running the worker. If the image already contains the CUDA driver, it will be overridden since its version is unlikely to match the version of the NVIDIA driver on the host machine. To use the CUDA Toolkit you need to include it in your image. A good base image that contains the toolkit is `nvidia/cuda:7.5-cudnn4-devel`. The Tensorflow image `gcr.io/tensorflow/tensorflow:latest-gpu` is also set up correctly. Note, the Theano image available from the Theano website is not set up correctly at the time of writing, since it is missing the CUDA toolkit.
