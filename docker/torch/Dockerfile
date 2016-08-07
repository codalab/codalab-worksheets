# Copied mostly from kaixhin/cuda-torch:7.0
# Difference is that we install things into /user instead of /root.

# Start with Ubuntu base image
FROM ubuntu:14.04
MAINTAINER Percy Liang <pliang@cs.stanford.edu>

# Install git, apt-add-repository and dependencies for iTorch
RUN apt-get update && apt-get install -y \
  git \
  software-properties-common \
  ipython3 \
  libssl-dev \
  libzmq3-dev \
  python-zmq \
  python-pip

# Install Jupyter Notebook for iTorch
RUN pip install notebook ipywidgets

RUN mkdir -p /user

# Run Torch7 installation scripts
RUN git clone https://github.com/torch/distro.git /user/torch --recursive && cd /user/torch && \
  bash install-deps && \
  ./install.sh

RUN /user/torch/install/bin/luarocks install nn

# Set ~/torch as working directory
WORKDIR /user

# These are moved out of the Dockerfile, otherwise CodaLab doesn't load them currently.
COPY .bashrc /user/.bashrc
