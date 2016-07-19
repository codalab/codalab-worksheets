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

# Set ~/torch as working directory
WORKDIR /user/torch

# Export environment variables manually
ENV LUA_PATH='/user/.luarocks/share/lua/5.1/?.lua;/user/.luarocks/share/lua/5.1/?/init.lua;/user/torch/install/share/lua/5.1/?.lua;/user/torch/install/share/lua/5.1/?/init.lua;./?.lua;/user/torch/install/share/luajit-2.1.0-beta1/?.lua;/usr/local/share/lua/5.1/?.lua;/usr/local/share/lua/5.1/?/init.lua'
ENV LUA_CPATH='/user/.luarocks/lib/lua/5.1/?.so;/user/torch/install/lib/lua/5.1/?.so;./?.so;/usr/local/lib/lua/5.1/?.so;/usr/local/lib/lua/5.1/loadall.so'
ENV PATH=/user/torch/install/bin:$PATH
ENV LD_LIBRARY_PATH=/user/torch/install/lib:$LD_LIBRARY_PATH
ENV DYLD_LIBRARY_PATH=/user/torch/install/lib:$DYLD_LIBRARY_PATH
ENV LUA_CPATH='/user/torch/install/lib/?.so;'$LUA_CPATH
