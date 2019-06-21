FROM ubuntu:xenial
MAINTAINER CodaLab Team "codalab.worksheets@gmail.com"

# Begin common steps (Must be the same in the CPU and GPU images)
RUN apt-get update && apt-get install -y \
    apt-transport-https \
    iputils-ping \
    git \
    python2.7 \
    python-pip \
    python-dev \
    python-software-properties \
    python-tk \
    software-properties-common \
    build-essential \
    cmake \
    libhdf5-dev \
    swig \
    wget \
    curl

## Python 3.6
RUN add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update -y  && \
    apt-get install python3.6 -y \
        python3.6-venv \
        python3.6-dev \
        python3-software-properties

RUN curl https://bootstrap.pypa.io/get-pip.py | python3.6

### Without this Python thinks we're ASCII and unicode chars fail
ENV LANG C.UTF-8

## Oracle JDK 11
RUN echo oracle-java11-installer shared/accepted-oracle-license-v1-2 select true | debconf-set-selections && \
    add-apt-repository -y ppa:linuxuprising/java && \
    apt-get update && \
    apt-get install -y oracle-java11-installer && \
    apt-get install -y oracle-java11-set-default && \
    rm -rf /var/cache/oracle-jdk11-installer

ENV SCALA_VERSION 2.12.6

## Scala
RUN wget http://scala-lang.org/files/archive/scala-$SCALA_VERSION.deb && \
    dpkg -i scala-$SCALA_VERSION.deb && \
    echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list && \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 && \
    apt-get -y update && \
    apt-get -y install sbt && \
    apt-get clean && \
    apt-get autoremove && \
    rm -rf /var/lib/apt/lists/*

ENV SCALA_HOME /usr/share/java

RUN pip2 install -U pip
RUN pip2 install -U \
      numpy \
      scipy \
      matplotlib \
      pandas \
      sympy \
      nose \
      spacy \
      tqdm \
      wheel \
      scikit-learn \
      scikit-image \
      nltk

RUN python -m spacy download en

## Set up python3.6 environment

RUN pip3 install -U pip
RUN pip3 install -U \
      numpy \
      scipy \
      matplotlib \
      pandas \
      sympy \
      nose \
      spacy \
      tqdm \
      wheel \
      scikit-learn \
      scikit-image \
      nltk

RUN python3.6 -m spacy download en

# End common steps
# CPU-specific commands

RUN pip2 install -U \
      tensorflow==1.12.0 \
      tensorboard \
      keras

RUN pip2 install -U http://download.pytorch.org/whl/cpu/torch-0.4.1-cp27-cp27mu-linux_x86_64.whl || pip install -U http://download.pytorch.org/whl/cpu/torch-0.4.1-cp27-cp27m-linux_x86_64.whl
RUN pip2 install torchvision


RUN pip3 install -U \
      tensorflow==1.12.0 \
      tensorboard \
      keras

RUN pip3 install http://download.pytorch.org/whl/cpu/torch-0.4.1-cp36-cp36m-linux_x86_64.whl
RUN pip3 install torchvision
