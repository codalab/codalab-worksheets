FROM ubuntu:xenial
MAINTAINER CodaLab Team "codalab.worksheets@gmail.com"

# Installed packages:

# python 2.7
# pip
# cmake
# numpy
# scipy
# matplotlib
# pandas
# sympy
# nose
# spacy
# spacy-en
# tqdm
# wheel
# scikit-learn
# scikit-image
# dlib
# OpenAI gym[all]
# NLTK (no data)
# TensorFlow CPU
# keras
# OpenAI universe
# PyTorch CPU
# torchvision

RUN apt-get update; apt-get -y upgrade; apt-get -y install \
      python2.7 \
      python-pip \
      python-dev \
      python-software-properties \
      python-tk \
      software-properties-common \
      build-essential \
      cmake \
      default-jdk \
      libav-tools \
      libboost-all-dev \
      libeigen3-dev \
      libfreetype6-dev \
      libhdf5-dev \

RUN apt-get -y install \
      python-numpy \
      python-scipy \
      python-matplotlib \
      python-pandas \
      python-sympy \
      python-nose

RUN apt-get clean && apt-get autoremove

RUN pip install -U pip
RUN pip install -U \
      spacy \
      tqdm \
      wheel \
      scikit-learn \
      scikit-image \
      dlib \
      gym[all] \
      nltk \
      tensorflow \
      keras

RUN python -m spacy download en

RUN git clone https://github.com/openai/universe.git && \
    cd universe && \
        pip install -e .

RUN pip install -U http://download.pytorch.org/whl/cpu/torch-0.4.1-cp27-cp27mu-linux_x86_64.whl || pip install -U http://download.pytorch.org/whl/cpu/torch-0.4.1-cp27-cp27m-linux_x86_64.whl
RUN pip install torchvision
