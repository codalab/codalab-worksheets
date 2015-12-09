FROM ubuntu:14.04

MAINTAINER Percy Liang <pliang@cs.stanford.edu>

# Basic Python environment
RUN apt-get update
RUN apt-get install -y python-pip
RUN apt-get install -y python-dev

# Numerical libraries
RUN apt-get install -y libopenblas-dev
RUN apt-get install -y liblapack-dev
RUN pip install numpy
RUN pip install nose
RUN python -c "import numpy; numpy.test()"

RUN apt-get install -y libatlas-base-dev
RUN apt-get install -y gfortran
RUN pip install scipy
RUN python -c "import scipy; scipy.test()"

# Useful utilities
RUN apt-get install -y wget
RUN apt-get install -y screen
RUN pip install virtualenv

# Plotting
RUN apt-get install -y libfreetype6-dev
RUN apt-get install -y pkg-config
RUN pip install matplotlib

# Basic packages
RUN pip install sklearn
RUN pip install pyyaml
RUN pip install pandas
RUN pip install sympy
RUN pip install scikit-tensor
RUN pip install cvxopt
RUN pip install jupyter

# Dev version of Theano to make keras work
RUN apt-get install -y git
RUN pip install --upgrade --no-deps git+git://github.com/Theano/Theano.git
#RUN python -c "import theano; theano.test()"  # Takes too long
RUN python `python -c "import os, theano; print os.path.dirname(theano.__file__)"`/misc/check_blas.py

RUN pip install keras
