FROM ubuntu:16.04
MAINTAINER CodaLab Worksheets <codalab.worksheets@gmail.com>

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update; apt-get install -y \
    build-essential \
    git \
    libfuse-dev \
    libjpeg-dev \
    libmysqlclient-dev \
    mysql-client \
    python-dev \
    python-pip \
    python-software-properties \
    python-virtualenv \
    software-properties-common \
    zip;

# Install dependencies
RUN pip install --upgrade pip MySQL-python
COPY docker/compose_files/files/wait-for-it.sh /opt/wait-for-it.sh
RUN mkdir /opt/codalab-worksheets
COPY worker /opt/codalab-worksheets/worker
COPY requirements.txt /opt/codalab-worksheets
COPY requirements-server.txt /opt/codalab-worksheets
COPY setup.sh /opt/codalab-worksheets
RUN cd /opt/codalab-worksheets && ./setup.sh server

# Install code
COPY . /opt/codalab-worksheets

RUN pip install -e /opt/codalab-worksheets/worker/
RUN pip install -e /opt/codalab-worksheets/

ENV PATH="/opt/codalab-worksheets/codalab/bin:${PATH}"
ENV CODALAB_HOME=/home/codalab

EXPOSE 2900

ENTRYPOINT ["/opt/codalab-worksheets/codalab/bin/cl"]
