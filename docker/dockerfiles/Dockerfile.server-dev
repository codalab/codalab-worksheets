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
COPY docker/compose_files/files/wait-for-it.sh /opt/wait-for-it.sh
WORKDIR /opt/codalab-worksheets
RUN pip  install -U setuptools pip
COPY . /opt/codalab-server/
COPY ./worker /opt/codalab-worker/
RUN cd /opt/codalab-server && pip install -r requirements-server.txt
RUN pip install -e /opt/codalab-worker/
RUN pip install -e /opt/codalab-server/

ENV CODALAB_HOME=/home/codalab

EXPOSE 2900

ENTRYPOINT ["cl"]
