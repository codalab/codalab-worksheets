FROM ubuntu:16.04
MAINTAINER Percy Liang <pliang@cs.stanford.edu>

ENV DEBIAN_FRONTEND noninteractive

# Install dependencies
RUN apt-get update; apt-get install -y \
  python-dev \
  python-pip;

COPY worker/requirements.txt /opt
RUN /usr/bin/env python -m pip install --user --upgrade pip; \
    /usr/bin/env python -m pip install -r /opt/requirements.txt;

# Install dependencies again
COPY worker /opt/worker

RUN /usr/bin/env python -m pip install --user --upgrade pip; \
    /usr/bin/env python -m pip install -e /opt/worker;

ENTRYPOINT ["cl-worker"]
