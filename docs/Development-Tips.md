Below is a collection of development tips when running CodaLab locally.

# setting up local instance (-bd is sane default (build, dev mode))
python codalab_service.py start -bd
username codalab
password codalab

# list docker containers running
docker ps (`docker ps -a` shows all dead containers as well)
# check logs of docker container
docker logs codalab_worker_1
# run commands in container
docker exec -it codalab_worker_1 /bin/bash

# connect to prod instance from the CLI
cl work https://worksheets.codalab.org::

# connect to dev instance from the CLI
cl work https://worksheets-dev.codalab.org::

# connect to stanford instance from the CLI
cl work https://codalab.stanford.edu::

# connect to local instance from the CLI
cl work localhost::

# if you're making changes to the CLI and want to test them out real time,
# in order to use the current directory's changes as the "cl" executable
pip install -e .
