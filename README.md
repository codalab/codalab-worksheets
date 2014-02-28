# CodaLab Bundle Service and Comand-Line Interface [![Build Status](https://travis-ci.org/codalab/codalab-cli.png?branch=master)](https://travis-ci.org/codalab/codalab-cli)

This repository provides the Python backend for operating on bundles (creating,
running, uploading, etc.).  The API exposed is used by both the
[website](https://github.com/codalab/codalab) and the command-line utility
(provided in this package).

## Getting started

1. After cloning the repository, install the appropriate packages:

    virtualenv codalab_env --no-site-packages

    codalab_env/bin/pip-2.7 install sqlalchemy

2. Setup the interface to 

    export PATH=$PWD/codalab/bin

    cl  # This should print out a help message.

    cl worker  # Start up a worker in another shell

3. Let's populate your CodaLab service actually with some initial content.
Normally, you would have the content on your local machine and just upload it
to CodaLab.  Just to get some basic programs and datasets into your system, you
will first run some scripts to download them to your local machine and then
upload them into the CodaLab instance running on your local machine.

For machine learning:

    (cd examples/weka && ./download.sh)
    (cd examples/weka && ./upload.sh)
    (cd examples/uci_arff && ./download.sh)
    (cd examples/uci_arff && ./upload.sh)
    (cd examples && ./basic_ml.sh)  # Run basic ML pipeline

For NLP:

    (cd examples/stanford_corenlp && ./download.sh)
    (cd examples/stanford_corenlp && ./upload.sh)
    (cd examples/lewis_carroll_poems && ./upload.sh)
    cl run stanford_corenlp lewis_carroll_poems 'program/run input output' --auto
