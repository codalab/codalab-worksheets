# CodaLab Bundle Service and Comand-Line Interface [![Build Status](https://travis-ci.org/codalab/codalab-cli.png?branch=master)](https://travis-ci.org/codalab/codalab-cli)

The CodaLab Bundle Service allows users to create *bundles*, which are
immutable directories containing either code or data.  Bundles are either
uploaded or created from other bundles by executing generic code in an
experimental workflow.  When the latter happens, all the provenance information
is preserved.  Think: Git for experiments.  In addition, users can create
*worksheets*, which interleave bundles with free-form textual descriptions,
allowing one to easily document an experiment.

This package also contains a command-line interface `cl` that provides flexible
access to the CodaLab Bundle Service.  The CodaLab
[website](https://github.com/codalab/codalab) provides a graphical interface to
this functionality.

## Installation

1. Clone the repository:

    git clone https://github.com/codalab/codalab-cli

2. Setup a Python virtual environment:

    virtualenv -p /usr/bin/python2.7 codalab_env --no-site-packages

3. Install dependencies into that virtual environment:

    codalab_env/bin/pip-2.7 install sqlalchemy

4. Set your path to include CodaLab for convenience (add this line to your .bashrc):

    export PATH=$PATH:$PWD/codalab/bin

Now you are ready to start using CodaLab!

## Filesystem analogy

It's helpful to keep the following analogy in mind:

    CodaLab bundle = operating system file

    CodaLab worksheet = operating system directory

    CodaLab instance = operating system drive

Some differences:

- The contents of bundles are immutable.
- A worksheet contains bundles in a user-specified order interleaved with text.
- Each bundle are owned by exactly one worksheet, but can appear in many
  worksheets (as symlinks).
- CodaLab instances can be remote.

## Basic Local Usage

To print out a help message, simply type:

    cl
    
To upload a bundle into the system:

    echo -e "foo\nbar\nbaz" > a.txt  # Create the bundle in the current directory
    cl upload dataset a.txt          # This will prompt you for information
    cl ls                            # Should show the new bundle that you uploaded
    cl info a.txt                    # Show info about the last bundle

To run a command on this bundle:

    cl run :a.txt 'sort a.txt > output/b.txt' --name sort # Creates a job to be run
    cl worker                                             # Start up a worker in another shell, it should run sort
    cl cat sort/output/b.txt                              # Display the result of the run
    cl download sort                                      # Download that bundle to the current directory

## Remote Usage

[TODO: copying between instances]

## Populating 

Let's populate your CodaLab service actually with some initial content.
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
    cl run program:stanford_corenlp input:lewis_carroll_poems 'program/run input output' --auto


3. Validate by running tests

    pip install simplejson mock nose
    
    nosetests

