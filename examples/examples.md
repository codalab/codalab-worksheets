# Codalab CLI Examples

This sub-directory contains a number of example programs and datasets for you
to use the Codalab CLI with. For the initial commit to the Python Package Index
we have included a pre-compiled version of Weka 3.7.9 for you to create a
program bundle with. Going forward, we will add commands to the CLI that will
allow browse example Program and Dataset bundles to download from the Codalab
Example server, multiple entry-points and Windows CLI support.

One of the initial steps is to seed CodaLab with all the standard and
state-of-the-art algorithms as well as popular datasets in machine learning,
NLP, and computer vision.  This document keeps track of the programs and
datasets which are to be uploaded to CodaLab, as well as providing guidelines
on how to do this.

Bundles will consist primarily of the following:

- Learning algorithms
- Utilities
- Standard machine learning datasets
- NLP datasets
- Vision datasets
- Tutorials

Note: these will no longer be necessary once we have a CodaLab instance
running.

Let us populate your CodaLab service actually with some initial content.
Normally, you would have the content on your local machine and just upload it
to CodaLab.  Just to get some basic programs and datasets into your system, you
will first run some scripts to download them to your local machine and then
upload them into the CodaLab instance running on your local machine.

For machine learning:

    (cd weka && ./download.sh)
    (cd weka && ./upload.sh)
    (cd uci_arff && ./download.sh)
    (cd uci_arff && ./upload.sh)
    ./basic_ml.sh  # Run basic ML pipeline

For NLP:

    (cd stanford_corenlp && ./download.sh)
    (cd stanford_corenlp && ./upload.sh)
    (cd lewis_carroll_poems && ./upload.sh)
    cl run program:stanford_corenlp input:lewis_carroll_poems 'program/run input output' --auto
