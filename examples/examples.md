# Codalab CLI Examples

This sub-directory contains a number of example programs and datasets for you to use the Codalab CLI with. For the initial commit to the Python Package Index we have included a pre-compiled version of Weka 3.7.9 for you to create a program bundle with. Going forward, we will add commands to the CLI that will allow browse example data & program bundles to download from the Codalab Example server, multiple entry-points and Windows CLI support.


## Bundles

One of the initial steps is to seed CodaLab with all the standard and state-of-the-art algorithms as well as popular datasets in machine learning, NLP, and computer vision.  This document keeps track of the programs and datasets which are to be uploaded to CodaLab, as well as providing guidelines on how to do this.

Bundles will consist primarily of the following:

- Learning algorithms
- Utilities
- Standard machine learning datasets
- NLP datasets
- Vision datasets
- Tutorials

### Learning algorithms

#### 1. Weka
Weka is a collection of machine learning algorithms for data mining tasks. The algorithms can either be applied directly to a dataset or called from your own Java code. Weka contains tools for data pre-processing, classification, regression, clustering, association rules, and visualization. It is also well-suited for developing new machine learning schemes.
source: http://www.cs.waikato.ac.nz/ml/weka/

This package contains Weka (with some unnecessary files removed to save space). Currently, all these wrappers require bash. src/Main.java contains a light wrapper around some of the utilties. See this page to find more information about invoking Weka: http://weka.wikispaces.com/Primer




### Utilities

- Converter between csv, tsv, json, arff formats.
- Programs that plot curves (turn tsv into a jpg).


### Standard machine learning datasets


