#!/bin/bash

# Download all the data
wget -np -r http://repository.seasr.org/Datasets/UCI/arff || exit 1

# Move it into the right place
mkdir auto || exit 1
mv repository.seasr.org/Datasets/UCI/arff/*.arff auto || exit 1
rm -rf repository.seasr.org/ || exit 1
