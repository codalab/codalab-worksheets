#!/bin/bash

# Download all the data
wget -np -r http://repository.seasr.org/Datasets/UCI/arff || exit 1
mv repository.seasr.org/Datasets/UCI/arff/*.arff . || exit 1
rm -rf repository.seasr.org/ || exit 1

# Format data
mkdir auto || exit 1
for x in *.arff; do
  y=`echo $x | sed -e 's/.arff//'`
  echo $y
  mkdir auto/$y || exit 1
  mv $x auto/$y/data.arff || exit 1
done
