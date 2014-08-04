# !/bin/bash

# Creates a macro for doing simple classification using Weka.
# Macro:
#
#   cl macro weka <learner (file containing the class name)> <arff file>
#
# To create the macro, we use Weka's J48 decision trees and vote.arff (a simple
# UCI dataset).

cl work weka-uci || cl new weka-uci

cl add -m "A simple machine learning pipeline consists of splitting data into train and test, learning a classifier, stripping the labels on the test set, predicting on the test set, and evaluating the predictions."
cl add -m ""
cl add -m "We will use Weka, a popular library for machine learning written in Java."
cl add weka || exit 1

echo "Setting learner..."
cl add -m ""
cl add -m "We will use the J48 decision trees algorithm."
echo weka.classifiers.trees.J48 | cl upload dataset - -n weka-in1 -a || exit 1

cl add -m ""
cl add -m "We will run on a trivial dataset (arff format):"
echo -e "@relation sample\n@attribute 'x' {'a', 'b'}\n@attribute 'y' {'-', '+'}\n@data\n'a','-'\n'b','+'" | cl upload dataset - -n weka-in2 -a || exit 1

echo "Splitting data into training and test..."
cl add -m ""
cl add -m "First, we need to split the data into training and test."
cl run :weka all.arff:%weka-in2% 'weka/split all.arff train.arff test.arff 2' -n split-data -a || exit 1

echo "Learning a model..."
cl add -m ""
cl add -m "Second, we learn a decision tree (one of many algorithms in Weka) on the training data."
cl run '%weka%/learn `cat %weka-in1%` %split-data%/train.arff weka_classifier' -n model -a || exit 1

echo "Stripping labels..."
cl add -m ""
cl add -m "Third, we remove the output labels on the test set so that the algorithm can't cheat."
cl run '%weka%/stripLabels %split-data%/test.arff data.arff' -n stripped-test -a || exit 1

echo "Predicting..."
cl add -m ""
cl add -m "Fourth, predict using the trained model on the stripped dataset."
cl run '%weka%/predict %model%/weka_classifier %stripped-test%/data.arff predictions' -n predict-on-test -a

echo "Evaluate..."
cl add -m ""
cl add -m "Finally, we evaluate the predictions of the algorithm on the test set (with the true outputs).  The error rate is"
cl add -m "% display inline /status:errorRate"
cl run '%weka%/evaluate %split-data%/test.arff %predict-on-test%/predictions status' -n weka-out -a

cl add -m ""
cl add -m "And that's it!"
echo "Waiting for results..."
cl wait ^
cl cat ^/status
