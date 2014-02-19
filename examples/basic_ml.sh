# !/bin/bash

# Demonstrates using command-line tools to run a basic ML pipeline.

cl new simple-ml

echo "Uploading program and data..."
weka=$(cl upload program weka --name weka --description "Weka is a collection of machine learning algorithms for data mining tasks.  The algorithms can either be applied directly to a dataset or called from your own Java code. Weka contains tools for data pre-processing, classification, regression, clustering, association rules, and visualization.  It is also well-suited for developing new machine learning schemes." --tags machine_learning --auto)
vote=$(cl upload dataset uci_arff/auto/vote --name vote --description "Congressional voting records" --tags machine_learning --auto)

echo "Splitting data into training and test..."
split=$(cl run $weka $vote 'program/split input output 4' --description "Splitting data into training and test" --auto)/output
strippedTest=$(cl run $weka $split 'program/stripLabels input/test output' --description "Stripping labels" --auto)/output

echo "Learn a model..."
model=$(cl run $weka $split/train 'program/learn input output weka.classifiers.trees.J48' --description "Learn a model" --auto)/output

echo "Predict..."
modelAndData=$(cl make model:$model data:$strippedTest --description "model + test data" --auto)
predictions=$(cl run $weka $modelAndData 'program/predict input output' --description "Predict on test" --auto)/output

echo "Evaluate..."
predictionsAndData=$(cl make predictions:$predictions data:$split/test --description "predictions + data" --auto)
evaluation=$(cl run $weka $predictionsAndData 'program/evaluate input output' --description "Evaluate on predictions" --auto)

echo "Waiting for results..."
cl wait $evaluation
cl info $evaluation
cl cat $evaluation/output/stats
