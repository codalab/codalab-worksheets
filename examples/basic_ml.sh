# !/bin/bash

# Demonstrates using command-line tools to run a basic ML pipeline.

cl new simple-ml

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
