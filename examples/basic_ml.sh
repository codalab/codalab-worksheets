# !/bin/bash

# Creates a worksheet that demonstrates running a basic ML pipeline.

cl work simple-ml || cl new simple-ml

weka=weka
vote=vote

cl add -m "This simple worksheet demonstrates how to run a simple machine learning pipeline: split, learn, predict, evaluate."
cl add -m "We will use Weka, a popular library for machine learning written in Java."
cl add $weka
cl add -m "We will run on a simple dataset:"
cl add $vote

echo "Splitting data into training and test..."
cl add -m "First, we need to split the data into training and test."
split=$(cl run program:$weka input:$vote 'program/split input output 4' --name split-data --auto)/output

echo "Learn a model..."
cl add -m "Second, we learn a decision tree (one of many algorithms in Weka) on the training data."
model=$(cl run program:$weka input:$split/train 'program/learn input output weka.classifiers.trees.J48' --name model --auto)/output

echo "Predict..."
cl add -m "Third, we remove the output labels on the test set so that the algorithm can't cheat."
strippedTest=$(cl run program:$weka input:$split 'program/stripLabels input/test output' --name stripped-test --auto)/output
cl add -m "We group the model and the test data into one bundle..."
modelAndData=$(cl make model:$model data:$strippedTest --name "model-and-test" --auto)
cl add -m "...and send it to the prediction program."
predictions=$(cl run program:$weka input:$modelAndData 'program/predict input output' --name predict-on-test --auto)/output

echo "Evaluate..."
cl add -m "Finally, we evaluate the predictions of the algorithm on the test set (with the true outputs)."
predictionsAndData=$(cl make predictions:$predictions data:$split/test --name predictions-and-data --auto)
evaluation=$(cl run program:$weka input:$predictionsAndData 'program/evaluate input output' --name evaluation --description "Evaluate on predictions" --auto)

cl add -m "And that's it!"
echo "Waiting for results..."
cl wait $evaluation
cl info $evaluation
cl cat $evaluation/output/stats
