# Worksheets

NOTE: this document is out of date.

Codalab Worksheets provide a light-weight method for creating multi-stage experiment that a user wants to perform (e.g., running and evaluating a machine learning algorithm) that is independent from your main Codalab Bundle collection. In other words, Worksheets operate sort of like Git Branches.


Formally, a Worksheet is a sequence of blocks, where each block is one of the following:

1.  A rich text block which allows users to document their experiment using free text.

2. A single Bundle whose contents can be viewed and edited.

3.  A Table, where each row represents a Bundle and columns represent different attributes of that Bundle. This is useful for comparing different algorithms side-by-side according to some metric.


This guide explains how use **Codalab Worksheets**.

### Worksheet Commands

```
Commands for using worksheets include:
  new     Create a new worksheet and make it the current one.
  add     Append a bundle to a worksheet.
  work    Set the current worksheet.
  print   Print the full-text contents of a worksheet.
  edit    Use `cl edit -w` to edit worksheets.
  list    Use `cl list -w` to list worksheets.
  rm      Use `cl rm -w` to rm worksheets.
```


## 1. Create a worksheet
When you're working on a machine learning experiment, you're going to have a bunch of different features, labels, hyperparameters or weights that you are modifying at any given time. Keeping track of these and their output can be time consuming and difficult. Worksheets are a way to help you manage this workflow.

When you create a **Worksheet**, you are creating a seperate collection of **Bundles** where you can try out new ideas and tie things together logically.

To create a **Worksheet** run the following command:

```
$ cl new deeplearning
```

which will return :
```
Switched to worksheet deeplearning.
```

Once your **Worksheet** has been created, it's time to start adding Bundles to work with. Whenever you add, edit, or delete a file, you're modifying the collection available and adding them to your **Worksheet**.

When you initialize the Codalab CLI, you automatically join the master Worksheet. Now that you've created the **Worksheet** called `deeplearning`, Codalab will switch you to that **Worksheet**. Let's confirm we're on the correct worksheet for the experiment by returning a list of all available worksheets on our system:

```
$ cl list -w
```
The `cl list -w` command has returned a list of all the worksheets on our system.

```
Listing all worksheets:

uuid                                name
-------------------------------------------------
0x60bb57b41e9e4d98815db966bd6be85c  testworksheet
0x770d9dd15b294fa1bd5130d30145914d  msr
0x239475fd6c574f8c89bd6de0885501a6  deeplearning
```
Just like **Bundles** each worksheet is referenced by a `UUID`. But how do we know if we are on the correct worksheet ? One way to figure this out is to run the `cl list` command to view the available **Bundles** on this branch. Since Codalab automatically makes the latest Worksheet created the active worksheet, if we run the `cl list` command, we should see a message stating that we have no **Bundles** available:

```
No bundles from worksheet deeplearning found.
```
Another way we can confirm that we are operating on the correct worksheet is to manually switch to the target worksheet with the `cl work` command. For this example, we would run the following:

```
$ cl work deeplearning
```

which should return:
```
Switched to worksheet deeplearning.
```

Next, let's add a **Dataset Bundle** to our worksheet.


## 2. Adding a Bundle to your Worksheet
We called our worksheet `deeplearning` because we're going to create a **Deep Learning** experiment. Specifically, we're going to utilize **Deep Unsupervised Learning** apply **Feature Extraction** on a recent Kaggle black-box learning challenge dataset.


First, download the example dataset and create a **Dataset Bundle** called `blackbox`:

```
$ cl upload dataset blackbox/
```

After creating the Dataset Bundle confirm that it has been added to our worksheet with `cl list`.

```
Listing all bundles from worksheet deeplearning:

uuid                                name      bundle_type  state
----------------------------------------------------------------
0xbb024aaf3a9743d5baac44ed577f2165  blackbox  dataset      ready
```

Now, we want to investigate the contents of the bundle. Run the `cl ls` command to view the files included with the Dataset Bundle:

```
$ cl ls blackbox
```

which will return:

```
Files:
  __init__.py
  black_box_dataset.py
  learn_zca.py
  make_submission.py
  mlp.yaml
  README
  zca_mlp.yaml
```


