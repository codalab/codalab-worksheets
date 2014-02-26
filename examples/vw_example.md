# Vowpal Wabbit Tutorial

This is a general overview and tutorial on how to use the Vowpal Wabbit project with Codalab.


## What is Vowpal Wabbit ?

Vowpal Wabbit (VW) is a fast out-of-core learning system developed by John Langford at Microsoft Research. VW consists of an efficient and scalable online machine learning implementation with support for several machine learning reductions, importance weighting, loss functions and optimization algorithms. For more information see [Vowpal Wabbit](http://hunch.net/~vw/).


## Examples

VW can handle a number of machine learning tasks that involve lots of data and lots of features. We'll start with a basic classification example.


### 1. Create your first Bundle

A bundle is a self-contained package that comes in a directory. Generally, bundles should contain either a program or a dataset, although the system should be flexible and allow for, say, the output of a program to be treated as a dataset that may be the input for another program. For now, we'll start by creating a Vowpal Wabbit bundle with the following command:

```
$ codalab upload program vw
```

This command will create the Program Bundle that contains the compiled Vowpal Wabbit binary. Once this command executes, Codalab will prompt you with the Metadata input screen. You should see something like this:

```
Name: vw

Description: Upload /usr/local/bin/vw

Tags:

Architectures: x86_64

# Record metadata for the new program, then save and quit.
# Leave the name blank to cancel the upload.
```

On the Metadata Screen you can add/edit information about the Bundle such as change the name of the bundle, add tags or a description. We'll leave it like this for now. Once you have made changes to the Bundle's Metadata, press `esc` then type `:wq` and press `enter`. If it works properly, Codalab should return the UUID of the Bundle you just created. It will look something like this:

`0x6a0ed7c8eadd4e53995b4b8c9bb231d7`

Now that we've created our first program Bundle, let confirm that the Bundle is in our Codalab File System. Run the following command:

```
$ codalab list
```

Which returns a list of all the Bundles in your Codalab File System


```
Listing all bundles:

uuid                                name  bundle_type  state
------------------------------------------------------------
0x6a0ed7c8eadd4e53995b4b8c9bb231d7  vw    program      ready

```
We've confirmed that our **Bundle** was created. Next, let's create a **Dataset Bundle** and upload the data we want VW to run on. On a side note, we've aliased the `codalab` command to `cl` as well. You can use either.



### 2. Upload your first dataset

Change into the directory where the dataset is located. In this case, we can download the dataset directly with the following command:

```
$ wget http://goo.gl/GzxxUw
```
This will download the file `house_dataset` from the Codalab servers. Next, let's take a look at the file with the command:

```
$ cat house_dataset
```
which should return the contents of the `house_dataset`

```
0 | price:.23 sqft:.25 age:.05 2006
1 2 'second_house | price:.18 sqft:.15 age:.35 1976
0 1 0.5 'third_house | price:.53 sqft:.32 age:.87 1924
```

Next, we'll create the **Data Bundle** and add the `house_dataset` to it by running the command:

```
$ cl upload dataset house_dataset
```

Just like the **Program Bundle**, Codalab will immediately prompt you with the **Metadata Screen** for the `house_dataset` Metadata.

```
Name: house_dataset

Description: Upload /Users/<name>/<dir>/house_dataset

Tags:

# Record metadata for the new dataset, then save and quit.
# Leave the name blank to cancel the upload.
~
~
```

We're not going to make any changes at the moment, so save the Metadata and exit. Once this is complete, Codalab will return a UUID for the dataset.

`0x0966574c9e9e482ab14c65fda510fc55`

We'll then confirm that the **Data Bundle** was created by using the `codalab list` command:

```
$ cl list
```
which returns:

```
Listing all bundles:

uuid                                name           bundle_type  state
---------------------------------------------------------------------
0x6a0ed7c8eadd4e53995b4b8c9bb231d7  vw             program      ready
0x0966574c9e9e482ab14c65fda510fc55  house_dataset  dataset      ready
```


### 3. Creating the Run Bundle

Now, we will create a **Run Bundle** which creates a Bundle by running a program on an input. In this case, `vw` will be our program and `house_dataset` will be our input. Create the **Run Bundle** with the following command:

```
$ cl run vw housedata 'program/vw input/house_dataset'
```

As with the **Program Bundle** and **Data Bundle**, Codalab will return the Bundle **Metadata Screen**. Let's change the name of the Bundle from `anon-run` to `ex1`. Update the Bundle's Metadata and save the changes. Then, when you run the `cl list` command again, you should see the three bundles you've created.


```
Listing all bundles:

uuid                                name       bundle_type  state
-------------------------------------------------------------------
0x20c08d5e0c464126a00dd4aa369dc6de  housedata  dataset      ready
0xf732bd6631854de6b076148560c31085  vw         program      ready
0x0ee9e5929f044658869ac97ee18c8ec5  ex1        run          created
```

The Bundle has been created. Notice that the `state` for the `ex1` Bundle has been set to `created`. So far, we've only created the Bundle, we haven't actually executed the command parameter we passed to the Run Bundle. In order to execute the Run Bundle we need to run the **Codalab Worker**.


### 4. Codalab Worker
Open a new tab in your shell program and start a **Codalab Worker** with the following command:

```
$ cl worker
```

This will start the **Codalab Worker** which will monitor the Codalab File System for **Run Bundles** to execute. Once the **Codalab Worker** finds a bundle, you will see it execute that bundle via the worker's continuous monitor.


```
-- Run started! --
Running RunBundle(uuid='0x0ee9e5929f044658869ac97ee18c8ec5', name='ex1').
Executing command: program/vw input/house_dataset
In temp directory: /var/folders/6b/_k429s2x4x51cw8wxlhwfbv00000gn/T/tmpyY0DP0
2014-01-30 19:11:24:   Setting 1 bundle to READY...
2014-01-30 19:11:24:   Done! Took 0.01s.
Got data hash: 0x9985f44fb28e64a1f0424ff37c5c99899d333160
-- Success! --
```

This shows that the Bundle has successfully been executed. In fact, we can confirm this by running the `cl list` command and comparing the aforementioned 'state' variable again. The `state` variable of the Run Bundle `ex1` should show `ready` rather than `created`.

```
Listing all bundles:

uuid                                name       bundle_type  state
-----------------------------------------------------------------
0x20c08d5e0c464126a00dd4aa369dc6de  housedata  dataset      ready
0xf732bd6631854de6b076148560c31085  vw         program      ready
0x0ee9e5929f044658869ac97ee18c8ec5  ex1        run          ready
```


### 5. Generating Output Files

Next, let's take a look at the output of the run execution. Type the following command into the CLI:

```
$ cl ls ex1
```
This will return the contents of the **Run Bundle** directory.

```
Directories:
  output
Files:
  stderr
  stdout
```

Our **Run Bundle** output directory contains two files. A `stderr` file and a `stdout` file. To read the contents of these use the `cl cat` command on a file in the ex1 Bundle directory. Let's take a look at the `stderr` file:

```
$ cl cat ex1/stderr
```

This should return Vowpal's evaluation of the dataset

```
Num weight bits = 18
learning rate = 0.5
initial_t = 0
power_t = 0.5
using no cache
Reading datafile = input/house_dataset
num sources = 1
average    since         example     example  current  current  current
loss       last          counter      weight    label  predict features
0.666667   0.666667            2         3.0   1.0000   0.0000        5

finished run
number of examples = 3
weighted example sum = 4
weighted label sum = 2
average loss = 0.75
best constant = 0.5
best constant's loss = 0.25
total feature number = 15
```

Great. So we can see how Vowpal evaluated the file, but what if we wanted to save the output of that evaluation into a text file. Let's create another **Run Bundle** with a few modifications to the commands we pass to the execute function. This new flag will tell `vw` to save the output of the evaluation to a file called `house.model`. Run the following command to create the bundle and name it `ex3`:


```
$ cl run vw housedata 'program/vw input/house_dataset -l 10 -c --passes 25 -f house.model'
```

The **Codalab Worker** running in the background will now execute the new bundle and save the output file to the bundle directory. Like before, use the `cl cat ex3/house.model` command to view the contents of the evaluation. You should get the following output:


```
7.2.0m�?�n�����F?��Xg�\�ꏐ>%|X-��Q�]d[���L�%
```

Unfortunately, the output is formated for Vowpal to understand and it isn't very human friendly. Let's change that by creating another **Run Bundle**, but this time passing in a command to make the output human readable. To do that, we will replace the `-f` flag with  `--readable_model`. Then run:

```
$ cl run vw housedata 'program/vw input/house_dataset -l 10 -c --passes 25 --readable_model house.model'
```

First, check the files generated by the Run Bundle with the `cl ls ex4` command. The output should show the `house.model` file.

```
Directories:
  output
Files:
  house.model
  stderr
  stdout
```

Next, take a look at the content of the `house.model` file with the `cl cat ex4/house.model` command. You should now see a more human friendly output.

```
Version 7.2.0
Min label:0.000000
Max label:1.000000
bits:18
0 pairs:
0 triples:
rank:0
lda:0
ngram:0
skips:0
options:
:0
1924:-0.619880
1976:0.777038
2006:-0.225925
116060:0.282348
162853:-0.031150
165201:-0.053563
229902:-0.199813
```

### A few more commands
Let's say we want to edit the content of the metadata file from the earlier `ex1`; the Codalab CLI also makes this simple. Run the command:

```
$ cl edit ex1
```
