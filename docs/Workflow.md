This document describes how you can use CodaLab as part of your daily research
pipeline to become a more efficient researcher.

First, CodaLab is not meant to be a replacement for Git, your text editor, or
your shell scripts.  You should expect to use your favorite tools as well.
CodaLab only handles the execution and storage of runs.

Running experiments in CodaLab is quite simple but does require a slight shift
in thinking (not unlike switching from a procedural language to a functional
language).  You have to be a bit more tidy in the input-output behavior of your
programs, but this requirement also promotes good modular design and makes your
work easier to build on.

Without further ado, let's get started.  We will use the [command-line
interface (CLI)](CLI-Reference.md), since it can be more efficient than using the web
interface for many things.  In what follows will be a fictitious scenario to
give you an idea.

## Basic setup

On your local working directory (perhaps a git clone of your project
repository), suppose you have the following typical directory structure:

    src/        # Contains all your source code
    scripts/    # Scripts (generally pre-processing, post-processing)
    lib/        # Libraries that are required to compile your code
    bin/        # Where the compiled binaries are written
    Makefile    # Used to compile things
    data/       # Datasets that you want to use

It is recommended that you use a file to keep track of all the CodaLab commands
that you run.  While CodaLab does keep track of the dependencies of all the
commands that you run, these dependencies are on particular bundles, which
you will likely update over time.

First, upload your datasets.  You shouldn't need to update these very often.

    cl upload data/dataset1 -d "Description of dataset."
    cl upload data/dataset2 -d "Description of another dataset."
    ...

You can also upload your training and test sets separately.  But it's probably
non-ideal to upload the entire `data` directory as one bundle.

Upload relevant scripts and process your data:

    cl upload data/preprocess-data.py
    cl run :preprocess-data.py raw:dataset1 'python preprocess-data.py raw > cooked' -n process1

Upload your source code and compile it (if necessary):

    cl upload src
    cl upload Makefile
    cl run :src :lib :Makefile 'make' -n compile  # Assume this outputs to bin

Now you can run your compiled program on the processed data:

    cl run bin:compile/bin :lib data:process1/cooked 'bin/main --input data --output . --eta 0.3'

You can launch multiple programs and monitor their progress in the web
interface.  To kill and remove the last 3 runs, for example:

    cl ls
    cl kill ^1-3
    cl rm ^1-3

From the web interface, type `u` to paste the uuid of the selected bundle into
the web terminal.  You can do this for all the runs you want to operate on.
Then type `c` to activate the web terminal and prepend `kill` or `rm`.

Often you will be making rapid changes to your program and wish to iterate quickly.
You can do this easily:

1. Edit your source code (in `src`).

2. Run the following command in a shell:

        cl upload src && cl mimic src^2 src

This wonderful command uploads `src` as a new bundle to the worksheet,
and then the `mimic` command runs all the commands that depend on `src^2` (the
second to last bundle called `src`) on the new `src` instead.  This will probably
include compiling the code as well as running any downstream tasks.  Note
that runs that depend on earlier versions of your command will still continue
to run.

If you want to delete the old code and all downstream dependencies recursively:

    cl rm -r src^2  # Be careful of what you're deleting!

## Using scripts

As your project becomes more complex, you'll want to create a script that
launches CodaLab runs by passing in the appropriate command-line arguments.
You can see
[./train.sh](https://github.com/codalab/worksheets-examples/blob/master/01-nli/train.sh)
from the worksheet examples repo or a [fancier Ruby
script](https://github.com/percyliang/seq2seq-utils/blob/master/run.rb) that
allows you to build command-line arguments in a more modular and flexible way.

## Interfacing with programs

Of course your program can do anything it wants, but there two things that help
CodaLab make it more useful.

**Command-line arguments**.
Anything that you want to change (hyperparameters, dataset path, etc.) should
be a command-line argument, rather than a hard-coded value in your program.

This allows you to run many versions of your program without having to change
your source code.  In Python, you can do this easily using `argparse`.

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-iterations', type=int, default=10)
    ...
    args = parser.parse_args()

**Outputing stats**.
As your program is running, it is useful to print out summary statistics (which
iteration, objective function value, accuracy, and any other metrics) so that
CodaLab can display them.  Here's how one might structure this in Python:

    import json
    STATS = {}
    def flush_stats():
        with open('stats.json', 'w') as f:
            print >>f, json.dumps(STATS)

    STATS['num_iterations'] = args.num_iterations
    for iteration in range(num_iterations):
        STATS['iteration'] = iteration
        ...
        STATS['accuracy'] = ...
        flush_stats()

Then insert the following into your worksheet:

    % schema r1
    % addschema run
    % add |
    % add #iter /stats.json:num_iterations
    % add iter /stats.json:iteration
    % add acc /stats.json:accuracy

    % display table r1
    ... (your bundles here) ...

Your table will now have three new columns corresponding to the values that you
output.  If you have 20 jobs running at once, this is a quick way to see at a
glance what is going on with them.  See [markdown
documentation](Worksheet-Markdown.md) for more information on how to customize the
display of tables.
