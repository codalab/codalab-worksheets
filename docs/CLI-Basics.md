The command-line interface (CLI) gives you the ability to use CodaLab more
efficiently and programatically.  For example, you can copy bundles between
CodaLab servers, use your own editor to edit worksheets, and write scripts that
interact with CodaLab.  It is common to use the CLI in conjunction with the web
interface.

## Installation

1. Make sure you have the dependencies (Python 2.7, virtualenv, fuse, and an up-to-date setuptools package).  

* Ubuntu users can install them by running:

        sudo apt-get install python2.7 python2.7-dev python-virtualenv fuse
        pip install -U setuptools

* Mac users can install FUSE for macOS, an optional dependency, here: https://osxfuse.github.io/. Install or upgrade setuptools by running `pip install -U setuptools`.

2. Install the CodaLab CLI:

        pip install codalab

3. To complete the configuration, type the following:

        cl work

  This will prompt you for your username/password at `worksheets.codalab.org`.
  You should create an account on the [CodaLab website](worksheets.codalab.org)
  if you haven't done so already.

Now you are ready to start using the CodaLab CLI!

## Quickstart

After having installed the CLI, let's upload a program, a dataset, and perform
a run, similar to what we did in the [Quickstart](Quickstart).

### Step 0: Orienting yourself

At any point in time, `cl` is pointing to a current CodaLab server and a
current worksheet (think of it like a current working directory).  This state
is stored in `~/.codalab/state.json`.

To see where you're pointed to, type:

    cl work

Most likely, you'll see something like:

    Currently on worksheet https://worksheets.codalab.org/bundleservice::home-pliang(0x2a0a3904a3e04840823aa9c7dd44f7e1).

Here, `https://worksheets.codalab.org/bundleservice` is the CodaLab server and
`home-pliang` is the worksheet (this acts like a home directory).

If you have [set up your own CodaLab server](Server-Setup) at `http://localhost:2900`,
then you might switch to it like this:

    cl work http://localhost:2900::

and switch back with either of these (`main` is an alias, type `cl alias` to see):

    cl work main::
    cl work https://worksheets.codalab.org/bundleservice::

### Step 1: Uploading bundles

Create a file on your local computer called `a.txt` with the following contents:

    foo
    bar
    baz

Create another file called `sort.py` with the following contents:

    import sys
    for line in sorted(sys.stdin.readlines()):
        print line,

To upload local files `a.txt` and `sort.py`:

    cl upload a.txt
    cl upload sort.py

To edit the metadata (to change the name, description, etc.) in a text editor:

    cl edit a.txt

To list the bundles you've uploaded, type:

    cl ls

You can see the metadata of a bundle:

    cl info -v a.txt

While `a.txt` is a dataset and `sort.py` is a program, from CodaLab's
perspective, these are just bundles.  Directories can be passed directly to `cl
upload`.

In general, to get more information about a command:

    cl upload --help
    cl edit --help
    ...

### Step 2: Run a command

To create our first run bundle, type:

    cl run :sort.py :a.txt 'python sort.py < a.txt' -n sort-run

This should append a new bundle named `sort-run` to the current worksheet.
Verify this by typing `cl ls` and see the state go from `created` to `staged`
to `running` to `ready`.

Let's unpack the command a bit.  The first two arguments of `cl run` specify the
dependencies and the third is the command, which is run in the presence of the
dependencies.  In this specific example, it is as if you ran the command in the
following environment:

    $ ls
    sort.py
    a.txt
    $ python sort.py < a.txt

CodaLab captures all the files and directories that are written to the current
directory, as well as stdout and stderr.  These form the contents of the
newly-created run bundle.  When the command terminates, the contents of the
bundle become immutable.

To look at the output of the run (you can do this while the run is in progress):

    cl cat sort-run         # Lists the files in the run bundle
    cl cat sort-run/stdout  # Shows the stdout (which CodaLab redirects to a file)
    cl info -v sort-run     # Shows metadata and a preview of the contents

You can treat the current worksheet like a current directory, and each new run
adds files/directories.  This is almost right, with the exception of two
differences: First, you need to explicitly specify the dependencies.
Second, when a command outputs a file, it is under the run bundle, not at the
top-level.  Therefore subsequent commands must specify the run bundle to refer
to them.  This example illustrates the idea:

        cl run 'echo hello > message' -n run1
        cl run message:run1/message 'cat message'     # right
        cl run message:message      'cat message'     # wrong

### Step 3: Present your results

So far, we have been typing `cl ls` to see what's in our worksheet.  To get a nicer view,
we can type the following (or go to the web interface):

    cl print

We can customize this view to better document and present our results.  To
edit the worksheet, type:

    cl wedit

You will open up your favorite editor, where you can freely edit the worksheet
using [CodaLab markdown](Worksheet-Markdown).
CodaLab markdown is an extension of markdown that allows you to interleave
bundles and formatting directives.

For example, you might edit your worksheet to the following (your worksheet
will show different UUIDs):

    This is my **first** [CodaLab worksheet](https://worksheets.codalab.org).
    I uploaded some bundles:
    [dataset a.txt]{0x34a1fa62acc840ec96da98f17dbddf66}
    [dataset sort.py]{0xf9fc733b19894eb2a97f6b47f35d7ea0}
    Here's my first CodaLab run:
    % display table name command /stdout time
    [run sort-run -- :sort.py,:a.txt : python sort.py < a.txt]{0x08908cc6cb594b9394ed7ba6a0bd25f6}

The directive `% display table ...` tells CodaLab to render the bundle as a table
with certain columns.  For example, the `/stdout` column tells CodaLab to display
the contents of the `stdout` file inside the bundle.  This custom formatting
is extremely useful if you are monitoring multiple runs, and you want to print
out various metrics such as time, accuracy, number of iterations, etc.

Remember that worksheets are just views on the underlying bundle graph, and the
lines that look like `[run sort-run ...]` are just pointers to a bundle.
Therefore, you can re-order, remove, duplicate the bundles in the worksheet,
and even move/copy bundles across worksheets as easily as text editing.

Note that deleting a reference to a bundle (in CodaLab parlance, detaching a bundle from a worksheet)
does not actually delete the bundle.
To delete an actual bundle, type the following command:

    cl rm sort-run

See the [CodaLab markdown documentation](Worksheet-Markdown) for more
information about the formatting.

### Step 4: Finding / browsing content in CodaLab

One of the benefits of CodaLab is it provides a global ecosystem for sharing
code, data, and results.  That is, once someone puts their content in CodaLab,
anyone else can find and build on top of it easily.

Click on **Public Home** on navigation bar to see the list of executable
papers, datasets in CodaLab.  Click on **My Dashboard** to see your own bundles
and worksheets.

You can search for bundles and worksheets using keywords.  Click on the web
terminal (the `CodaLab>` prompt) at the top of the screen and type in a command
for finding bundles:

    search <bundle keywords>                    # General form
    search mnist                                # Find all bundles matching "mnist"
    search .mine .limit=20 created=.sort-       # List your most recent bundles
    search .mine .floating                      # List your bundles not on any worksheet
    search .last                                # List the latest bundles

and finding worksheets:

    wsearch <worksheet keywords>                # General form
    wsearch acl2016                             # Find all worksheets matching "acl2016"

Look at the [CLI reference](CLI-Reference) for more information.

### Summary

For more information:

    cl help     # Print out the list of all commands
    cl help rm  # Print out usage for a particular command

The CLI has many more features; see the [reference](CLI-Reference) for more details.
