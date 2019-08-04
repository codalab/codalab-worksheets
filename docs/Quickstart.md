In five minutes, you will be able to upload a program, a dataset, and execute
your first run in CodaLab.

First, sign up for a CodaLab account at
[worksheets.codalab.org](https://worksheets.codalab.org), and sign in.  Then
click on **`My Home`** in the top-right navigation bar.  This takes to your
home worksheet (named something like `home-pliang`), which you should think of
as your home directory.

Now we will run through a simple example in which you will sort a file in CodaLab.

### Step 1: Upload files

Create a file on your local computer called `a.txt` with the following contents
(using your favorite editor):

    foo
    bar
    baz

Create another file called `sort.py` with the following contents:

    import sys
    for line in sorted(sys.stdin.readlines()):
        print line,

Click on the `Upload` button on the top of the side panel
(this button may not appear if your screen width is too small; increase the
width of your screen and it should appear).
This pops up a file browser; select `a.txt`.
Repeat the same thing for `sort.py`.

When each file is uploaded, it is appended to the current worksheet as a **bundle**.
The bundle shows up as a row in a table (later, we'll see how to change the formatting).
Click on the row to select it; more information about the bundle will appear
on the right side panel, including the **metadata** as well as the contents of the file.

Each bundle has a 32-character **UUID**, a globally unique identifier, which
can be used to unambiguously refer to that bundle forever.
You can send someone the UUID and they will know exactly what you're referring to.

While the contents of the bundle are immutable, you can edit its metadata
(name, description, etc.) by clicking on the fields in the side panel.

While `a.txt` is a dataset and `sort.py` is a program, from CodaLab's
perspective, these are just bundles.

Bundles can also be directories.  To upload a directory, zip (or tar) up the
directory and upload the zip file.  The zip file will be unpacked
automatically.  If you use the [command-line interface (CLI)](CLI-Basics),
you can upload directories directly.

### Step 2: Run a command

Already CodaLab provides a nice place to share code and data, but the true power of CodaLab
is the ability to operate on these bundles.
In particular, CodaLab allows you to run arbitrary shell commands,
which really gives you the freedom to do whatever computation you want.

To do this, click `New Run` on the top of the side panel.
Select `sort.py` and `a.txt` as the dependencies (Step 1), and enter the
following command (Step 2):

    python sort.py < a.txt

Click `Run`.  This should append a new (run) bundle to the current worksheet,
which encapsulates the computation.
If you wait a second, the state of the bundle will turn from `created` to
`staged` to `running` to `ready`.  You should see the `stdout` on the right
panel show the resulting output:

    bar
    baz
    foo

You have successfully run your first experiment in CodaLab.
To explain what just happened, CodaLab just ran your command in a [sandbox
environment](Execution) in a [docker container](https://www.docker.com).
From the point of view of your command,
you're sitting in some temporary directory that contains the dependencies you
specified.
It's like you did this:

    $ ls
    sort.py
    a.txt
    $ python sort.py < a.txt

All the files and directories that are written to the current directory
(including stdout and stderr, which are written as files)
are saved as the contents of the newly-created
run bundle.  The contents are changing as the command is executing, but once
the command terminates, the contents of the bundle become immutable.  Files
that you write elsewhere are not stored.  So a common convention in CodaLab is
to have your commands simply write into the current directory.

**Libraries**.  If your command depends on custom libraries (e.g., TensorFlow),
they need to be installed in the docker image.  You can use an existing docker
container or [build your own docker image](Execution).

**Parallelism**.  You can start off multiple runs in parallel, and even ones
that depend on previous runs that haven't yet finished.  Since CodaLab knows
about dependencies, it will wait for all the dependencies of a run to finish
before starting the run.

**Dependencies**.  You should think about the current worksheet as your current
directory, where you're running commands that generate new bundles.
However, you must be explicit about dependencies!  CodaLab will run your
command only in the presence of the dependencies you explicitly specify, not
all the bundles in the current worksheet.

**Operations**.  If you made a mistake, you can kill the process for
your bundle by right-clicking on the corresponding row in the table and
selecting **Kill this run bundle**.  You can remove non-running bundles by selecting
**Remove bundle permanently**.

Note that whenever you run a bundle or perform an operation, a command is sent
to the **web terminal** (`CodaLab>` prompt) at the top of the screen.  This
terminal allows you to use most of the [command-line interface
(CLI)](CLI-Basics) commands.  Type `help` to get more information.

### Step 3: Present your results

So far, your worksheet just contains a table with one bundle per row.
But we can customize this view to better document and present our results.  To
edit the worksheet, click `Edit Source` button (or hitting `e`).

You will be taken to an editor in your browser with plain-text markdown source of the
worksheet.  [CodaLab markdown](Worksheet-Markdown) is an extension of markdown
that allows you to interleave usual markdown with bundles and formatting directives.

You can edit the source freely.  For example, you might edit the source to look like this
(note that your worksheet will have different UUIDs):

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
Note that deleting a reference to a bundle does not actually delete the bundle;
it merely **detaches** it.

### Step 4: Finding / browsing content in CodaLab

One of the benefits of CodaLab is it provides a global ecosystem for sharing
code, data, and results.  That is, once someone puts their content in CodaLab,
anyone else can find and build on top of it easily.

Click on **Public Home** on navigation bar to see the list of executable
papers and datasets in CodaLab.  Click on **My Dashboard** to see your own bundles
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

Congratulations - you have successfully used CodaLab to upload a program, a dataset,
and performed a run!  In practice, you might have more [complex
workflows](Workflow), running commands to preprocess the data, running
algorithms with different settings, using CodaLab to
manage your runs.
If you prefer working from the shell, check out the [command-line interface (CLI)](CLI-Basics).
[CodaLab markdown](Worksheet-Markdown)
is a powerful way to create worksheets that document your
experiments, either for a private research log or a public executable paper.
There are many possibilities!
