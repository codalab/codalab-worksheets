# CodaLab Bundle Service [![Build Status](https://travis-ci.org/codalab/codalab-cli.png?branch=master)](https://travis-ci.org/codalab/codalab-cli)

The goal of CodaLab is to faciliate transparent, reproducible, and
collaborative research in computation- and data-intensive areas such as machine
learning.  Think Git for experiments.  This repository contains the code for
the CodaLab Bundle Service and provides the foundation on which the [CodaLab
website](https://github.com/codalab/codalab) is built.

The CodaLab Bundle Service allows users to create *bundles*, which are
immutable directories containing code or data.  Bundles are either
uploaded or created from other bundles by executing arbitrary commands.
When the latter happens, all the provenance information is preserved.  In
addition, users can create *worksheets*, which interleave bundles with
free-form textual descriptions, allowing one to easily describe an experimental
workflow.

This package also contains a command-line interface `cl` that provides flexible
access to the CodaLab Bundle Service.  The [CodaLab
website](https://github.com/codalab/codalab) provides a graphical interface to
the service, as well as supporting competitions.

## Installation

Skip this section if your administrator has already installed CodaLab for you.

1. Make sure you have the dependencies (Python 2.7 and virtualenv).  If you're running Ubuntu:

        sudo apt-get install python2.7 python2.7-dev python-virtualenv

2. Clone the CodaLab repository:

        git clone https://github.com/codalab/codalab-cli
        cd codalab-cli

3. Run the setup script (will install things into a Python virtual environment):

        ./setup.sh

4. Set your path to include CodaLab for convenience (add this line to your `.bashrc`):

        export PATH=$PATH:<your path>/codalab-cli/codalab/bin

Now you are ready to start using CodaLab!

## Filesystem analogy

CodaLab is structured much like a classic operating system, so it's useful to
keep the following analogy in mind:

- shell = CodaLab session (usually identified by the process ID of the shell)
- drive = CodaLab instance (e.g., `http://localhost:2800`)
- directory = CodaLab worksheet (e.g., `codalab`)
- file = CodaLab bundle (e.g., `stanford-corenlp`)
- line in a file = CodaLab target (e.g., `stanford-corenlp/src`)

There are some differences, however:

- The contents of bundles are immutable (only the metadata is mutable), whereas
  files on a filesystem are mutable.
- A worksheet contains bundles in a user-specified order interleaved with text,
  whereas a directory in a file system contains an unordered set of files.
- CodaLab maintains the provenance information for each bundle.

## Basic Local Usage

### Orienting oneself

Print out the list of available commands:

    cl

Print out options for a specific command (e.g., upload):

    cl upload -h

Each shell is associated with a CodaLab session.  To get the status of the
current session (like running `pwd`):

    cl status

Your session is associated with a session ID, which identifies the current
address/worksheet pair (usually, by default, address will be 'local' and
worksheet is 'codalab').

For reference, your CodaLab settings here:

    ~/.codalab/config.json

The session state and authentication tokens are stored here:

    ~/.codalab/state.json

By default, the metadata is stored in a SQLite database (you should switch to a
real database such as MySQL if you're going to do anything serious):

    ~/.codalab/bundle.db

All the bundles corresonding to the `local` address are stored here:

    ~/.codalab/data

Let's walk through a simple example to demonstrate the capabilities of
CodaLab.  The goal is to sort a file.

### Uploading bundles

To use CodaLab, you first need to create bundles.  You can do this by uploading
a bundle from your filesystem into a CodaLab instance (identified by an address
such as `local`).

Let's create an example dataset bundle to upload:

    echo -e "foo\nbar\nbaz" > a.txt

Upload the dataset into CodaLab as follows.  (The `--edit` (or `-e`) will pop
up a text editor to allow you to edit the metadata of this bundle.  You are
encouraged to fill this information out!)

    cl upload dataset a.txt --edit

After you quit the editor, a 32-character UUID will be printed.  This UUID
uniquely identifies the Bundle.  Forever.  You can't edit the contents since
bundles are immutable, but you can go back and edit the metadata:

    cl edit a.txt

To list the bundles (one so far) you've uploaded, type:

    cl ls

You can see the statistics about the bundle:

    cl info -v a.txt

Let's now create and upload the sorting program:

    echo -e "import sys\nfor line in sorted(sys.stdin.readlines()): print line," > sort.py
    cl upload program sort.py

Note that while `a.txt` and `sort.py` are dataset and programs, respectively,
they are both just bundles and are treated basically the same.  They only
differ in their metadata.  Also, note that while we've uploaded files, bundles
can be directories too.

### Creating runs

One can upload program and dataset bundles, but the interesting part about
CodaLab is that new bundles can be generated by running bundles.  A *run*
bundle consists of a set of dependencies on existing bundles and an arbitrary
*command* to execute.  When CodaLab runs this command behind the scenes, it
makes sure the dependencies are put into the right place.

Let us create our first run bundle:

    cl run sort.py:sort.py input:a.txt 'python sort.py < input > output' --name sort-run

The first two arguments specify the dependencies and the third is the command.
Each dependency has the form `<key>:<target>`; think of it as creating a
symlink called `<key>` pointing to `<target>`.  The target can be a bundle (e.g., `a.txt`),
or if the bundle is a directory rather than a file, we can references files
inside (e.g., `a.txt/file1`). During the run, targets are read-only.

Note that `cl run` doesn't actually run anything; it just creates the run
bundle and returns immediately.  You can see by doing `cl ls` that it's been
created, but it's state is `created`, not `ready`.  (You can add `-t` or
`--tail` to make `cl run` block and print out stdout/stderr, more like how you
would normally run a program.)

Look inside the bundle:

    cl info sort-run

You'll see that like any bundle, it consists of a set of files and directories.
Under *dependencies*, you will see two files (*keys*), `sort.py` and `input`,
which point to the *targets* `sort.py` and `a.txt`.

Note that runs need not have dependencies.  Here's a trivial run bundle that
doesn't:

    cl run 'echo hello'

Now let's actually execute these run bundles.  In general, a CodaLab instance
would already have workers constantly executing run bundles, but we're running
locally, so we have to start up our own worker.  Run this in another shell:

    cl worker

(See `~/.codalab/config.json` to customize the worker.)  You should see that this
shell immediately executes the run.  In our original shell, we can check that
the run completed successfully.

    cl info -v sort-run

We can look at individual targets inside the bundle:

    cl cat sort-run/output

To make things more convenient, we can define a bundle that points to a target
inside the last bundle:

    cl make sort-run/output --name a-sorted.txt
    cl cat a-sorted.txt

We can also download the results to local disk:

    cl download a-sorted.txt

If you messed up somewhere, you can always remove a bundle:

    cl rm sort-run

You'll see that the above command threw an error, because `a-sorted.txt`
depends on `sort-run`.  To delete both bundles, you can remove recursively:

    cl rm -r sort-run

Note: be *very careful* with `rm -r` because it might delete a lot of bundles!

#### Sugar

You can also include bundle references directly in your run command, which
might be more natural than listing the dependencies ahead of time:

    cl run 'python %sort.py% < %a.txt% > output' --name sort-run
    cl run 'python %arg1:sort.py% < %arg2:a.txt% > output' --name sort-run
    cl run 'python %:sort.py% < %:a.txt% > output' --name sort-run

These are equivalent to the following, respectively:

    cl run 1:sort.py 2:a.txt 'python 1 < 2 > output' --name sort-run
    cl run arg1:sort.py arg2:a.txt 'python arg1 < arg2 > output' --name sort-run
    cl run sort.py:sort.py a.txt:a.txt 'python sort.py < a.txt > output' --name sort-run

Note that the last line is also equivalent to:

    cl run :sort.py :a.txt 'python sort.py < a.txt > output' --name sort-run

### Macros

Once we produce a run, we might want to do it again with slightly different
settings (e.g., sort another example).  CodaLab macros allow you to do this,
although understanding this concept requires us to take a step back.

In CodaLab, bundles form a directed acyclic graph (DAG), where nodes are
bundles and a directed edge from A to B means that B depends on A.  Imagine we
have created some runs that produces some output bundle O from some input
bundle I; I is an ancestor of O in the DAG.  Now suppose we have a new input
bundle I', how can we produce the analogous O'.  The *mimic* command does
exactly this.

First, recall that we have created `a.txt` (I) and `sort-run` (O).  Let us
create another bundle and upload it:

    echo -e "6\n3\n8" > b.txt
    cl upload dataset b.txt

Now we can apply the same thing to `b.txt` that we did to `a.txt`:

    cl mimic a.txt a-sorted.txt b.txt --name b-sorted.txt

We can check that `b.txt.sorted` contains the desired sorted result:

    cl cat b-sorted.txt

Normally, in a programming language, we define macros as abstractions.  In
CodaLab though, notice that we've started instead by creating a concrete
example, and then used an analogy to re-apply this.  A positive side-effect is
that every macro automatically comes with an example of how it is used!

We can make the notion of a macro even more explicit.  Let's rename `a.txt` to
`sort-in1` and `a-sorted.txt` to `sort-out`:

    cl edit a.txt --name sort-in1
    cl edit a-sorted.txt --name sort-out

Then we can use the following syntactic sugar:

    cl macro sort b.txt --name b-sorted.txt

In CodaLab, macros are not defined ahead of time, but are constructed on the
fly from the bundle DAG.

### Worksheet basics

So far, every bundle we've created has been added to the `codalab` worksheet.
Recall that a worksheet is like a directory, but we can do much more.  We can
edit the worksheet:

    cl wedit

In the popped up editor, we can enter arbitrary text interleaved with the
bundles that we have created so far.  Try adding some text, saving, and exiting
the editor.  Then we can display the contents of this worksheet in a more
rendered fashion.

    cl print

We can add another worksheet:

    cl new scratch

This adds a link from `codalab` to `scratch` and a link from `scratch` to `codalab`.
A worksheet on another worksheet, like a bundle, is just a pointer.  The container
worksheet does not "own" its items.  To see this:

    cl print codalab
    cl print scratch

We can switch between worksheets (analogous to switching directories using `cd`):

    cl work codalab
    cl work scratch

We can add items (text or bundles) to a worksheet:

    cl add -m "Here's a simple bundle:"
    cl add sort.py
    cl print

Another way to add bundles to a worksheet is to use `cl wedit` and entering
additional lines:

    {sort.py}

If you save, exit, and open up the worksheet again, you'll see that the
reference has been resolved.  In general, editing the worksheet with a text
editor gives you a lot of flexibility for organizing bundles.

To remove the worksheet:

    cl wrm scratch

Note that the bundles and worksheets linked to in the worksheet are not
deleted.

### Referencing bundles

So far, we have referred to bundles by their names, which have been unique.  In
a large CodaLab system with many users, names are not unique, not even within
the same worksheet.  A *bundle_spec* refers to the string that identifies a
bundle, importantly given the context (address, current worksheet).

There are finally a number of other ways to reference bundles:

- UUID (`0x3739691aef9f4b07932dc68f7db82de2`): this should match at most one
  bundle.
- Prefix of UUID (`0x3739`): matches all bundles whose UUIDs start with this
  prefix.
- Name prefix (`foo`): matches all bundles with the given name.
  You can use `foo%` to match bundles that begin with `foo` or `%foo%` to match
  bundles that contain `foo` (SQL LIKE syntax).
- Ordering (`^, ^2, ^3`): returns the first, second, and third last bundles on
  the current worksheet.
- Named ordering (`foo^, foo^2, foo^3`): returns the first, second, and third
  last bundles with the given name on the current worksheet.

Each of the above matches some number of bundles.  Exactly one is chosen based
on the following rules (in order of precedence):

1. Bundles in the current worksheet are preferred to those not.
2. Later bundles are preferred.

In practice, `^` and `^2` are used frequently because future operations tend to
depend on the bundles you just created.

### Displaying worksheets

A worksheet contains an ordered list of *items*, which are either bundles,
worksheets, text, and *directives* (so far, we've seen only the first three).
Directives tell the `cl print` command how to render bundle items.

When you do `cl wedit`, the beginning of the document marked with comments
('//') give some documentation about what directives do, but we will talk about
them in more detail now.

All directives have the following form:

    % <command> <arg-1> ... <arg-n>

The command and arguments are separated by spaces, and an argument with spaces
should be quoted.

The `title` directive sets the title of a worksheet:

    % title "My Worksheet"

The `display` directive changes the way all bundles from that point on are
displayed (until the next `display` directive).  The general form is:

    % display <mode> <arg>

Here are the specific instances.  To hide the bundle completely:

    % display hidden

To display only a link to the bundle with the given anchor text:

    % display link "this program"

### Displaying parts of bundles

We can also display the contents of bundles using either the inline, contents,
image, or html modes, all of which take one argument `<genpath>`, which is a
*generalized path*, which specifies either a metadata field of the bundle
(e.g., name, command) or a (part of a) file/directory inside the bundle.

The `inline` mode just prints out the corresponding value.  Here, `uuid` and
`command` are the metadata values.  You can do `cl info -r <bundle_spec>` to
see the available `<genpath>`s.

    % display inline uuid
    % display inline command
    % display inline /output/stats:errorRate

The third example is interesting.  Here, `/output/stats` is a file inside the
bundle (the leading '/' is required).  The colon signals that we don't want
to display the entire file, but only part of it.  Here, we are assuming that the file
is either a JSON file:

    {"errorRate": 0.2, "method": "simple"}

a YAML file,

    errorRate: 0.2
    method: simple

or a tab-separated file,

    errorRate   0.2
    method	    simple

If you have a nested JSON dictionary, you can access it with
`/output/stats:train/errorRate`:

    {"train": {"errorRate": 0.2}}

For directories or large files, use `contents` rather than `inline`:

    % display contents /stdout

If your file is an image or HTML file, then you can tell CodaLab to render it
directly.  Note that these two modes are only really useful for displaying on
the webpage.

    % display image /graph.png
    % display html /visualization.html

### Displaying records and tables

The final two modes are `record` and `table`.  Both display a bundle as a set
of key-value pairs.  Like `inline`, each key-value pair is based on a `<genpath>`.

A *schema* specifies the set of key-value pairs to be printed out.  A schema is
created as follows:

    % schema <schema-name>
    % addschema <schema-name>
    % add <key-name> <genpath> [<post-processor>]
    % ...

A number of standard schemas are defined: `program`, `dataset`, `run`, and
`default`.

For example:

    % schema my-run
    % addschema run
    % add error /stats:errorRate %.3f
    % add method /options.map:method "s/-method// | [0:5]"

This creates a new schema called `my-run` which contains all the key-value
pairs from the standard schema `run`, and adds two more with keys `error` and
`method`.  The `<genpath>` is the same format as we described above for
`inline`.  The `<post-processor>` is optional, and is a sequence (separated by " |
"; note the spaces before and after the pipe are important) of primitive
post-processors, which are applied, one after the other.  There are four types
of primitive post-processors:

- `duration`, `date`, `size` for special formatting
- `%...` for sprintf-style formatting
- `s/<old>/<new>` for regular expression substitution
- `[<start index>:<end index>]` for taking substrings

Having defined a schema, you can enable it for all following bundles by:

    % display record <schema-name> ... <schema-name>
    % display table <schema-name> ... <schema-name>

A *record* displays each bundle separately, where each row is a key-value pair,
like the `cl info` output.

A *table* groups all the consecutive bundles immediately following into one
table where each row is a bundle, each column is a key, and each cell is the
corresponding value.

### Executing commands

When editing a worksheet, you can specify CLI commands to execute right in the worksheet.
For example, if you add `!rm ^`` after a bundle:

    {0xd84059a97bf544e89f18344221faa212}
    !rm ^

Then when you save the worksheet, then it is as if the following command was executed:

    cl rm 0xd84059a97bf544e89f18344221faa212

One common use case of these embedded commands is to move bundles around and
mark some as ones to `rm` or `kill`.

The same thing works for any CLI command.

## Working remotely

So far, we have been doing everything locally, but one advantage of CodaLab is
to have a centralized instance so that both the data and the computational
resources can be shared and scaled.

In the following, you can use `codalab` for `<username>`.

### Connecting to an existing server

Usually, a CodaLab instance will already be set up and you can just connect to
it directly.  For example, suppose a CodaLab instance is running at
`http://example.com:2800`.  Then you can connect to it by typing:

    cl work http://example.com:2800::<username>

This sets your current worksheet to `<username>` (think directory in the filesystem
analogy) on the remote machine (think drive).  The general form is:

    cl work <address>::<worksheet>

It is convenient to create an alias so you don't have to type the address every time:

    cl alias ex http://example.com:2800

so you now just need to type:

    cl work ex::<username>

### Starting your own server

Setting up a CodaLab instance for you or your group, you can do the following
in another shell:

    cl server

By default, the server is running at `http://localhost:2800`.  You can change
this in `~/.codalab/config.json`.

For security reasons, the server is only accessible from localhost.  To make
the server accessible from anywhere, under "server" / "host" in
`~/.codalab/config.json`, change "localhost" to "".  In this case, you should
change the authentication from `MockAuthHandler` to `OAuthHandler` or else you
will have a bad security vulnerability.

Now we can connect to this server:

    cl work http://localhost:2800::<username>

or

    cl alias localhost http://localhost:2800
    cl work localhost::<username>

This will prompt you for your password.  By default, the username is "root" and
the password is "".

To switch back to the `local` instance, type:

    cl work local

Note that when you are on `localhost`, you are accessing the same CodaLab
instance as when you are on `local`, but all requests go through the network,
which means that this CodaLab instance can be accessed from another machine.

### Copying between instances

It is easy to copy bundles and other worksheet items between instances.
To illustrate this concept, let us create a separate CodaLab instance:

    export CODALAB_HOME=~/.codalab2
    cl ls

You should see that there's nothing there because we are now accessing the new
CodaLab instance backed by `~/.codalab2` rather than `~/.codalab`.  In this shell,
the situation is as follows:

    local => ~/.codalab2
    localhost => ~/.codalab

We can copy bundles between CodaLab instances by doing:

    cl cp localhost::a.txt local

Now there are two physical copies of the bundle `a.txt`, and they have the same
bundle UUID.  We can create a bundle and copy it in the other direction too:

    echo hello > hello.txt
    cl upload dataset hello.txt
    cl cp hello.txt localhost

Note that currently, if a bundle has dependencies, `cl cp` does not copy these
dependencies.  In the future, you should be able to do this with `cl cp -r`.

In general, the `cp` command is as follows:

    cl cp [<address>::]<bundle> [<address>::]<worksheet>

To copy all the items from a worksheet (except worksheet items) to another:
    
    cl wcp [<address>::]<worksheet> [<address>::]<worksheet>

Note that worksheets themselves are not copied, just the items within a
worksheet.  Any bundles that don't exist on the destination CodaLab instance
are copied over.

## Using MySQL

By default, CodaLab is configured to use SQLite, and the database file is just a single
file in `~/.codalab`.  While this is a quick way to get started, SQLite is not a very
scalable solution (and also doesn't handle database migrations properly, so
don't put valuable stuff in a SQLite-backed database). Here are instructions to
set up MySQL:

Install the MySQL server.  On Ubuntu, run:

    sudo apt-get install mysql-server

Install the MySQL Python:

    venv/bin/pip install MySQL-python

In the configuration file `.codalab/config.json`,
change `"class": "SQLiteModel"` to

    "class": "MySQLModel",
    "engine_url": "mysql://<username>:<password>@<host>:<port>/<database>",

For example:

    "engine_url": "mysql://codalab@localhost:3306/codalab_bundles",

If you already have data in SQLite, you can load it into MySQL as follows:

    sqlite3 ~/.codalab/bundle.db .dump > bundles.sqlite
    python scripts/sqlite_to_mysql.py < bundles.sqlite > bundles.mysql 
    mysql -u codalab -p codalab_bundles < bundles.mysql

Once you set up your database, run the following so that future migrations
start from the right place (this is important!):

    venv/bin/alembic stamp head

## Authentication

If you want to make your server public, then you need to set up OAuth
authentication.  Follow the instructions in the Linux Quickstart section of the
[CodaLab website
README](https://github.com/codalab/codalab/blob/master/README.md).

### Permissions

CodaLab implements the following permissions model:

- Each user belongs to some groups.
- Each group has access to some worksheets.
- Each worksheets contains some bundles.

There are three levels of access or permission:

- None: You can't even see that the worksheet exists.
- Read: You can read/download, but not edit.
- All: You can do anything (edit/delete/etc.).

A user can read a bundle if there exists a group that contains that user, a
worksheet that the group has read permission on and also contains that bundle.

Currently, only the owner of a bundle has all permission (this can be relaxed
to groups later).

Notes:

- There is a designated `public` group that contains all users implicitly.  If
  you want to make a worksheet world-readable, give the `public` group read
  permission.
- There is a designated root user (`codalab`) that has *All* permission to
  everything.
- You automatically have *All* permission to all worksheets you own.

To make a worksheet `w1` public:

    cl wperm w1 public read

To make a worksheet `w1` mutually-writable with your research group, first create
the group `g1`, add users `u1` and `u2` to it, and then give the group all access:

    cl gnew g1
    cl uadd u1 g1
    cl uadd u2 g1
    cl wperm w1 g1 all
    
To list the groups that you've created or belong to:

    cl gls

To look more into a given group `g1`:

    cl ginfo g1

## Updating CodaLab

To update to the newest version of CodaLab, run:

    git pull

When you do this, the database schema might have changed, and you need to
perform a *database migration*.  To be on the safe side, first backup your
database.  Then run:

    venv/bin/alembic upgrade head

# User tips

The following describes some common tip and tricks to make the most out of CodaLab.

Delete the last five bundles (remember this also removes all other instances of
these bundles, so be careful):

    cl rm ^1-5

To kill the last running bundle:

    cl kill ^

To search for bundles by keyword:

    cl search a.txt

Most CodaLab commands generate one or more bundle UUIDs.  These can be piped to
further commands.  To kill all running bundles (be careful!):

    cl search state=running | xargs cl kill

To delete all "orphaned" bundles that do not appear on a worksheet (be careful!):

    cl search orphan -u | xargs cl rm

To run a bundle and create another bundle that depends on it:

    cl make $(cl run date)/stdout --name stdout

To wait for the last bundle to finish and then print out its output:
    
    cl run 'sleep 10; date'
    cl cat $(cl wait ^)/stdout

To find out what happened to the last bundle (e.g., why it failed):

    cl info -v ^

To rerun the last bundle (`-f args` prints out the command that was used to
generate the bundle):

    cl info -f args ^ | xargs cl

To put the command of a bundle back on the command-line for editing, create
this handy alias in bash:

    clhist() {
      history -s cl $(cl info -f args $1)
    }

Dependent bundles are read-only during a run, so to change files or
add to a dependent directory, everything must first be copied. Example
of compiling a source tree as a run bundle:

    cl run :src 'cp -r src src-build && cd src-build && make'

To compare two worksheets:

    vimdiff <(cl print -r worksheet1) <(cl print -r worksheet2)

To replace the contents of worksheet2 with worksheet1 (be careful when we do
this, since all the contents of worksheet1 are removed, although the bundles
themselves are not removed and will be orphaned):

    cl wedit -f /dev/null -w worksheet2
    cl wcp worksheet1 worksheet2

## Editing worksheets

By default, you will use `cl wedit` to edit worksheets.  However, it is
convenient to just keep a text editor open.  Here's one way to do this.

1. Save the contents of a worksheet to a local file:

        cl print -r codalab > codalab.ws

2. Edit `codalab.ws`.

3. Save the worksheet back into CodaLab:

        cl wedit codalab -f codalab.ws

It is useful to define editor macros to execute the first and third commands.
For example, in vim, you could define a *save* and *load* command by adding the
following two lines to your `.vimrc`:

    map mk :wa<CR>:!cl wedit % -f %<CR>
    map mr :wa<CR>:!cl print -r % > %<CR>

The file that you load is in general not identical to the one you save (because
references get interpreted and commands get executed), so it's a good idea to
load right after you save.

Also, if you add bundles to the worksheet on the CLI, then you should reload
the worksheet before you make edits or else you will lose those changes.

# For developers

Here are some helpful links:

- [CodaLab instance](http://codalab.org/)
- [GitHub site](http://codalab.github.io/codalab/)
- [GitHub repository](https://github.com/codalab/codalab)
- [Codalab Wiki](https://github.com/codalab/codalab/wiki)

## Code design

The main components of the CodaLab CLI are as follows:

- Command-line interface (`bundle_cli.py`): main entry point, talks to a `BundleClient`.
  This should be the only code that prints to stdout and is command-line specific.

- `LocalBundleClient` and `RemoteBundleClient`.  All the functionality here
  should be used by the CLI and the website.  `LocalBundleClient` does most of the work;
  `RemoteBundleClient` mostly forwards its requests to the `LocalBundleClient`
  with a few exceptions which require more involved access to CodaLab
  (uploading/downloading files).

- `bundle_model.py`: in charge of updates to the database

- `bundle_store.py`: in charge of updates to the filesystem

- `work_manager.py`: manages workers/execution

[TODO]

Bundle hierarchy:

    Bundle
      NamedBundle
        UploadedBundle
          ProgramBundle
          DatasetBundle
        MakeBundle [DerivedBundle]
        RunBundle [DerivedBundle]

## Unit tests

To run tests on the code, first install the libraries for testing:

    venv/bin/pip install mock nose

Then run all the tests:

    venv/bin/nosetests

## Database migrations

Migrations are handled with [Alembic](http://alembic.readthedocs.org/en/latest/).

If you are planning to add a migration, please check whether:

* You have a fresh DB with no migrations, or
* You have already done a migration and wish to add/upgrade to another.

By running this command:

    venv/bin/alembic current

If you have a migration, it will show you your last migration (head).  (In this
case it's `341ee10697f1`.)

    INFO  [alembic.migration] Context impl SQLiteImpl.
    INFO  [alembic.migration] Will assume non-transactional DDL.
    Current revision for sqlite:////Users/Dave/.codalab/bundle.db: 531ace385q2 -> 341ee10697f1 (head), name of migration

If the DB has no migrations and is all set, the output will be:

    INFO  [alembic.migration] Context impl SQLiteImpl.
    INFO  [alembic.migration] Will assume non-transactional DDL.
    Current revision for sqlite:////Users/Dave/.codalab/bundle.db: None

##### You have a fresh DB with no migrations.

Simply stamp your current to head and add your migration:

    venv/bin/alembic stamp head

##### You have already done a migration and wish to upgrade to another.

    venv/bin/alembic upgrade head

[TODO write about edge cases]

### Adding a new migration

1. Make modifications to the database schema in `tables.py`.

2. If necessary, update COLUMNS in the corresponding ORM objects (e.g., `objects/worksheet.py`).

3. Add a migration:

        venv/bin/alembic revision -m "<your commit message here>" --autogenerate

This will handle most use cases but **check the file it generates**.  If it is
not correct please see the [Alembic
Docs](http://alembic.readthedocs.org/en/latest/tutorial.html#create-a-migration-script)
for more information on the migration script.

4. Upgrade to your migration (modifies the underlying database):

        venv/bin/alembic upgrade head

## Execution using docker

Every execution on CodaLab (should ideally) happen in a
[docker](https://www.docker.com/) container, which provides a standardized
Linux environment that is lighterweight than a full virtual machine.

The current official docker image is `codalab/ubuntu`, which consists of
Ubuntu 14.04 plus some standard packages.  See the [CodaLab docker
registery](https://registry.hub.docker.com/u/codalab/ubuntu/).

To install docker on your local machine (either if you want see what's actually
in the environment or to run your own local CodaLab instance), follow these
[instructions](http://docs.docker.com/installation/ubuntulinux/):

    sudo sh -c "echo deb https://get.docker.io/ubuntu docker main > /etc/apt/sources.list.d/docker.list"
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9
    sudo apt-get update
    sudo apt-get install lxc-docker
    sudo useradd $USER docker

Then, to test out your environment, open a shell (the first time you do this,
it will take some time to download the image):

    docker run -t -i codalab/ubuntu:1.1

Now, let us integrate docker into CodaLab.  First, we need to setup a job
scheduling system (that manages the deployment of runs on machines).  Note that
CodaLab itself doesn't do this, so that it can be easily integrated into
different systems.  An easy way to set this up is to use `q` from Percy Liang's `fig` package:

    git clone https://github.com/percyliang/fig
    # Add fig/bin/q to your $PATH
    q -mode master   # Run in a different terminal
    q -mode worker   # Run in a different terminal

Now, let us tell CodaLab to use `q` and run things in docker (these two things
are orthogonal choices).  Edit the `.codalab/config.json` as follows:

    "workers": {
        "q": {
            "verbose": 1,
            "max_instances": 10,
            "docker_image": "codalab/ubuntu:1.1"
            "dispatch_command": "python $CODALAB_CLI/scripts/dispatch-q.py"
        }
    }

To test it out:

    cl worker                            # Run in a different terminal
    cl run 'cat /proc/self/cgroup' -t    # Should eventually print out lines containing the string `docker`
