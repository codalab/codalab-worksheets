Having gone over the [basics of the command-line interface (CLI)](CLI-Basics),
let us provide a more complete picture of the capabilities of the CLI, along
with more details about the general structure of CodaLab.  Note that many
(though not all) of the commands work in the web terminal (the `CodaLab>`
prompt at the top of the web interface).

For a complete list of CLI commands, type:

    cl help -v

Here is the output:

    CodaLab CLI version 0.1.8
    Usage: cl <command> <arguments>

    Commands for bundles:
      upload (up):
        Create a bundle by uploading an existing file/directory.
          upload <path>            : Upload contents of file/directory <path> as a bundle.
          upload <path> ... <path> : Upload one bundle whose directory contents contain <path> ... <path>.
          upload -c <text>         : Upload one bundle whose file contents is <text>.
          upload <url>             : Upload one bundle whose file contents is downloaded from <url>.
          upload                   : Open file browser dialog and upload contents of the selected file as a bundle (website only).
        Most of the other arguments specify metadata fields.
        Arguments:
          path                    Paths (or URLs) of the files/directories to upload.
          -c, --contents          Specify the string contents of the bundle.
          -L, --follow-symlinks   Always dereference (follow) symlinks.
          -x, --exclude-patterns  Exclude these file patterns.
          -g, --git               Path is a git repository, git clone it.
          -p, --pack              If path is an archive file (e.g., zip, tar.gz), keep it packed.
          -w, --worksheet-spec    Upload to this worksheet ([<alias>::|<address>::](<uuid>|<name>)).
          -n, --name              Short variable name (not necessarily unique); must conform to ^[a-zA-Z_][a-zA-Z0-9_\.\-]*$.
          -d, --description       Full description of the bundle.
          --tags                  Space-separated list of tags used for search (e.g., machine-learning).
          --license               The license under which this program/dataset is released.
          --source-url            URL corresponding to the original source of this bundle.
          -e, --edit              Show an editor to allow editing of the bundle metadata.

      make:
        Create a bundle by combining parts of existing bundles.
          make <bundle>/<subpath>                : New bundle's contents are copied from <subpath> in <bundle>.
          make <key>:<bundle> ... <key>:<bundle> : New bundle contains file/directories <key> ... <key>, whose contents are given.
        Arguments:
          target_spec                  [<key>:](<uuid>|<name>)[/<subpath within bundle>]
          -w, --worksheet-spec         Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).
          -n, --name                   Short variable name (not necessarily unique); must conform to ^[a-zA-Z_][a-zA-Z0-9_\.\-]*$. (for makes)
          -d, --description            Full description of the bundle. (for makes)
          --tags                       Space-separated list of tags used for search (e.g., machine-learning). (for makes)
          --allow-failed-dependencies  Whether to allow this bundle to have failed dependencies. (for makes)
          -e, --edit                   Show an editor to allow editing of the bundle metadata.

      run:
        Create a bundle by running a program bundle on an input bundle.
        Arguments:
          target_spec                  [<key>:](<uuid>|<name>)[/<subpath within bundle>]
          command                      Arbitrary Linux command to execute.
          -w, --worksheet-spec         Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).
          -n, --name                   Short variable name (not necessarily unique); must conform to ^[a-zA-Z_][a-zA-Z0-9_\.\-]*$. (for runs)
          -d, --description            Full description of the bundle. (for runs)
          --tags                       Space-separated list of tags used for search (e.g., machine-learning). (for runs)
          --allow-failed-dependencies  Whether to allow this bundle to have failed dependencies. (for runs)
          --request-docker-image       Which docker image (e.g., codalab/ubuntu:1.9) we wish to use. (for runs)
          --request-time               Amount of time (e.g., 3, 3m, 3h, 3d) allowed for this run. (for runs)
          --request-memory             Amount of memory (e.g., 3, 3k, 3m, 3g, 3t) allowed for this run. (for runs)
          --request-disk               Amount of disk space (e.g., 3, 3k, 3m, 3g, 3t) allowed for this run. (for runs)
          --request-cpus               Number of CPUs allowed for this run. (for runs)
          --request-gpus               Number of GPUs allowed for this run. (for runs)
          --request-queue              Submit run to this job queue. (for runs)
          --request-priority           Job priority (higher is more important). (for runs)
          --request-network            Whether to allow network access. (for runs)
          -e, --edit                   Show an editor to allow editing of the bundle metadata.
          -W, --wait                   Wait until run finishes.
          -t, --tail                   Wait until run finishes, displaying stdout/stderr.
          -v, --verbose                Display verbose output.

      edit (e):
        Edit an existing bundle's metadata.
          edit           : Popup an editor.
          edit -n <name> : Edit the name metadata field (same for other fields).
        Arguments:
          bundle_spec           (<uuid>|<name>|^<index>)
          -n, --name            Change the bundle name (format: ^[a-zA-Z_][a-zA-Z0-9_\.\-]*$).
          -d, --description     New bundle description.
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).

      detach (de):
        Detach a bundle from this worksheet, but doesn't remove the bundle.
        Arguments:
          bundle_spec           (<uuid>|<name>|^<index>)
          -n, --index           Specifies which occurrence (1, 2, ...) of the bundle to detach, counting from the end.
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).

      rm:
        Remove a bundle (permanent!).
        Arguments:
          bundle_spec           (<uuid>|<name>|^<index>)
          --force               Delete bundle (DANGEROUS - breaking dependencies!)
          -r, --recursive       Delete all bundles downstream that depend on this bundle (DANGEROUS - could be a lot!).
          -d, --data-only       Keep the bundle metadata, but remove the bundle contents on disk.
          -i, --dry-run         Perform a dry run (just show what will be done without doing it).
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).

      search (s):
        Search for bundles on a CodaLab instance (returns 10 results by default).
          search <keyword> ... <keyword> : Match name and description.
          search name=<name>             : More targeted search of using metadata fields.
          search size=.sort              : Sort by a particular field.
          search size=.sort-             : Sort by a particular field in reverse.
          search size=.sum               : Compute total of a particular field.
          search .mine                   : Match only bundles I own.
          search .floating               : Match bundles that aren't on any worksheet.
          search .count                  : Count the number of bundles.
          search .limit=10               : Limit the number of results to the top 10.
        Arguments:
          keywords              Keywords to search for.
          -a, --append          Append these bundles to the current worksheet.
          -u, --uuid-only       Print only uuids.
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).

      ls:
        List bundles in a worksheet.
        Arguments:
          -u, --uuid-only       Print only uuids.
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).

      info (i):
        Show detailed information for a bundle.
        Arguments:
          bundle_spec           (<uuid>|<name>|^<index>)
          -f, --field           Print out these comma-separated fields.
          -r, --raw             Print out raw information (no rendering of numbers/times).
          -v, --verbose         Print top-level contents of bundle, children bundles, and host worksheets.
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).

      cat:
        Print the contents of a file/directory in a bundle.
        Note that cat on a directory will list its files.
        Arguments:
          target_spec           (<uuid>|<name>)[/<subpath within bundle>]
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).

      wait:
        Wait until a run bundle finishes.
        Arguments:
          target_spec           (<uuid>|<name>)[/<subpath within bundle>]
          -t, --tail            Print out the tail of the file or bundle and block until the run bundle has finished running.
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).

      download (down):
        Download bundle from a CodaLab instance.
        Arguments:
          target_spec           (<uuid>|<name>)[/<subpath within bundle>]
          -o, --output-path     Path to download bundle to.  By default, the bundle or subpath name in the current directory is used.
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).

      mimic:
        Creates a set of bundles based on analogy with another set.
          mimic <run>      : Rerun the <run> bundle.
          mimic A B        : For all run bundles downstream of A, rerun with B instead.
          mimic A X B -n Y : For all run bundles used to produce X depending on A, rerun with B instead to produce Y.
        Arguments:
          bundles               Bundles: old_input_1 ... old_input_n old_output new_input_1 ... new_input_n ((<uuid>|<name>|^<index>)).
          -n, --name            Name of the output bundle.
          -d, --depth           Number of parents to look back from the old output in search of the old input.
          -s, --shadow          Add the newly created bundles right after the old bundles that are being mimicked.
          -i, --dry-run         Perform a dry run (just show what will be done without doing it)
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).
          -W, --wait            Wait until run finishes.
          -t, --tail            Wait until run finishes, displaying stdout/stderr.
          -v, --verbose         Display verbose output.

      macro:
        Use mimicry to simulate macros.
          macro M A B   <=>   mimic M-in1 M-in2 M-out A B
        Arguments:
          macro_name            Name of the macro (look for <macro_name>-in1, ..., and <macro_name>-out bundles).
          bundles               Bundles: new_input_1 ... new_input_n ((<uuid>|<name>|^<index>))
          -n, --name            Name of the output bundle.
          -d, --depth           Number of parents to look back from the old output in search of the old input.
          -s, --shadow          Add the newly created bundles right after the old bundles that are being mimicked.
          -i, --dry-run         Perform a dry run (just show what will be done without doing it)
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).
          -W, --wait            Wait until run finishes.
          -t, --tail            Wait until run finishes, displaying stdout/stderr.
          -v, --verbose         Display verbose output.

      kill:
        Instruct the appropriate worker to terminate the running bundle(s).
        Arguments:
          bundle_spec           (<uuid>|<name>|^<index>)
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).

      write:
        Instruct the appropriate worker to write a small file into the running bundle(s).
        Arguments:
          target_spec           (<uuid>|<name>)[/<subpath within bundle>]
          string                Write this string to the target file.
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).


    Commands for worksheets:
      new:
        Create a new worksheet.
        Arguments:
          name                  Name of worksheet (^[a-zA-Z_][a-zA-Z0-9_\.\-]*$).
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).

      add:
        Append text items, bundles, or subworksheets to a worksheet (possibly on a different instance).
        Bundles that do not yet exist on the destination instance will be copied over.
        Arguments:
          item_type                Type of item(s) to add {text, bundle, worksheet}.
          item_spec                Item specifications, with the format depending on the specified item_type.
        text:      (<text>|%%<directive>)
        bundle:    ((<uuid>|<name>|^<index>)|(<alias>|<address>)::(<uuid>|<name>))
        worksheet: [<alias>::|<address>::](<uuid>|<name>)
          dest_worksheet           Worksheet to which to add items ([<alias>::|<address>::](<uuid>|<name>)).
          -d, --copy-dependencies  If adding bundles, also add dependencies of the bundles.

      wadd:
        Append all the items of the source worksheet to the destination worksheet.
        Bundles that do not yet exist on the destination service will be copied over.
        The existing items on the destination worksheet are not affected unless the -r/--replace flag is set.
        Arguments:
          source_worksheet_spec  [<alias>::|<address>::](<uuid>|<name>)
          dest_worksheet_spec    [<alias>::|<address>::](<uuid>|<name>)
          -r, --replace          Replace everything on the destination worksheet with the items from the source worksheet, instead of appending (does not delete old bundles, just detaches).

      work (w):
        Set the current instance/worksheet.
          work <worksheet>          : Switch to the given worksheet on the current instance.
          work <alias>::            : Switch to the home worksheet on instance <alias>.
          work <alias>::<worksheet> : Switch to the given worksheet on instance <alias>.
        Arguments:
          -u, --uuid-only  Print only the worksheet uuid.
          worksheet_spec   [<alias>::|<address>::](<uuid>|<name>)

      print (p):
        Print the rendered contents of a worksheet.
        Arguments:
          worksheet_spec  [<alias>::|<address>::](<uuid>|<name>)
          -r, --raw       Print out the raw contents (for editing).

      wedit (we):
        Edit the contents of a worksheet.
        See https://github.com/codalab/codalab-worksheets/wiki/User_Worksheet-Markdown for the markdown syntax.
          wedit -n <name>          : Change the name of the worksheet.
          wedit -T <tag> ... <tag> : Set the tags of the worksheet (e.g., paper).
          wedit -o <username>      : Set the owner of the worksheet to <username>.
        Arguments:
          worksheet_spec    [<alias>::|<address>::](<uuid>|<name>)
          -n, --name        Changes the name of the worksheet (^[a-zA-Z_][a-zA-Z0-9_\.\-]*$).
          -t, --title       Change title of worksheet.
          -T, --tags        Change tags (must appear after worksheet_spec).
          -o, --owner-spec  Change owner of worksheet.
          --freeze          Freeze worksheet to prevent future modification (PERMANENT!).
          -f, --file        Replace the contents of the current worksheet with this file.

      wrm:
        Delete a worksheet.
        To be safe, you can only delete a worksheet if it has no items and is not frozen.
        Arguments:
          worksheet_spec  [<alias>::|<address>::](<uuid>|<name>)
          --force         Delete worksheet even if it is non-empty and frozen.

      wls (wsearch, ws):
        List worksheets on the current instance matching the given keywords.
          wls tag=paper : List worksheets tagged as "paper".
          wls .mine     : List my worksheets.
        Arguments:
          keywords         Keywords to search for.
          -a, --address    (<alias>|<address>)
          -u, --uuid-only  Print only uuids.


    Commands for groups and permissions:
      gls:
        Show groups to which you belong.
        Arguments:
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).

      gnew:
        Create a new group.
        Arguments:
          name  Name of new group (^[a-zA-Z_][a-zA-Z0-9_\.\-]*$).

      grm:
        Delete a group.
        Arguments:
          group_spec  Group to delete ((<uuid>|<name>|public)).

      ginfo:
        Show detailed information for a group.
        Arguments:
          group_spec  Group to show information about ((<uuid>|<name>|public)).

      uadd:
        Add a user to a group.
        Arguments:
          user_spec    Username to add.
          group_spec   Group to add user to ((<uuid>|<name>|public)).
          -a, --admin  Give admin privileges to the user for the group.

      urm:
        Remove a user from a group.
        Arguments:
          user_spec   Username to remove.
          group_spec  Group to remove user from ((<uuid>|<name>|public)).

      perm:
        Set a group's permissions for a bundle.
        Arguments:
          bundle_spec           (<uuid>|<name>|^<index>)
          group_spec            (<uuid>|<name>|public)
          permission_spec       ((n)one|(r)ead|(a)ll)
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).

      wperm:
        Set a group's permissions for a worksheet.
        Arguments:
          worksheet_spec   [<alias>::|<address>::](<uuid>|<name>)
          group_spec       (<uuid>|<name>|public)
          permission_spec  ((n)one|(r)ead|(a)ll)

      chown:
        Set the owner of bundles.
        Arguments:
          user_spec             Username to set as the owner.
          bundle_spec           (<uuid>|<name>|^<index>)
          -w, --worksheet-spec  Operate on this worksheet ([<alias>::|<address>::](<uuid>|<name>)).


    Commands for users:
      uinfo:
        Show user information.
        Arguments:
          user_spec  Username or id of user to show [default: the authenticated user]

      uedit:
        Edit user information.
        Note that password and email can only be changed through the web interface.
        Arguments:
          user_spec         Username or id of user to update [default: the authenticated user]
          --first-name      First name
          --last-name       Last name
          --affiliation     Affiliation
          --url             Website URL
          -t, --time-quota  Total amount of time allowed (e.g., 3, 3m, 3h, 3d)
          -d, --disk-quota  Total amount of disk allowed (e.g., 3, 3k, 3m, 3g, 3t)


    Other commands:
      help:
        Show usage information for commands.
          help           : Show brief description for all commands.
          help -v        : Show full usage information for all commands.
          help <command> : Show full usage information for <command>.
        Arguments:
          command        name of command to look up
          -v, --verbose  Display all options of all commands.

      status (st):
        Show current client status.

      uedit:
        Edit user information.
        Note that password and email can only be changed through the web interface.
        Arguments:
          user_spec         Username or id of user to update [default: the authenticated user]
          --first-name      First name
          --last-name       Last name
          --affiliation     Affiliation
          --url             Website URL
          -t, --time-quota  Total amount of time allowed (e.g., 3, 3m, 3h, 3d)
          -d, --disk-quota  Total amount of disk allowed (e.g., 3, 3k, 3m, 3g, 3t)

      alias:
        Manage CodaLab instance aliases.
          alias                   : List all aliases.
          alias <name>            : Shows which instance <name> is bound to.
          alias <name> <instance> : Binds <name> to <instance>.
        Arguments:
          name          Name of the alias (e.g., main).
          instance      Instance to bind the alias to (e.g., https://codalab.org/bundleservice).
          -r, --remove  Remove this alias.

      server:
        Start an instance of the CodaLab bundle service.
        Arguments:
          --watch              Restart the server on code changes.
          -p, --processes      Number of processes to use. A production deployment should use more than 1 process to make the best use of multiple CPUs.
          -t, --threads        Number of threads to use. The server will be able to handle (--processes) x (--threads) requests at the same time.
          -d, --debug          Run the development server for debugging.

      logout:
        Logout of the current session.

      bs-add-partition:
        Add another partition for storage (MultiDiskBundleStore only)
        Arguments:
          name  The name you'd like to give this partition for CodaLab.
          path  The target location you would like to use for storing bundles. This directory should be underneath a mountpoint for the partition you would like to use. You are responsible for configuring the mountpoint yourself.

      bs-rm-partition:
        Remove a partition by its number (MultiDiskBundleStore only)
        Arguments:
          partition  The partition you want to remove.

      bs-ls-partitions:
        List available partitions (MultiDiskBundleStore only)

      bs-health-check:
        Perform a health check on the bundle store, garbage collecting bad files in the store. Performs a dry run by default, use -f to force removal.
        Arguments:
          -f, --force      Perform all garbage collection and database updates instead of just printing what would happen
          -d, --data-hash  Compute the digest for every bundle and compare against data_hash for consistency
          -r, --repair     When used with --force and --data-hash, repairs incorrect data_hash in existing bundles

## Bundles

This section describes bundles in more detail.  Recall that a bundle consists
of (i) metadata and (ii) contents. 
The metadata consists of a set of key-value pairs, some of which can be edited
by the user, and some automatically generated.
The contents is an immutable file or directory, which can store programs,
datasets, results, etc.

There are three types of bundles (metadata field `bundle_type`):

- **dataset**: any bundle that is uploaded by the user.
- **run**: any bundle that is created in CodaLab as a result of executing a command.
- **make**: any bundle that is created in CodaLab by combining parts of multiple bundles.

Each bundle has a `uuid`, which is unique and immutable.  It looks like this:

    0xa7da173bc0474344b326b406307dd1c7

Each bundle also has a `name` (a generally short string consisting of letters,
digits, underscores and dashes), `description` (which is generally longer and arbitrary),
and `tags` (a list of strings).  These fields need not be unique and can be changed:

    cl edit <bundle> --name <new name>
    cl edit <bundle> --description <new description>
    cl edit <bundle> --tags <tag_1> ... <tag_n>

Each bundle has an `owner`, which can also be changed:

    cl chown <user> <bundle>

A bundle also has `data_size`, which is how much space (unzipped) the bundle
contents takes up.

### Run bundles

Each run bundle has a `command`, which is a (bash) shell command that is executed
to produce the contents of the run bundle.  When a run bundle is created, one can specify
other options:

    --request-docker_image tensorflow/tensorflow:0.8.0-gpu
    --request-time 2d
    --request-memory 5g
    --request-network       # by default disabled

Each run has a `state`, which evolves through the following values:

- `created`: initial state
- `staged`: for run bundles, meaning dependencies are `ready`
- `waiting_for_worker_startup`: we launched a worker just for this run, waiting for it
- `running`: a worker is running the command
- `ready`/`failed`: terminal states corresponding to a successful or unsuccessful run

In addition, the `run_status` field is filled in by the worker who is running
the job to provide more information (downloading dependencies, running the job,
uploading results, etc.).

As the run is in progress, several other metadata fields are updated (this is not the full set):

- `remote`: machine where this bundle is running
- `time`: time spent on this run so far
- `memory`: memory spent on this run so far
- `exitcode`: set when command terminates
- `failure_message`: if job ended badly, here's the reason

### Dependencies

Run and make bundles have *dependencies* (think parents in a graph).
Each dependency has the form `<key>:<target>`.  For example:

    lib:lib
    train.json:dataset3/train.json

The key is like an alias or local variable that points to the target,
which can be any path inside a bundle, or just the bundle itself.
For a run bundle, these dependencies are not part of the bundle contents,
but only present when the run is executing.

    cl run lib:lib train.json:dataset3/train.json <command>

For a make bundle, these dependencies are copied to form the contents of the
bundle:

    cl make lib:lib train.json:dataset3/train.json

A special case of a make bundle is when there is a single dependency with a
null key.  Then the contents are simply copied from the target:

    cl make dataset3/train.json

Each bundle has a set of *children* bundles, which are simply those that depend
on the bundle.

To show all this verbose information about a bundle:

    cl info -v <bundle>

## Worksheets

A worksheet consists of (i) metadata and (ii) contents.  The metadata includes:

- `uuid`: globally unique, automatically assigned
- `name`: unique across the CodaLab server (unlike bundle names)
- `title`: any textual description

The contents is a list of items, where each item is one of the following:

- text (e.g., `Here is some *text*`)
- bundle reference (`{<bundle_spec>}`)
- worksheet reference (`{{<worksheet_spec>}}`)
- directive (e.g., `% display table s1`)

Worksheets are modified by editing the markdown source directly (see [worksheet
markdown reference](Worksheet-Markdown)) or by running commands that remove/add
bundles to the worksheet.

    cl wedit <worksheet>

We can display the contents of a worksheet from the CLI as follows:

    cl print    # Shows all items (bundles, text, and worksheets)
    cl ls       # Only shows the bundles

Unlike bundles, worksheets are mutable until they are frozen (this cannot be
undone!):

    cl wedit --freeze

One useful analogy is to think of a worksheet as a directory
and the bundles in the worksheet as the files in that directory.
But unlike a directory, a worksheet remembers the order of the bundles (which
can be changed), which is useful for organization, and other text and
directives, which is useful for presenting the worksheet.

Two special symbols denoting worksheets are:

- `.`: refers to the current worksheet
- `/`: refers to your home worksheet (e.g., `home-pliang`)

We can add items (text or bundle references) to the current worksheet, which appends to
the end:

    cl add text "Here's a simple bundle:" .
    cl add bundle sort.py .
    cl add worksheet home-pliang .

We can create a worksheet as follows (try to use the `<username>-<name>`
convention for naming worksheets):

    cl new pliang-scratch

We can switch back and forth between worksheets:

    cl work pliang-scratch
    cl work /

To remove a worksheet:

    cl wrm pliang-scratch

## Referencing bundles

So far, we have referred to bundles by their names.  In
a large CodaLab system with many users, names are not unique, and possibly not
even within the same worksheet.  A *bundle_spec* refers to the string that
identifies a bundle, importantly given the context (current worksheet).

There are a number of ways to reference bundles:

- UUID (`0x3739691aef9f4b07932dc68f7db82de2`): this should match at most one
  bundle.  You can use a prefix (an error is thrown if the prefix doesn't
  resolve uniquely).
- Name (`foo`): matches the *last* bundle on the current worksheet with the
  given name.  You can use `foo%` to match bundles that begin with `foo` or
  `%foo%` to match bundles that contain `foo` (SQL LIKE syntax).
  You can use `w1/foo` to refer to a bundle by name on worksheet `w1`.
- Ordering (`^, ^2, ^3`): returns the first, second, and third bundles from the end
  of the current worksheet.
- Named ordering (`foo^, foo^2, foo^3`): returns the first, second, and third
  bundles from the end with the given name.
- You can refer to a range of bundles: `^1-3` resolves to `^1 ^2 ^3`.
- In the worksheet interface, if you press 'u', then this will paste the UUID
  of the current bundle into the command.  This is a very convenient way of mixing
  command-line and graphical interfaces.

In practice, `^` and `^2` are used frequently because future operations tend to
depend on the bundles you just created.

Warning: ordering references are not stable.  For example,
if you run:

    cl ls
    cl rm ^1
    cl rm ^2

This *does not* delete the first and second last bundles, but rather the first
and third!  The intended behavior is:

    cl rm ^1 ^2

Also, if someone else is adding to your worksheet while you're editing it, you
might end up referring to the wrong bundle.

## Mimic and macros

Mimic and macros are the most advanced features of CodaLab which really
leverage the fact that we have the full dependency graph.  It allows you to
rerun many commands at once with newer versions of code or alternative
datasets.

Let us return to our sorting example.  Suppose we have run the following
command that sorts a file and extracts a single file.

    cl run :sort.py input:a.txt 'python sort.py < input' -n sort-run
    cl make sort-run/stdout -n a-sorted.txt

Now suppose we upload a new file `b.txt`.  Can we easily do this?

CodaLab macros allow you to do this, although understanding this concept
requires us to take a step back.

In CodaLab, bundles form a directed acyclic graph (DAG), where nodes are
bundles and a directed edge from A to B means that B depends on A.  Imagine we
have created some runs that produces some output bundle O from some input
bundle I; I is an ancestor of O in the DAG.  Now suppose we have a new input
bundle I', how can we produce the analogous O'.  The *mimic* command does
exactly this.

First, recall that we have created `a.txt` (I) and `sort-run` (O).  Let us
create another bundle called `b.txt`:

    6
    3
    8

and upload it:

    cl upload b.txt

Now we can apply the same thing to `b.txt` that we did to `a.txt`:

    cl mimic a.txt a-sorted.txt b.txt -n b-sorted.txt

We can check that `b.txt.sorted` contains the desired sorted result:

    cl cat b-sorted.txt

Normally, in a programming language, we define macros as abstractions.  In
CodaLab though, notice that we've started instead by creating a concrete
example, and then used an analogy to re-apply this.  A positive side-effect is
that every macro automatically comes with an example of how it is used!

We can make the notion of a macro even more explicit.  Let's rename `a.txt` to
`sort-in1` and `a-sorted.txt` to `sort-out`:

    cl edit a.txt -n sort-in1
    cl edit a-sorted.txt -n sort-out

Then we can use the following syntactic sugar:

    cl macro sort b.txt -n b-sorted.txt

In CodaLab, macros are not defined ahead of time, but are constructed on the
fly from the bundle DAG.

## Permissions

CodaLab implements the following permissions model:

- Users belong to groups.
- Each group has access to some bundles and worksheets.

There are three levels of access or permission:

- `none`: You can't even see that the worksheet exists.
- `read`: You can read/download, but not edit.
- `all`: You can do anything (edit/delete/etc.).

Notes:

- There is a designated `public` group to which all users implicitly belong.  If
  you want to make a worksheet world-readable, give the `public` group read
  permission.
- There is a designated root user (`codalab`) that has `all` permission to
  all bundles and worksheets.
- Each user has `all` permission to all bundles and worksheets that he/she owns.

To grant/revoke permissions:

    cl perm <bundle> <group> <(n)one|(r)ead|(a)ll>
    cl wperm <bundle> <group> <(n)one|(r)ead|(a)ll>

For example:

    cl perm bundle1 public r  # grant read permission
    cl perm bundle1 public a  # grant all permission
    cl perm bundle1 public n  # revoke permissions

We can transfer ownership (and therefore permissions) of bundles and
worksheets:

    cl chown <username> <bundle-1> ... <bundle-n>
    cl wedit <worksheet> -o <username>

To make a worksheet `w1` mutually-writable with your research group, first
create a group `g1`, add users `u1` and `u2` to it, and then give the group all
access:

    cl gnew g1
    cl uadd u1 g1
    cl uadd u2 g1
    cl wperm w1 g1 all

All bundles created on `w1` will initially inherit the permissions of that
worksheet, but these permissions can be changed independently.
    
To list the groups that you've created or belong to:

    cl gls

To look more into a given group `g1`:

    cl ginfo g1

## Search

The `cl search` command allows us to find bundles and compute various
statistics over them.  The search performs a conjunction over keywords.

    cl search <keyword-1> ... <keyword-n>

Some initial examples:

    cl search mnist                        # bundles whose name or uuid contains `mnist`
    cl search e342f                        # bundles whose name or uuid contains `e342f`
    cl search type=program                 # program bundles
    cl search name=mnist                   # bundles whose names is exactly `mnist`
    cl search state=running                # all running bundles
    cl search command=%python%             # bundles whose command contains `python`
    cl search dependency=0xa11%            # bundles that depends on the given bundle
    cl search worksheet=0xfdd%             # bundles that are on the given worksheet
    cl search owner=codalab                # bundles that are owned by the given user name
    cl search =%python%                    # match any field

You can combine search terms:

    cl search type=program owner=codalab   # programs owned by user `codalab`

You can change the number and ordering of results:

    cl search .offset=50 .limit=100        # bundles 50-99
    cl search size=.sort                   # sort by increasing size
    cl search size=.sort-                  # sort by decreasing size

There are some special commands:

    cl search .mine                        # show bundles that the current user owns
    cl search .last                        # bundles in reverse order of creation
    cl search .floating                    # bundles that aren't on any worksheet

Operations that return a single number rather than a list of bundles:

    cl search .count                       # return total number of bundles in the system
    cl search size=.sum                    # return total number of bytes (nominal)
    cl search size=.sum data_hash=0x%      # return total number of bytes (actual, where we only count bundles with data)

We can combine these keywords to yield the following handy queries:

    cl search .mine .last                  # bundles that you just created
    cl search .mine .floating              # bundles that are floating (probably want to delete these periodically)
    cl search .mine size=.sort-            # what are the biggest bundles I own?

The search returns a list of bundles.  We can use `-u` to just get the uuids.
This can be piped into other commands:

    cl search .mine .floating -u | xargs cl rm  # delete the floating bundles
    cl search mnist -u | xargs cl add           # add mnist to the current worksheet

We can list and search worksheets in a similar fashion:

    cl wsearch                       # all worksheets
    cl wsearch .mine                 # my worksheet
    cl wsearch .last .limit=3        # last worksheets created
    cl wsearch name=.sort            # worksheets sorted by name
    cl wsearch bundle=0x3bb%         # worksheets containing this bundle
    cl wsearch owner=codalab         # worksheets owned by `codalab`
    cl wsearch =%Hello%              # worksheets containing 'Hello'

## CodaLab instances

When you're using the web interface, you are connected to one particular
CodaLab instance (e.g., `worksheets.codalab.org`).  If you're using the CLI,
you can connect to multiple CodaLab instances and copy information between
them.

Suppose you have set up a local instance in addition to the official instance:

    http://localhost:2800
    https://worksheets.codalab.org/bundleservice

To save typing, you can create an alias for instances:

    cl alias  # shows all aliases
    cl alias localhost http://localhost:2800
    cl alias main https://worksheets.codalab.org/bundleservice

At any point in time, your CodaLab session (identified usually by your shell)
is pointing to a particular worksheet on a particular instance.

    cl work                          # Show the current instance and worksheet
    cl work localhost::
    cl work localhost::home-pliang
    cl work main::
    cl work main::home-pliang

The general form is:

    cl work <instance>::<worksheet>
    cl work <instance>::             # Defaults to <worksheet>=/

Just as in Git, sometimes you want to work locally and then once things are
ready, push things to the main server.  You can do the same with CodaLab.  The
difference is that bundles are atomic and mutable, so there is no merging.

Suppose we are on the home worksheet of `localhost`:

    cl work localhost::

To copy a bundle `a.txt` from `localhost` to `main`, do the following:

    cl add bundle a.txt main::

If the bundle `a.txt` (identified by UUID) already exists on `main`, then
nothing will be copied.  Otherwise, the contents of the bundle are copied from `localhost` to `main`.
In either case, a reference to the bundle is appended to the home worksheet on `main`.

You can also copy a bundle from `main` to `localhost`:

    cl add bundle main::a.txt .

By default, `cl add bundle` does not copy the dependencies of a bundle.  If you want to
copy the dependencies (for example, to reproduce a run on another machine),
then use `cl add bundle -d`.  The dependencies of the dependencies are not copied,
since only the immediate dependencies are required to execute a run.

In general, the `add bundle` command is as follows:

    cl add bundle [<address>::]<bundle> [<address>::]<worksheet>

To copy all the items from a worksheet (except nested worksheets) to another:
    
    cl wadd [<address>::]<worksheet> [<address>::]<worksheet>

Note that worksheets themselves are not copied, just the items within a
worksheet.  Any bundles that don't exist on the destination CodaLab instance
are copied over.

## User tips

The following describes some common tip and tricks to make the most out of CodaLab.

Delete the last five bundles (remember this also removes all other instances of
these bundles on the current worksheet):

    cl rm ^1-5

To kill the last bundle:

    cl kill ^

Most CodaLab commands generate one or more bundle UUIDs.  These can be piped to
further commands.  To kill all running bundles (be careful!):

    cl search state=running -u | xargs cl kill

To delete all *floating* bundles that do not appear on a worksheet (be careful!):

    cl search .floating -u | xargs cl rm

To run a bundle and create another bundle that depends on it:

    cl make $(cl run date)/stdout -n stdout

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
themselves are not removed and will be floating):

    cl wedit -f /dev/null -w worksheet2
    cl wadd worksheet1 worksheet2

To change the metadata of a worksheet (e.g., rename or change the owner):

    cl wedit <worksheet> -n <new name>
    cl wedit <worksheet> -o <new owner>

To change the metadata of a bundle (e.g., rename or change the description):

    cl edit <bundle> -n <new name>
    cl edit <bundle> -d <new description>

## Editing worksheets

By default, you will use `cl wedit` to edit worksheets.  However, it is
convenient to just keep a text editor open.  Here's one way to do this:

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

## Updating CodaLab CLI

To update to the newest version of CodaLab, cd into `codalab-cli` and run:

    git pull

When you do this, the database schema might have changed, and you need to
perform a *database migration*.  To be on the safe side, first backup your
database.  Then run:

    ./setup.sh client
    venv/bin/alembic upgrade head

Additionally, note that if you run your own worker, it will upgrade itself automatically. To avoid having to type in your username and password after a worker upgrades, you can pass in a file containing your credentials using the `--password-file` flag.

## Where things are stored

For reference, your CodaLab settings here:

    ~/.codalab/config.json

The session state and authentication tokens are stored here:

    ~/.codalab/state.json

If you're running a server, all the bundle contents are stored here:

    ~/.codalab/partitions

and the metadata is stored in a MySQL database.