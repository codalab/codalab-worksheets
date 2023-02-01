# CLI Reference

This file is auto-generated from the output of `cl help -v -m` and provides the list of all CLI commands.

Usage: `cl <command> <arguments>`

## Commands for bundles
### upload (up)
    Create a bundle by uploading an existing file/directory.
      upload <path>            : Upload contents of file/directory <path> as a bundle.
      upload <path> ... <path> : Upload one bundle whose directory contents contain <path> ... <path>.
      upload -c <text>         : Upload one bundle whose file contents is <text>.
      upload <url>             : Upload one bundle whose file contents is downloaded from <url>.
    Most of the other arguments specify metadata fields.
    Arguments:
      path                       Paths of the files/directories to upload, or a single URL to upload.
      -c, --contents             Specify the string contents of the bundle.
      -L, --follow-symlinks      Always dereference (follow) symlinks.
      -x, --exclude-patterns     Exclude these file patterns.
      -g, --git                  Path is a git repository, git clone it.
      -p, --pack                 If path is an archive file (e.g., zip, tar.gz), keep it packed.
      -z, --force-compression    Always use compression (this may speed up single-file uploads over a slow network).
      -w, --worksheet-spec       Upload to this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).
      -i, --ignore               Name of file containing patterns matching files and directories to exclude from upload. This option is currently only supported with the GNU tar library.
      -l, --link                 Makes the path the source of truth of the bundle, meaning that the server will retrieve the bundle directly from the specified path rather than storing its contentsin its own bundle store.
      -a, --use-azure-blob-beta  Use Azure Blob Storage to store files (beta feature).
      -n, --name                 Short name (not necessarily unique), which must start with a letter or underscore and can only contain letters, digits, underscores, periods, and dashes (name).
      -d, --description          Full description of the bundle (description).
      --tags                     Space-separated list of tags used for search, e.g. machine-learning (tags).
      --license                  The license under which this program/dataset is released (license).
      --source-url               URL corresponding to the original source of this bundle (source_url).
      --link-url                 Link URL of bundle (link_url).
      --link-format              Link format of bundle (link_format). Can be equal to "raw" or "zip" (only "raw" is supported as of now).
      --store                    The name of the bundle store where the bundle should be uploaded to (store). If unspecified, an optimal available bundle store will be chosen.
      -e, --edit                 Show an editor to allow editing of the bundle metadata.

### make
    Create a bundle by combining parts of existing bundles.
      make <bundle>/<subpath>                : New bundle's contents are copied from <subpath> in <bundle>.
      make <key>:<bundle> ... <key>:<bundle> : New bundle contains file/directories <key> ... <key>, whose contents are given.
    Arguments:
      target_spec                  [<key>:][[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)[/<subpath within bundle>]
      -w, --worksheet-spec         Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).
      -n, --name                   Short name (not necessarily unique), which must start with a letter or underscore and can only contain letters, digits, underscores, periods, and dashes (name).
      -d, --description            Full description of the bundle (description).
      --tags                       Space-separated list of tags used for search, e.g. machine-learning (tags).
      --allow-failed-dependencies  Whether to allow this bundle to have failed or killed dependencies (allow_failed_dependencies).
      -e, --edit                   Show an editor to allow editing of the bundle metadata.

### run
    Create a bundle by running a program bundle on an input bundle.
    Arguments:
      target_spec                  [<key>]:[[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)[/<subpath within bundle>]
      command                      Arbitrary Linux command to execute.
      -w, --worksheet-spec         Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).
      -a, --after_sort_key         Insert after this sort_key
      -m, --memoize                If a bundle with the same command and dependencies already exists, return it instead of creating a new one.
      -i, --interactive            Beta feature - Start an interactive session to construct your run command.
      -n, --name                   Short name (not necessarily unique), which must start with a letter or underscore and can only contain letters, digits, underscores, periods, and dashes (name).
      -d, --description            Full description of the bundle (description).
      --tags                       Space-separated list of tags used for search, e.g. machine-learning (tags).
      --allow-failed-dependencies  Whether to allow this bundle to have failed or killed dependencies (allow_failed_dependencies).
      --request-docker-image       Which docker image (either tag or digest, e.g., codalab/default-cpu:latest) we wish to use (request_docker_image).
      --request-time               Amount of time (e.g., 3, 3m, 3h, 3d) allowed for this run (request_time). Defaults to user time quota left.
      --request-memory             Amount of memory (e.g., 3, 3k, 3m, 3g, 3t) allowed for this run (request_memory).
      --request-disk               Amount of disk space (e.g., 3, 3k, 3m, 3g, 3t) allowed for this run (request_disk). Defaults to user disk quota left.
      --request-cpus               Number of CPUs allowed for this run (request_cpus).
      --request-gpus               Number of GPUs allowed for this run (request_gpus).
      --request-queue              Submit run to this job queue (request_queue).
      --request-priority           Job priority (request_priority). Higher is more important. Negative priority bundles are queued behind bundles with no specified priority.
      --request-network            Whether to allow network access (request_network).
      --exclude-patterns           Exclude these file patterns from being saved into the bundle contents (exclude_patterns).
      --store                      The name of the bundle store where bundle results should be initially uploaded (store). If unspecified, an optimal available bundle store will be chosen.
      -e, --edit                   Show an editor to allow editing of the bundle metadata.
      -W, --wait                   Wait until run finishes.
      -t, --tail                   Wait until run finishes, displaying stdout/stderr.
      -v, --verbose                Display verbose output.

### edit (e)
    Edit an existing bundle's metadata.
      edit           : Popup an editor.
      edit -n <name> : Edit the name metadata field (same for other fields).
      edit -T <tag> ... <tag> : Set the tags of the bundle (e.g., training-dataset).
    Arguments:
      bundle_spec           [[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)
      -n, --name            Change the bundle name (format: ^[a-zA-Z_][a-zA-Z0-9_\.\-]*$).
      -T, --tags            Change tags (must appear after worksheet_spec).
      -d, --description     New bundle description.
      --freeze              Freeze bundle to prevent future metadata modification.
      --unfreeze            Unfreeze bundle to allow future metadata modification.
      --anonymous           Set bundle to be anonymous (identity of the owner will NOT be visible to users without 'all' permission on the bundle).
      --not-anonymous       Set bundle to be NOT anonymous.
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).
      -f, --field           Edit any specified bundle metadata field.

### detach (de)
    Detach a bundle from this worksheet, but doesn't remove the bundle.
    Arguments:
      bundle_spec           [[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)
      -n, --index           Specifies which occurrence (1, 2, ...) of the bundle to detach, counting from the end.
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).

### rm
    Remove a bundle (permanent!).
    Arguments:
      bundle_spec           [[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)
      --force               Delete bundle (DANGEROUS - breaking dependencies!)
      -r, --recursive       Delete all bundles downstream that depend on this bundle (DANGEROUS - could be a lot!).
      -d, --data-only       Keep the bundle metadata, but remove the bundle contents on disk.
      -i, --dry-run         Perform a dry run (just show what will be done without doing it).
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).

### search (s)
    Search for bundles on a CodaLab instance (returns 10 results by default).
      search <keyword> ... <keyword>         : Name or uuid contains each <keyword>.
      search name=<value>                    : Name is <value>, where `name` can be any metadata field (e.g., description).
      search type=<type>                     : Bundle type is <type> (`run` or `dataset`).
      search id=<id>                         : Has <id> (integer used for sorting, strictly increasing over time).
      search uuid=<uuid>                     : UUID is <uuid> (e.g., 0x...).
      search state=<state>                   : State is <state> (e.g., staged, running, ready, failed).
      search command=<command>               : Command to run is <command>.
      search dependency=<uuid>               : Has a dependency with <uuid>.
      search dependency/<name>=<uuid>        : Has a dependency <name>:<uuid>.
    
      search owner=<owner>                   : Owned by <owner> (e.g., `pliang`).
      search .mine                           : Owned by me.
      search group=<group>                   : Shared with <group>.
      search .shared                         : Shared with any of the groups I'm in.
    
      search host_worksheet=<worksheet>      : On <worksheet>.
      search .floating                       : Not on any worksheet.
    
      search .limit=<limit>                  : Limit the number of results to the top <limit> (e.g., 50).
      search .offset=<offset>                : Return results starting at <offset>.
    
      search .before=<datetime>              : Returns bundles created before (inclusive) given ISO 8601 timestamp (e.g., .before=2042-03-14).
      search .after=<datetime>               : Returns bundles created after (inclusive) given ISO 8601 timestamp (e.g., .after=2120-10-15T00:00:00-08).
    
      search size=.sort                      : Sort by a particular field (where `size` can be any metadata field).
      search size=.sort-                     : Sort by a particular field in reverse (e.g., `size`).
      search .last                           : Sort in reverse chronological order (equivalent to id=.sort-).
      search .count                          : Count the number of matching bundles.
      search size=.sum                       : Compute total of a particular field (e.g., `size`).
      search .format=<format>                : Apply <format> function (see worksheet markdown).
    Arguments:
      keywords              Keywords to search for.
      -f, --field           Print out these comma-separated fields in the results table
      -a, --append          Append these bundles to the current worksheet.
      -u, --uuid-only       Print only uuids.
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).

### ls
    List bundles in a worksheet.
    Arguments:
      -f, --field           Print out these comma-separated fields in the results table
      -u, --uuid-only       Print only uuids.
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).

### info (i)
    Show detailed information for a bundle.
    Arguments:
      bundle_spec           [[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)
      -f, --field           Print out these comma-separated fields.
      -r, --raw             Print out raw information (no rendering of numbers/times).
      -v, --verbose         Print top-level contents of bundle, children bundles, and host worksheets.
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).

### cat
    Print the contents of a file/directory in a bundle.
    Note that cat on a directory will list its files.
    Arguments:
      target_spec           [[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)[/<subpath within bundle>]
      --head                Display first NUM lines of contents.
      -t, --tail            Display last NUM lines of contents
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).

### wait
    Wait until a run bundle finishes.
    Arguments:
      target_spec           [[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)[/<subpath within bundle>]
      -t, --tail            Print out the tail of the file or bundle and block until the run bundle has finished running.
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).

### download (down)
    Download bundle from a CodaLab instance.
    Arguments:
      target_spec           [[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)[/<subpath within bundle>]
      -o, --output-path     Path to download bundle to.  By default, the bundle or subpath name in the current directory is used.
      -f, --force           Overwrite the output path if a file already exists.
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).

### mimic
    Creates a set of bundles based on analogy with another set.
      mimic <run>      : Rerun the <run> bundle.
      mimic A B        : For all run bundles downstream of A, rerun with B instead.
      mimic A X B -n Y : For all run bundles used to produce X depending on A, rerun with B instead to produce Y.
    Any provided metadata arguments will override the original metadata in mimicked bundles.
    Arguments:
      bundles                      Bundles: old_input_1 ... old_input_n old_output new_input_1 ... new_input_n ([[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)).
      -n, --name                   Short name (not necessarily unique), which must start with a letter or underscore and can only contain letters, digits, underscores, periods, and dashes (name). (for makes and runs)
      -d, --description            Full description of the bundle (description). (for makes and runs)
      --tags                       Space-separated list of tags used for search, e.g. machine-learning (tags). (for makes and runs)
      --allow-failed-dependencies  Whether to allow this bundle to have failed or killed dependencies (allow_failed_dependencies). (for makes and runs)
      --request-docker-image       Which docker image (either tag or digest, e.g., codalab/default-cpu:latest) we wish to use (request_docker_image). (for runs)
      --request-time               Amount of time (e.g., 3, 3m, 3h, 3d) allowed for this run (request_time). Defaults to user time quota left. (for runs)
      --request-memory             Amount of memory (e.g., 3, 3k, 3m, 3g, 3t) allowed for this run (request_memory). (for runs)
      --request-disk               Amount of disk space (e.g., 3, 3k, 3m, 3g, 3t) allowed for this run (request_disk). Defaults to user disk quota left. (for runs)
      --request-cpus               Number of CPUs allowed for this run (request_cpus). (for runs)
      --request-gpus               Number of GPUs allowed for this run (request_gpus). (for runs)
      --request-queue              Submit run to this job queue (request_queue). (for runs)
      --request-priority           Job priority (request_priority). Higher is more important. Negative priority bundles are queued behind bundles with no specified priority. (for runs)
      --request-network            Whether to allow network access (request_network). (for runs)
      --exclude-patterns           Exclude these file patterns from being saved into the bundle contents (exclude_patterns). (for runs)
      --store                      The name of the bundle store where bundle results should be initially uploaded (store). If unspecified, an optimal available bundle store will be chosen. (for runs)
      --depth                      Number of parents to look back from the old output in search of the old input.
      -s, --shadow                 Add the newly created bundles right after the old bundles that are being mimicked.
      -i, --dry-run                Perform a dry run (just show what will be done without doing it)
      -w, --worksheet-spec         Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).
      -m, --memoize                If a bundle with the same command and dependencies already exists, return it instead of creating a new one.
      -W, --wait                   Wait until run finishes.
      -t, --tail                   Wait until run finishes, displaying stdout/stderr.
      -v, --verbose                Display verbose output.

### macro
    Use mimicry to simulate macros.
      macro M A B <name1>:C <name2>:D <=> mimic M-in1 M-in2 M-in-name1 M-in-name2 M-out A B C D
    Arguments:
      macro_name                   Name of the macro (look for <macro_name>-in1, <macro_name>-in-<name>, ..., and <macro_name>-out bundles).
      bundles                      Bundles: new_input_1 ... new_input_n named_input_name:named_input_bundle other_named_input_name:other_named_input_bundle ([[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>))
      -n, --name                   Short name (not necessarily unique), which must start with a letter or underscore and can only contain letters, digits, underscores, periods, and dashes (name). (for makes and runs)
      -d, --description            Full description of the bundle (description). (for makes and runs)
      --tags                       Space-separated list of tags used for search, e.g. machine-learning (tags). (for makes and runs)
      --allow-failed-dependencies  Whether to allow this bundle to have failed or killed dependencies (allow_failed_dependencies). (for makes and runs)
      --request-docker-image       Which docker image (either tag or digest, e.g., codalab/default-cpu:latest) we wish to use (request_docker_image). (for runs)
      --request-time               Amount of time (e.g., 3, 3m, 3h, 3d) allowed for this run (request_time). Defaults to user time quota left. (for runs)
      --request-memory             Amount of memory (e.g., 3, 3k, 3m, 3g, 3t) allowed for this run (request_memory). (for runs)
      --request-disk               Amount of disk space (e.g., 3, 3k, 3m, 3g, 3t) allowed for this run (request_disk). Defaults to user disk quota left. (for runs)
      --request-cpus               Number of CPUs allowed for this run (request_cpus). (for runs)
      --request-gpus               Number of GPUs allowed for this run (request_gpus). (for runs)
      --request-queue              Submit run to this job queue (request_queue). (for runs)
      --request-priority           Job priority (request_priority). Higher is more important. Negative priority bundles are queued behind bundles with no specified priority. (for runs)
      --request-network            Whether to allow network access (request_network). (for runs)
      --exclude-patterns           Exclude these file patterns from being saved into the bundle contents (exclude_patterns). (for runs)
      --store                      The name of the bundle store where bundle results should be initially uploaded (store). If unspecified, an optimal available bundle store will be chosen. (for runs)
      --depth                      Number of parents to look back from the old output in search of the old input.
      -s, --shadow                 Add the newly created bundles right after the old bundles that are being mimicked.
      -i, --dry-run                Perform a dry run (just show what will be done without doing it)
      -w, --worksheet-spec         Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).
      -m, --memoize                If a bundle with the same command and dependencies already exists, return it instead of creating a new one.
      -W, --wait                   Wait until run finishes.
      -t, --tail                   Wait until run finishes, displaying stdout/stderr.
      -v, --verbose                Display verbose output.

### kill
    Instruct the appropriate worker to terminate the running bundle(s).
    Arguments:
      bundle_spec           [[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).

### write
    Instruct the appropriate worker to write a small file into the running bundle(s).
    Arguments:
      target_spec           [[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)[/<subpath within bundle>]
      string                Write this string to the target file.
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).

### mount
    Beta feature: this command may change in a future release. Mount the contents of a bundle at a read-only mountpoint.
    Arguments:
      target_spec           [[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)[/<subpath within bundle>]
      --mountpoint          Empty directory path to set up as the mountpoint for FUSE.
      --verbose             Verbose mode for BundleFUSE.
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).

### netcat
    Beta feature: this command may change in a future release. Send raw data into a port of a running bundle
    Arguments:
      bundle_spec           [[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)
      port                  Port
      message               Arbitrary message to send.
      -f, --file            Add this file at end of message
      --verbose             Verbose mode.
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).

### store
    Add a bundle store.
    Arguments:
      command            Set to "add" to add a new bundle store, "ls" to list bundle stores, and "rm" to remove a bundle store.
      bundle_store_uuid  Bundle store uuid. Specified when running "cl store rm [uuid]".
      -n, --name         Name of the bundle store; must be globally unique.
      --storage-type     Storage type of the bundle store. Acceptable values are "disk" and "azure_blob".
      --storage-format   Storage format of the bundle store. Acceptable values are "uncompressed" and "compressed_v1". Optional; if unspecified, will be set to an optimal default.
      --url              A self-referential URL that points to the bundle store.
      --authentication   Key for authentication that the bundle store uses.


## Commands for worksheets
### new
    Create a new worksheet.
    Arguments:
      name                  Name of worksheet (^[a-zA-Z_][a-zA-Z0-9_\.\-]*$).
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).

### add
    Append text items, bundles, or subworksheets to a worksheet (possibly on a different instance).
    Bundles that do not yet exist on the destination instance will be copied over.
    Arguments:
      item_type                Type of item(s) to add {text, bundle, worksheet}.
      item_spec                Item specifications, with the format depending on the specified item_type.
    text:      (<text>|%%<directive>)
    bundle:    [[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)
    worksheet: [(<alias>|<address>)::](<uuid>|<name>)
      --dest-worksheet         Worksheet to which to add items ([(<alias>|<address>)::](<uuid>|<name>)).
      -d, --copy-dependencies  If adding bundles, also add dependencies of the bundles.

### wadd
    Append all the items of the source worksheet to the destination worksheet.
    Bundles that do not yet exist on the destination service will be copied over.
    Bundles in non-terminal states (READY or FAILED) will not be copied over to destination worksheet.
    The existing items on the destination worksheet are not affected unless the -r/--replace flag is set.
    Arguments:
      source_worksheet_spec  [(<alias>|<address>)::](<uuid>|<name>)
      dest_worksheet_spec    [(<alias>|<address>)::](<uuid>|<name>)
      -r, --replace          Replace everything on the destination worksheet with the items from the source worksheet, instead of appending (does not delete old bundles, just detaches).

### work (w)
    Set the current instance/worksheet.
      work <worksheet>          : Switch to the given worksheet on the current instance.
      work <alias>::            : Switch to the home worksheet on instance <alias>.
      work <alias>::<worksheet> : Switch to the given worksheet on instance <alias>.
    Arguments:
      -u, --uuid-only  Print only the worksheet uuid.
      worksheet_spec   [(<alias>|<address>)::](<uuid>|<name>)

### print (p)
    Print the rendered contents of a worksheet.
    Arguments:
      worksheet_spec  [(<alias>|<address>)::](<uuid>|<name>)
      -r, --raw       Print out the raw contents (for editing).

### wedit (we)
    Edit the contents of a worksheet.
    See https://codalab-worksheets.readthedocs.io/en/latest/User_Worksheet-Markdown for the markdown syntax.
      wedit -n <name>          : Change the name of the worksheet.
      wedit -T <tag> ... <tag> : Set the tags of the worksheet (e.g., paper).
      wedit -o <username>      : Set the owner of the worksheet to <username>.
    Arguments:
      worksheet_spec    [(<alias>|<address>)::](<uuid>|<name>)
      -n, --name        Changes the name of the worksheet (^[a-zA-Z_][a-zA-Z0-9_\.\-]*$).
      -t, --title       Change title of worksheet.
      -T, --tags        Change tags (must appear after worksheet_spec).
      -o, --owner-spec  Change owner of worksheet.
      --freeze          Freeze worksheet to prevent future modification.
      --unfreeze        Unfreeze worksheet to allow future modification.
      --anonymous       Set worksheet to be anonymous (identity of the owner will NOT be visible to users without 'all' permission on the worksheet).
      --not-anonymous   Set worksheet to be NOT anonymous.
      -f, --file        Replace the contents of the current worksheet with this file.

### wrm
    Delete a worksheet.
    To be safe, you can only delete a worksheet if it has no items and is not frozen.
    Arguments:
      worksheet_spec  [(<alias>|<address>)::](<uuid>|<name>)
      --force         Delete worksheet even if it is non-empty and frozen.

### wls (wsearch, ws)
    List worksheets on the current instance matching the given keywords (returns 10 results by default).
    Searcher's own worksheets are prioritized.
      wls tag=paper           : List worksheets tagged as "paper".
      wls group=<group_spec>  : List worksheets shared with the group identfied by group_spec.
      wls .mine               : List my worksheets.
      wls .notmine            : List the worksheets not owned by me.
      wls .shared             : List worksheets that have been shared with any of the groups I am in.
      wls .limit=10           : Limit the number of results to the top 10.
    Arguments:
      keywords         Keywords to search for.
      -a, --address    (<alias>|<address>)
      -u, --uuid-only  Print only uuids.


## Commands for groups and permissions
### gls
    Show groups to which you belong.
    Arguments:
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).

### gnew
    Create a new group.
    Arguments:
      name  Name of new group (^[a-zA-Z_][a-zA-Z0-9_\.\-]*$).

### grm
    Delete a group.
    Arguments:
      group_spec  Group to delete ((<uuid>|<name>|public)).

### ginfo
    Show detailed information for a group.
    Arguments:
      group_spec  Group to show information about ((<uuid>|<name>|public)).

### uadd
    Add a user to a group.
    Arguments:
      user_spec    Username to add.
      group_spec   Group to add user to ((<uuid>|<name>|public)).
      -a, --admin  Give admin privileges to the user for the group.

### urm
    Remove a user from a group.
    Arguments:
      user_spec   Username to remove.
      group_spec  Group to remove user from ((<uuid>|<name>|public)).

### perm
    Set a group's permissions for a bundle.
    Arguments:
      bundle_spec           [[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)
      group_spec            (<uuid>|<name>|public)
      permission_spec       ((n)one|(r)ead|(a)ll)
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).

### wperm
    Set a group's permissions for a worksheet.
    Arguments:
      worksheet_spec   [(<alias>|<address>)::](<uuid>|<name>)
      group_spec       (<uuid>|<name>|public)
      permission_spec  ((n)one|(r)ead|(a)ll)

### chown
    Set the owner of bundles.
    Arguments:
      user_spec             Username to set as the owner.
      bundle_spec           [[(<alias>|<address>)::](<uuid>|<name>)//](<uuid>|<name>|^<index>)
      -w, --worksheet-spec  Operate on this worksheet ([(<alias>|<address>)::](<uuid>|<name>)).


## Commands for users
### uinfo
    Show user information.
    Arguments:
      user_spec    Username or id of user to show [default: the authenticated user]
      -f, --field  Print out these comma-separated fields.

### uedit
    Edit user information.
    Note that password and email can only be changed through the web interface.
    Arguments:
      user_spec                 Username or id of user to update [default: the authenticated user]
      --first-name              First name
      --last-name               Last name
      --affiliation             Affiliation
      --url                     Website URL
      -t, --time-quota          Total amount of time allowed (e.g., 3, 3m, 3h, 3d)
      -p, --parallel-run-quota  Total amount of runs the user may have running at a time on shared public workers
      -d, --disk-quota          Total amount of disk allowed (e.g., 3, 3k, 3m, 3g, 3t)
      --grant-access            Grant access to the user if the CodaLab instance is in protected mode
      --remove-access           Remove the user's access if the CodaLab instance is in protected mode

### ufarewell
    Delete user permanently. Only root user can delete other users. Non-root user can delete his/her own account.
    To be safe, you can only delete a user if user does not own any bundles, worksheets, or groups.
    Arguments:
      user_spec  Username or id of user to delete.

### uls
    Lists users on CodaLab (returns 10 results by default).
      uls <keyword> ... <keyword>         : Username or id contains each <keyword>.
      uls user_name=<value>               : Name is <value>, where `user_name` can be any metadata field (e.g., first_name).
    
      uls .limit=<limit>                  : Limit the number of results to the top <limit> (e.g., 50).
      uls .offset=<offset>                : Return results starting at <offset>.
    
      uls .joined_before=<datetime>       : Returns users joined before (inclusive) given ISO 8601 timestamp (e.g., .before=2042-03-14).
      uls .joined_after=<datetime>        : Returns users joined after (inclusive) given ISO 8601 timestamp (e.g., .after=2120-10-15T00:00:00-08).
      uls .active_before=<datetime>       : (Root user only) Returns users last logged in before (inclusive) given ISO 8601 timestamp (e.g., .before=2042-03-14).
      uls .active_after=<datetime>        : (Root user only) Returns users last logged in after (inclusive) given ISO 8601 timestamp (e.g., .after=2120-10-15T00:00:00-08).
    
      uls .disk_used_less_than=<percentage> or <float>       : (Root user only) Returns users whose disk usage less than (inclusive) given value (e.g., .disk_used_less_than=70% or 0.3).
      uls .disk_used_more_than=<percentage> or <float>       : (Root user only) Returns users whose disk usage less than (inclusive) given value (e.g., .disk_used_more_than=70% or 0.3).
      uls .time_used_less_than=<<percentage> or <float>      : (Root user only) Returns users whose time usage less than (inclusive) given value (e.g., .time_used_less_than=70% or 0.3).
      uls .time_used_more_than=<percentage> or <float>       : (Root user only) Returns users whose time usage less than (inclusive) given value (e.g., .time_used_more_than=70% or 0.3).
    
      uls size=.sort                      : Sort by a particular field (where `size` can be any metadata field).
      uls size=.sort-                     : Sort by a particular field in reverse (e.g., `size`).
      uls .last                           : Sort in reverse chronological order (equivalent to id=.sort-).
      uls .count                          : Count the number of matching bundles.
      uls .format=<format>                : Apply <format> function (see worksheet markdown).
    Arguments:
      keywords     Keywords to search for.
      -f, --field  Print out these comma-separated fields.


## Commands for managing server
### workers
    Display information about workers that you have connected to the CodaLab instance.

### bs-add-partition
    Add another partition for storage (MultiDiskBundleStore only)
    Arguments:
      name  The name you'd like to give this partition for CodaLab.
      path  The target location you would like to use for storing bundles. This directory should be underneath a mountpoint for the partition you would like to use. You are responsible for configuring the mountpoint yourself.

### bs-rm-partition
    Remove a partition by its number (MultiDiskBundleStore only)
    Arguments:
      partition  The partition you want to remove.

### bs-ls-partitions
    List available partitions (MultiDiskBundleStore only)

### bs-health-check
    Perform a health check on the bundle store, garbage collecting bad files in the store. Performs a dry run by default, use -f to force removal.
    Arguments:
      -f, --force      Perform all garbage collection and database updates instead of just printing what would happen
      -d, --data-hash  Compute the digest for every bundle and compare against data_hash for consistency
      -r, --repair     When used with --force and --data-hash, repairs incorrect data_hash in existing bundles


## Other commands
### help
    Show usage information for commands.
      help           : Show brief description for all commands.
      help -v        : Show full usage information for all commands.
      help -v -m     : Show full usage information for all commands in Markdown format.
      help <command> : Show full usage information for <command>.
    Arguments:
      command         name of command to look up
      -v, --verbose   Display all options of all commands.
      -m, --markdown  Auto-generate all options of all commands for CLI markdown in Markdown format.

### status (st)
    Show current client status.

### alias
    Manage CodaLab instance aliases. These are mappings from names to CodaLab Worksheet servers.
      alias                   : List all aliases.
      alias <name>            : Shows which instance <name> is bound to.
      alias <name> <instance> : Binds <name> to <instance>.
    Arguments:
      name          Name of the alias (e.g., main).
      instance      Instance to bind the alias to (e.g., https://worksheets.codalab.org).
      -r, --remove  Remove this alias.

### config
    Set CodaLab configuration.
      config <key>         : Shows the value of <key>.
      config <key> <value> : Sets <key> to <value>.
    Arguments:
      key           key to set (e.g., cli/verbose).
      value         Instance to bind the alias to (e.g., https://worksheets.codalab.org).
      -r, --remove  Remove this key.

### logout
    Logout of the current session, or a specific instance.
    Arguments:
      alias  Alias or URL of instance from which to logout. Default is the current session.
