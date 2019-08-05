CodaLab worksheets are represented using the standard
[markdown](http://daringfireball.net/projects/markdown/syntax) with a few modifications.

- Lines that start with a coment (//) are simply instructions to you
  and are deleted by CodaLab.
- Some lines are reference to bundles and worksheets.
- Some lines are directives which tell CodaLab how to render the bundles.
- You can use the MathJax subset of LaTeX to render equations.

## An example

    // Editing worksheet pliang(0x17f9afe57d664a06b80c1a31f32456b6).
    // https://github.com/codalab/codalab-cli/worksheet-syntax.md
    //
    ## Heading
    This is *italics* and this is **bold**.
    This is a [link](http://codalab.org).
    This is an equation: $x^2$.
    Some code:

        x = 3

    A list:

    - one
    - two
    - three

    % schema simple1
    % add uuid uuid [0:8]
    % add name
    % add output /stdout
    % add time time duration

    Here are my runs, nicely formatted:
    % display table simple1
    [run run-date : date]{0x223abce2364c439596f05b5da0fa7e5d}
    [run run-date : date]{0x096b5b6b03a94ce3b47337db38822504}
    [run run-date : date]{0x1fc1dd80e2394655aa8117a21ed911b2}

## References to bundles

To enter a bundle reference, use a bundle specification (e.g., uuid):
    
    {<bundle_spec>}

The bundle reference will be presented to you as:

    [dataset worksheets-schema.png]{0x466cd19aeb204b59a13b213289de795a}

Bundle references must be by themselves on a single line.

## References to worksheets

Worksheet references work the same way as bundle references, but with two curly
braces instead of one:

    {{<worksheet_spec>}}

## Directives

A directive is a line that starts with a `%` and tells CodaLab how to render
the bundles.  For example, here's how to display an image:

    % display image / width=800
    [dataset worksheets-schema.png]{0x466cd19aeb204b59a13b213289de795a}

There are three types of directives for (i) defining schemas on the fly,
(ii) setting how the subsequent block of bundles are to displayed,
and (iii) displaying a set of bundles dynamically based on search criteria.

### Schemas

Suppose you have run 5 experiments, corresponding to 5 bundles and you would
like to display a table whose rows are the bundles and the columns are various
properties of the runs (e.g., time, memory, accuracy).  A schema allows you to
specify a list of fields and how to get the field value from the bundles.

Here is a simple example which shows the first 8 characters of the uuid, the
name, the stdout and the time:

    % schema simple1
    % add uuid uuid [0:8]
    % add name
    % add output /stdout
    % add time time duration

The general form of the commands:

    % schema <schema-name>
    % add <field-name> <generalized-path> [<post-processor>]
    % addschema <schema-name>

The generalized path can either refer to the bundle's metadata (type `cl info -r <bundle>` to get a complete list):

    uuid
    name
    command
    created
    data_size
    time
    memory

or a file inside the bundle (if prefixed by a '/'):

    /stdout

If a file `stats` is a JSON file

    {"errorRate": 0.2, "method": "simple"}

or a YAML file

    errorRate: 0.2
    method: simple

or a tab-separated file

    errorRate   0.2
    method	    simple

then we we can access particular fields inside:

    /stats:errorRate

If you have a nested JSON dictionary,

    {"train": {"errorRate": 0.2}}

you can access it with something like `/output/stats:train/errorRate`.

The post-processor, which is optional, specifies a function that transforms the
string value of the generalized path into another (usually more friendly)
string.  Formally, it is a sequence (separated by " | ") of the following functions:

    duration            # 61         => 1m1s
    date                # 1442513840 => 2015-09-17 11:17:20
    size                # 4125       => 4K
    %.3f                # 0.1234567  => 0.123
    s/a/b               # a-a        => b-b
    [2:4]               # abcdef     => cd

Here is a more complex post-processor example that prints out only the year:

    date | [0:4]

To display links, do:

    % add out uuid "key uuid | add path /stdout"

This should display a link labeled `out` that points to the `stdout` file in the given UUID.  Here's how it works:

    `uuid`                                 => '0x223abce2364c439596f05b5da0fa7e5d'
    `uuid` "key uuid"                      => {'uuid': '0x223abce2364c439596f05b5da0fa7e5d'}
    `uuid` "key uuid | add path /stdout"   => {'uuid': '0x223abce2364c439596f05b5da0fa7e5d', path: '/stdout'}

To change the text of the link do:

    % add out uuid "key uuid | add path /stdout | add text StandardOutput"

This dictionary is processed by the frontend to render the link.

### Display modes

By default, a bundle will be displayed as a table with default fields.
You can change this by putting a `% display <mode> ...` directive right before a block
of bundles with no intervening newlines:

    % display table simple1
    [run run-date : date]{0x223abce2364c439596f05b5da0fa7e5d}
    [run run-date : date]{0x096b5b6b03a94ce3b47337db38822504}
    [run run-date : date]{0x1fc1dd80e2394655aa8117a21ed911b2}

Here are the possible display modes.

1. Display the file contents of the generalized path:

        % display contents <generalized-path> [maxlines=<int>]
        % display contents /stdout maxlines=100
    
1. Display an image:

        % display image <generalized-path> [width=<int>,height=<int>]
        % display image /output.png width=300 height=50

1. Display HTML:
    
        % display html <generalized-path>
        % display html /output.html

1. Display a table given a pre-defined schema:

        % display table <schema-name-1> ... <schema-name-n>

1. Display a record given a pre-defined schema (where the rows are fields, so
there is a bit more room if you have schemas with lots of fields):

        % display record <schema-name-1> ... <schema-name-n>

1. Display a graph:

        % display graph <generalized-path> [display_name=<field>,x=<int>,y=<int>,xlabel=<string>,ylabel=<string>]
        % display /progress.tsv display_name=command xlabel=iteration ylabel=accuracy

The <generalized-path> should point to a TSV file.  For each subsequent bundle, the TSV file inside that bundle is read, and the columns corresponding to `x` and `y` are pulled out (defaulting to 0 and 1).  These are the points that are graphed in a line.  For display, `display_name` specifies the field that is used to pull out a name for the bundle in the legend, and `xlabel`/`ylabel` are just the labels of the axes.

### Displaying a dynamic set of bundles

To reference a set of bundles by a search criteria:

    % search <keyword-1> ... <keyword-n>

See documentation for `cl search` for more information.

## Frequently asked questions

### Can I have spaces in my directives?

Yes, you can quote them:

    % display "dataset size" data_size size
    % display error "/stats.json:error rate" %.3f
    % display created created "date | [0:4]"