A CodaLab executable paper is a worksheet that contains the code, data, main experiments, tables, and graphs used in a published paper (think of it as an appendix or supplementary material).

Since CodaLab keeps track of the full provenance generating a particular result, executable papers allow someone to independently verify the results in a published paper.  Furthermore, it is likely that the original authors did not explore the full space of possible experiments, but one can easily use CodaLab to run variants of the paper to answer new questions (`cl mimic` makes this super easy).

> ***Executable papers make it easy to verify, explore, and extend publish work.***

## **Guidelines for an executable paper**

The executable is not (currently) meant to be a replacement of your original
paper, but rather should be viewed as supplementary material.  It should
contain an explanation of the experiments as well as how they can be modified.
The full pipeline from raw data to final results (a table or graph) should be
represented.

- **Description**: Start by linking to the main paper.  Briefly describe the
  problem that you're solving and the solution.  Draw a figure if possible.
- **Data**: Upload the dataset(s) that you are using in raw format (i.e., don't
  upload a file with integers that encode some featurization of a document).
  Upload your preprocessing scripts and run your preprocessing steps as
  bundles in CodaLab.  This will make it easier to change the preprocessing
  later or have more transparency about these decisions, which while
  not part of your main algorithm, could be important for explaining
  your empirical results.
- **Code**: You should release your code (e.g., using
  [GitHub](https://github.com) or [mloss](http://mloss.org)).  Upload a
  snapshot version of your code to CodaLab and create a separate run bundle to
  compile it.  This ensures that someone who wants to modify the code can
  recompile it.  If you use third-party libraries, make them separate bundles
  that the compilation depends on.
- **Experiments**: Run the main experiments with the code on the data.
  If possible, output the predictions (at least on the test set) to a file,
  so that further analysis can be performed easily.  Ideally, you would
  use different run bundles to generate the results in a TSV or JSON file,
  or generate a graph.  You might find it useful to output HTML files to
  customize your output.

Here are some general tips:
- Check if the dataset you're using already exists on CodaLab to avoid
  uploading duplicate copies.
- Make sure that your run deletes large temporary files (so that you're not
  using unnecessary disk space).
- Try to break up your code/data into modules if it is useful to swap out
  different versions of a module.  This makes the pipeline more transparent,
  but also don't overdo it.
- Try to output your experimental results in a JSON or TSV file, so that you
  can use CodaLab's table formatting features to display this structured
  output.
- To copy someone's executable paper, simply copy the contents of the worksheet
  (you can use `cl wadd`) to your own worksheet, and then start using `cl mimic`
  to make modifications!

## **An example walkthrough**

Here is a concrete example of how to create an executable paper.  You should 
Note: in the following, replace `pliang` with your username.

### **Setup**

1. Create an account by going to [CodaLab website](https://worksheets.codalab.org),
clicking 'Sign Up'.  You will get an email confirmation; click on the link to activate
your account.  Now you can sign in through the browser.  Optionally, you can install the
[command-line interface (CLI)](CLI-Basics).

1. For your new executable paper, create a new worksheet (you should use a
different worksheet name than the one below).  Click on 'New Worksheet'
in the top of the side panel on the web interface or type the following:

        cl new magic-acl2015
        cl work magic-acl2015   # Switch to this worksheet

  This should print out the UUID of your worksheet.  For the name of
  worksheets, try to follow the convention of having a brief description
  followed by the conference and the year.  Note that these names are like
  identifier names in programs and cannot contain spaces or other weird
  characters.

1. Add a description of your worksheet.  Click 'Edit Source' or type:

        cl wedit

  Add a description of your paper:

        This worksheet contains the experiments from the following paper:

        > Percy Liang.
        >
        > [Language Understanding via Magic](http://www.cs.stanford.edu/~pliang/work_in_progress.pdf)
        >
        > Association for Computational Linguistics (ACL), 2015.

        # Background

        (explain the problem)

  You can look at the markdown source of another executable paper here for reference:

        cl wedit main::sempre-tables-acl2015

### **Uploading your executable paper**

Now you are ready to actually upload content.  The plan is to upload your
source code, libraries, datasets, evaluation program as separate bundles.  You
then compile the source code, run your algorithm, and run the evaluation
program.  Note: for your particular paper, you might choose to structure things differently,
so treat this only as a rough guide.  See [the workflow](Workflow) for a
similar example.

1. From the CLI, you can upload bundles (with meaningful descriptions):

        cl upload src -d "Source code for magic."
        cl upload lib -d "Libraries needed for magic."
        cl upload evaluation.py -d "Magical evaluation program."
        cl upload data -d "Magical data."
        cl upload Makefile -d "For compiling."

        cl ls    # Make sure that everything is uploaded correctly

    From the web terminal, you can upload bundles by first zipping them up and
    clicking the "Upload" button on the top of the side panel.

1. Compile the source code.  The details of this depend on how you have your
code setup.  As an example, suppose `src` contains your source code and it has
a `Makefile` inside it.  Compilation also depends on some libraries `lib`.
In this case, we will run a command that first copies `src` to `build` and runs
`make`.  This is needed because `src` is an input bundle which is immutable,
and `make` presumably creates new files (e.g., `bin`) in the directory from
which it's run.

        cl run :Makefile :src :lib 'make' -n compile

  The `cl run` runs the bash command in a temporary directory with only access
  to the dependent bundles `src` and `lib`.  The resulting bundle is called `compile`.

1. Run your program with different settings:

        cl run :bin :data 'bin/learn --input-dir data --output-dir . --num-iters 100' -n learn-baseline
        cl run :bin :data 'bin/learn --input-dir data --output-dir . --num-iters 100 --magic' -n learn-magic

  These runs run the given command in a temporary directory with access to
  `bin` and `data`.  Note that we simply output any files to the current
  directory.

1. Rendering your results:

  Suppose your program outputs a JSON or TSV file `stats.json` describing the
  statistics of the run (including command-line options).  Then you can
  customize the rendering of these bundles by editing (via `cl wedit`) and
  putting the following directives before the two bundles:

        % schema custom
        % addschema run
        % add trainError /stats.json:trainError %.3f
        % add testError /stats.json:testError %.3f
        % display table custom

  If your program outputs images you can display them as follows:

        % display image /graph.png

  You can have two instances of the same bundle on a worksheet, so you can have
  both a graph and a table.  See the [worksheet markdown](Worksheet-Markdown) for full details.

1. Run the official evaluation script on the predictions output by your system,
so if other people use your dataset, they can use the exact same evaluation
script to make sure you have comparable numbers:

        cl run :evaluation.py true:data pred:learn-magic/predictions.txt 'python evaluation.py true pred' -n run-eval

  Note that you can name the dependencies (`true` and `pred`) that the program sees.

1. **Important**: Add the `paper` tag to your worksheet so that your paper will be easily searchable:

        cl wedit magic-acl2015 --tags paper
