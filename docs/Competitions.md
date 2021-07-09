_This article is about hosting competitions on CodaLab **Worksheets**. To learn
more about CodaLab **Competitions**, which is an affiliated project built on a
separate stack, check out the [Competitions
Wiki](https://github.com/codalab/codalab-competitions/wiki)._

# List of competitions

The following competitions are hosted on CodaLab Worksheets ([rough Google query](https://www.google.com/search?q=codalab+submission+leaderboard+site%3Aworksheets.codalab.org%2Fworksheets)):

- [SQuAD](https://stanford-qa.com/) [[instructions](https://worksheets.codalab.org/worksheets/0x8212d84ca41c4150b555a075b19ccc05/)]: question answering
- [HotpotQA](https://hotpotqa.github.io/) [[instructions](https://worksheets.codalab.org/worksheets/0xa8718c1a5e9e470e84a7d5fb3ab1dde2/)]: multi-hop question answering
- [QAngaroo](http://qangaroo.cs.ucl.ac.uk/) [[instructions](https://worksheets.codalab.org/worksheets/0x9acb78d24d454203ae197439130def65/)]: multi-hop question answering (WikiHop and MedHop)
- [MultiRC](http://cogcomp.org/multirc/) [[instructions](https://worksheets.codalab.org/worksheets/0x55757d643dde4692b9b515575e45487e/)]: multi-hop question answering
- [CoQA](https://stanfordnlp.github.io/coqa/) [[instructions](https://github.com/stanfordnlp/coqa-baselines/blob/master/codalab.md)]: conversational question answering
- [QuAC](https://quac.ai/) [[instructions](https://worksheets.codalab.org/worksheets/0x6c09e167a1884d359b171e13b80b95d3/)]: conversational question answering
- [ShARC](https://sharc-data.github.io/) [[instructions](https://worksheets.codalab.org/worksheets/0xcd87fe339fa2493aac9396a3a27bbae8/)]: conversational question answering
- [QANTA](https://pinafore.github.io/qanta-leaderboard/) [[instructions](https://worksheets.codalab.org/worksheets/0x2f5d8362ba434c578e455a6344513e9b/)]: question answering on Quizbowl
- [KorQuAD](https://korquad.github.io/) [[instructions](https://worksheets.codalab.org/worksheets/0x7b06f2ebd0584748a3a281018e7d19b0/)]: Korean question answering
- [RecipeQA](https://hucvl.github.io/recipeqa/) [[instructions](https://worksheets.codalab.org/worksheets/0x44226bd1e87546f1bdaea162090c4a7d/)]: multimodal comprehension of cooking recipes
- [MRQA2019](https://mrqa.github.io/shared/) [[instructions](https://worksheets.codalab.org/worksheets/0x926e37ac8b4941f793bf9b9758cc01be/)]: question answering
- [CMRC2018](https://hfl-rc.github.io/cmrc2018/submission/) [[instructions](https://worksheets.codalab.org/worksheets/0x96f61ee5e9914aee8b54bd11e66ec647)]: Chinese question answering
- [SMP2018](https://smp2018ecdt.github.io/Leader-board/) [[instructions](https://worksheets.codalab.org/worksheets/0x1a7d7d33243c476984ff3d151c4977d4)]: Chinese dialogue
- [Spider](https://yale-lily.github.io/spider) [[instructions](https://worksheets.codalab.org/worksheets/0x10cf3ba43d784d77a5fc62a50b96f1e2)]: semantic parsing
- [COIN](https://coinnlp.github.io/) [[instructions](https://worksheets.codalab.org/worksheets/0x683ccf06dbe34c0384465f861020f917/)]: commonsense inference
- [HYPE](https://hype.stanford.edu/) [[instructions](https://worksheets.codalab.org/worksheets/0xcd8c3390ab394a50b047ee86a9f84fa0/)]: image generation
- [CheXpert](https://stanfordmlgroup.github.io/competitions/chexpert/) [[instructions](https://worksheets.codalab.org/worksheets/0x693b0063ee504702b21f94ffb2d99c6d/)]: chest x-ray interpretation
- [MURA](https://stanfordmlgroup.github.io/competitions/mura/) [[instructions](https://worksheets.codalab.org/worksheets/0x42dda565716a4ee08d61f0a23656d8c0/)]: bone x-ray interpretation

# Hosting a competition

The job execution and provenance facilities of CodaLab Worksheets enable a wide variety of applications in computational 
experimentation, including hosting competitions on machine learning tasks.

We provide a small application built on the [CodaLab Worksheets API](REST-API-Reference.md) for running small-scale 
Kaggle-like competitions. It is packaged as a simple Python script. Competition participants can submit their models to 
the competition by running them on an public dataset provided by the competition organizers, then tagging the resulting 
predictions bundle with a specific tag. The script searches for bundles with the configured tag, and "mimics" the submitted 
executions -- rerunning the models while replacing the public dataset with the hidden evaluation dataset. Competition 
organizers can run this script manually, as a cron job, or as a long-lived daemon.

Once you have finished setting up the submission tutorial worksheet and generating your leaderboard in JSON format, 
please fill out this [form](https://docs.google.com/forms/d/e/1FAIpQLSdFdQTi4i0uXfXR00JD40HlW-j-Np2XCacgaPSZVfBdL4QwQg/viewform?usp=sf_link) 
to register your competition. 

## Installation and setup

Install codalab package and its dependencies:

    pip3 install codalab

Minimally verify CodaLab is installed properly by running the competition script with the help 
flag `-h`. The output should look something like this:

    cl-competitiond -h

    usage: cl-competitiond [-h] [-l] [-d] [-v] config_file output_path

    Competition leaderboard evaluation daemon.

        1. Find bundles tagged with {submission_tag} and filter them.
        2. Run the {predict} command with the submitted bundle to generate
           predictions on the test set.
        3. Tag the resulting test run bundle with {predict.tag}, untagging
           any previous test run bundles for the same submitter.
        4. Run {evaluate} command with the test run bundle.
        5. Tag the resulting evaluation bundle with {evaluate.tag}, untagging
           any previous evaluation bundles for the same submitter.

    If in daemon mode, performs the above steps in a loop every
    {refresh_period_seconds} seconds. Otherwise, just runs through them once.

    All bundles created by this daemon are added to {log_worksheet_uuid}.
    Each user will be limited to {max_submissions_per_period} every
    {quota_period_seconds}, and {max_submissions_total} ever.

    positional arguments:
      config_file           YAML/JSON file containing configurations.
      output_path           path to write JSON file containing leaderboard.

    optional arguments:
      -h, --help            show this help message and exit
      -l, --leaderboard-only
                            Generate a new leaderboard but without creating any new runs.
      -d, --daemon          Run as a daemon. (By default only runs once.)
      -v, --verbose         Output verbose log messages.

The competition script takes in a config file (in YAML or JSON format) and performs the actions
listed above, then generates a leaderboard in JSON format at the specified output path.

Construct a config file for your competition by starting up your favorite editor and creating
a new file, say, at path `~/competition-config.yml` (this can be anywhere). Use the following
as a template:

```yaml
# Allows at most 5 submissions per user per period, where period is 24 hours by default.
max_submissions_per_period: 5

# UUID of the worksheet where prediction and evaluation bundles are created for submissions.
log_worksheet_uuid: '0x2263f854a967abcabade0b6c88f51f29'    

# Configure the tag that participants use to submit to the competition.
# In this example, any bundle with the tag `some-competition-submit` would be
# considered as an official submission.
submission_tag: some-competition-submit

# Configure how to mimic the submitted prediction bundles. When evaluating a submission, 
# `new` bundle will replace `old` bundle.
# For a machine learning competition, `old` bundle might be the dev set and `new` bundle
# might be the hidden test set.
predict:
  mimic:
  - {new: '0xbcd57bee090b421c982906709c8c27e1', old: '0x4870af25abc94b0687a1927fcec66392'}

# Configure how to evaluate the new prediction bundles.
# In this example, evaluate.py is script that takes in the paths of the test labels and 
# predicted labels and outputs the evaluation results.
evaluate:
  # Essentially
  #     cl run evaluate.py:0x089063eb85b64b239b342405b5ebab57 \
  #            test.json:0x5538cba32e524fad8b005cd19abb9f95 \
  #            predictions.json:{predict}/predictions.json --- \
  #            python evaluate.py test.json predictions.json
  # where {predict} gets filled in with the uuid of the mimicked bundle above.
  dependencies:
  - {child_path: evaluate.py, parent_uuid: '0x089063eb85b64b239b342405b5ebab57'}
  - {child_path: test.json, parent_uuid: '0x5538cba32e524fad8b005cd19abb9f95'}
  - {child_path: predictions.json, parent_path: predictions.json, parent_uuid: '{predict}'}
  command: python evaluate.py test.json predictions.json

# Define how to extract the scores from the evaluation bundle.
# In this example, result.json is a JSON file outputted from the evaluation step
# with F1 and exact match metrics (e.g. {"f1": 91, "exact_match": 92}).
score_specs:
- {key: '/result.json:f1', name: f1}
- {key: '/result.json:exact_match', name: exact_match}
```

## Running the competition

There are many ways you could choose to set up the competition. The recommended way is to set up a static webpage that 
loads the generated leaderboard JSON file and formats it into a leaderboard table using a simple templating language such 
as [Mustache](https://mustache.github.io/). We even provide an 
[example HTML page that does just that](https://github.com/codalab/codalab-cli/blob/ecbc9146918415b3a53d1e61dc8c9c9185cc10ba/scripts/leaderboard.html). 
Just make sure to tweak it for your own purposes.

Running the competition script in daemon mode will start a long-running process that periodically checks for new submissions, 
runs them, then updates the leaderboard file.

    cl-competitiond -d ~/competition-config.yml /var/www/leaderboard.json

## Submitting a model (as a participant)

For the participant, submitting a model involves uploading their code, running it against a public dataset provided by the 
competition organizers, tagging the bundle appropriately, then waiting for the next time the competition script checks for 
new submissions. The [submission tutorial for the SQuAD competition](https://worksheets.codalab.org/worksheets/0x8403d867f9a3444685c344f4f0bc8d34/) 
provides a good example.

When submitting a bundle, please ensure none of your submission bundle's dependencies have been deleted. Otherwise,
your submission bundle will be disregarded as an invalid submission.

## FAQ / Known issues

* Q: How do I reset the quota for a participant?
  * A: Delete their associated evaluation bundles from the log worksheet, and their quota values should go down accordingly 
  the next time that the leaderboard is generated.
* Q: Which submissions correspond to the scores displayed in the leaderboard?
  * A: The last submission submitted by each participant chronologically. (No max is performed.)
* Q: Why is there a finite `max_leaderboard_size`? Why not just support any leaderboard size?
  * A: Because of a quirk in the CodaLab search API, we need to specific a finite "limit" to the number of search results. 
  Just make this number big enough and it should be fine.
* Q: This script is really slow.
  * A: The script is pretty bare bones and doesn't do much in the way of optimizing API calls. The implementation is pretty small, 
  so feel free to shoot us some pull requests to make it better!
* Q: How can I let participants debug their own submissions when they fail?
  * A: There are some rare cases where a submission succeeds on the public dataset but fails on the hidden dataset. Unfortunately, 
  the competition organizers will have to manually send the participants stacktraces for those failed runs, unless they are willing 
  to make the prediction bundles public (`make_predictions_public: true` in config).
