# Basic Requirements

Before working on CodaLab, you should be able to successfully build and run the project.  Remember to follow basic [Git hygiene](http://blog.ericbmerritt.com/2011/09/21/commit-hygiene-and-git.html). CodaLab is built using [Python](http://www.python.org/) and [Django](https://www.djangoproject.com/), so you should be familiar with these technologies.

# Branches
In the [CodaLab Github Repo](http://github.com/codalab/codalab) there may be many branches. We want to be very careful about two specific branches to ensure the quality of the codebase stays high.
1. **release** - This is the branch whose HEAD is the current released version that is in production. We only push staging to release on weekly deploys after thorough testing.

2. **staging** - This is the branch where new patches are tested before being deployed to production. Code should only be pushed to staging from master as part of the weekly deploys for testing.

3. **master** - This is the branch that should have the all the completed features on it. While it is not thoroughly tested or production ready, automated builds and QA should be passing on it. Code should only be merged onto master from approved PRs.


## Get the Source Code
Follow these instructions to create a local clone of the CodaLab source code.

1. [Fork](https://help.github.com/articles/fork-a-repo) the [CodaLab repo](https://github.com/codalab/codalab-cli) from GitHub.

1. Clone the fork to your local computer.
    ```
    git clone https://github.com/<username>/codalab-cli.git
    ```

## Create a 'blessed' Branch
Follow these optional steps to create a 'blessed' master branch, which is an "untouched" master that can be updated to get the latest changes with `git pull`.

1. Open a command prompt and navigate to the CodaLab folder.
1. Assign the original repo to a remote called "blessed".
    ```
    git remote add blessed https://github.com/codalab/codalab-cli.git
    ```

1. Get the latest changes from blessed:
    ```
    git checkout master
    git pull blessed master
    ```

## Use Topic Branches
Topic branches are typically lightweight branches that you create locally and that have a name that is meaningful for you. They are where you might do work for a bug fix or feature (they're also called feature branches). Follow these steps to create a topic branch:

1. In your GitHub repository, create a [topic branch](http://learn.github.com/p/branching.html). 

    `git checkout -b branch_name`
    
    Make all of your changes in the topic branch, and consider the master branch to be  a reference of the latest version.

1. Commit your changes:
```git commit -a -m "A meaningful description of my commit."```

1. Push your local changes to your fork:
```git push origin <branch_name>```
    Now you're ready to make a pull request.

## Pull Requests

When the code in your GitHub fork is ready to make its way into the master branch of the CodaLab GitHub account, you should [submit a pull request](https://help.github.com/articles/using-pull-requests#initiating-the-pull-request). Be detailed in your description of the changes.

When you submit a pull request, GitHub will let you know if the request cannot be merged automatically. This GitHub page explains [how to merge a pull request](https://help.github.com/articles/merging-a-pull-request).

**Other assumptions:**

* Committers will review pull requests and fold them into the master branch of the CodaLab account.
* Developers should ask for code reviews of their code when they feel like it's getting ready to pull into master. This is easily done by adding github id's to the message on the pull request (mentioning them sends a notification).
* If a change is going to affect the user experience, it should include code reviews/discussions (again through pull requests mentions) because this is a broad platform, intended for use by many diverse communities.
* Merges to master and deployment to production will be done by the project QA/Release Engineer.

# Best practices

* DO read and follow [the process](https://github.com/codalab/codalab/wiki/Dev_Issue-tracking) for managing tasks and bugs.
* DO associate your commit with the issue that you are fixing. The mechanism for doing so is explained in this [GitHub Blog entry](https://github.com/blog/831-issues-2-0-the-next-generation) and in this [StackOverflow question](http://stackoverflow.com/questions/1687262/link-to-github-issue-number-with-commit-message).
* DO make sure to add unit tests as you add new features.
* DO make sure to add unit tests as you fix bugs that could have been fixed by unit testing.
* DO use ambv/black (as documented at https://github.com/codalab/codalab/wiki/Dev_Code-checkers) to validate your code formatting matches our formatting standards.

## Additional Resources

Finally, here is some recommended reading for participating in an open source project:
- [Open Source Contribution Etiquette](http://tirania.org/blog/archive/2010/Dec-31.html) by Miguel de Icaza
- [Don't "Push" your Pull Requests](http://www.igvita.com/2011/12/19/dont-push-your-pull-requests/) by Ilya Grigorik
- [A Successful Git Branching Model](http://nvie.com/posts/a-successful-git-branching-model/) by Vincent Driessen
- CodaLab is built using [Python](http://www.python.org/) and [Django](https://www.djangoproject.com/).