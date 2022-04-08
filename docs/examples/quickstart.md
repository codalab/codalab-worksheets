# Tutorial: Quickstart
 
See the [examples GitHub repository](https://github.com/codalab/worksheets-examples/tree/master/00-quickstart) for the code related to this tutorial.


**Estimated time:** 15 minutes

In this tutorial, you will use the CodaLab UI to create a CodaLab account, install the CodaLab CLI,
upload some code and data, and run your first experiment.

## 1. Create an account

1.  Go to [http://worksheets.codalab.org](http://worksheets.codalab.org) and click "Sign Up" in the top-right corner of the navigation bar.
2.  Fill out the subsequent form.
3.  A verification email will be sent to the email address you used to sign up. When you open it, there will be a link to follow in order to verify your account.
4.  After verifying your account, sign in again. This will bring you to your **User Profile Page**. 
This is the page where contains your account information, and an overview of your bundles and worksheets. 
You can get back to this page at any time by clicking the "MY WORKSHEETS" button in the top-right of the navigation bar or directly going to [http://worksheets.codalab.org/users](http://worksheets.codalab.org/users)
![Dashboard](../images/quickstart/profile.png)
5. You can hover over your avatar in the side bar and then click to upload and edit your profile image.
![Dashboard](../images/quickstart/edit-avatar.png)
6. Your account information, including the usage of disk and time (these will only be visible to you), will be shown in the side bar of the dashboard.
7. Click on the "Add" button, or the "New Worksheet" icon to create a new worksheet and start exploring!
![Dashboard](../images/quickstart/add-worksheet.png)

#
*You can also view the old "My Dashboard" worksheet by clicking on the "VIEW DASHBOARD" button on the profile page or directly going to [http://worksheets.codalab.org/worksheets/?name=dashboard](http://worksheets.codalab.org/worksheets/?name=dashboard).
This is a **special, read-only worksheet** that contains an
overview of your bundles and worksheets.*



## 2. Install the CLI

You can do many things from the web interface,
but to be truly productive on CodaLab, you should install
the CodaLab command-line interface (CLI).
We assume you already have Python 3 (Python 2 is no longer supported) and pip installed.
Open a terminal and run the following:

    $ pip install codalab -U --user

Or if you'd rather install it in a virtualenv:

    $ virtualenv -p python3.6 venv
    Running virtualenv with interpreter /usr/bin/python3.6
    ...
    $ . venv/bin/activate
    (venv) $ pip install codalab -U

*Note.* If you are in the Stanford NLP group, you can ssh into the NLP cluster,
where the CodaLab CLI should already be installed (`/u/nlp/bin/cl`).

To login from the CLI, type:

    $ cl work
    Requesting access at https://worksheets.codalab.org
    Username: guest1
    Password:
    Currently on worksheet https://worksheets.codalab.org::home-guest1(0x39729afdca6140869a11e055e4cc0649).

The CLI is associated with a **current worksheet**, which is by default pointed
to your home worksheet (`home-<username>`).

## 3. Uploading files

We will now run your first experiment in CodaLab.  We will run a simple Python
script that simply sorts the lines of a text file.

Clone the example repo from [https://github.com/codalab/worksheets-examples/tree/master/00-quickstart](https://github.com/codalab/worksheets-examples/tree/master/00-quickstart).

Locally, make sure you stay in the `worksheets-examples/00-quickstart` directory for the rest of this tutorial:

    $ cd 00-quickstart

In our example, the `data` and `code` directories have the following contents:

`data/lines.txt`:

    e
    d
    c
    b
    a

`code/sort.py`

    import sys
    for line in sorted(sys.stdin.readlines()):
    	  print(line)

We can upload the `data` and `code` as two separate bundles.  Each command outputs the UUID of the bundle that was created,
a globally unique identifier that you can use to refer to that particular bundle.
By default, the name of the bundle (which need not be unique) is the name of the file/directory you're uploading (which can be overridden with `-n <name>`).

    $ cl upload data
    Preparing upload archive...
    Uploading data.tar.gz (0x7f321e61544f437a8cc292882b91d302) to https://worksheets.codalab.org
    Sent 0.01MiB [0.03MiB/sec]    			
    0x7f321e61544f437a8cc292882b91d302
    $ cl upload code
    Preparing upload archive...
    Uploading code.tar.gz (0x92ddae772dca44a69c56bcac3711bc54) to https://worksheets.codalab.org
    Sent 0.01MiB [0.03MiB/sec]    			
    0x92ddae772dca44a69c56bcac3711bc54

Refresh the web interface (shift-R) to see the data and code bundles:

![Data and code](../images/quickstart/data-code.png)

You can look at their contents and other information in the down panel by clicking on the drop arrow icon.
![Data and code](../images/quickstart/bundle-detail.png)

## 4. Running an experiment

Now we're going to create a run bundle that executes the code on the data:

    $ cl run :data :code 'python code/sort.py < data/lines.txt'
    0x2f696f1483ba47a6a45b6baff6ff63ad

In this command, `:data` and `:code` specify the dependencies, and the command
is the arbitrary shell command that will be run.
When CodaLab executes this command, it will mount the dependencies `data` and `code` to the particular bundles that their names refer to, like this:

    data -> 0x7f321e61544f437a8cc292882b91d302
    code -> 0x92ddae772dca44a69c56bcac3711bc54

On the web interface, you can see the run going:

![Run](../images/quickstart/run-python.png)

Congratulations, you just created your first experiment in CodaLab!
To do a real NLP task, go to the [next tutorial](quickstart.md).
