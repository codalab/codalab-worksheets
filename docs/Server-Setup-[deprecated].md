This is the old Codalab Server Setup procedure. Keeping for reference and also because slightly relevant to what our docker service does behind the scenes:
Follow these instructions to setup your own CodaLab server.

## Get the source code

    git clone https://github.com/codalab/codalab-worksheets

In the following, `$HOME` will refer to the repository root of `codalab-worksheets`.

Configuration files will be stored in `$CODALAB_HOME`, which by default is `~/.codalab`.

## Install packages

Make sure you have the dependencies (Python 2.7 and virtualenv). If you're
running Ubuntu 16.04, you can just install them:

    sudo apt-get install -y python2.7 python2.7-dev python-virtualenv

Install node.js for the website:

    sudo apt-get install -y npm nodejs-legacy

Install MySQL for the backend:

    sudo apt-get install -y mysql-server libmysqlclient-dev

[Install Docker](Installing-Docker) so that you can run commands (instructions in the link).

Now, run the setup scripts to install the necessary
packages (in user space):

    (cd $HOME && ./setup.sh server && ./setup.sh frontend)


Activate the virtualenv created in the CLI repo folder for ease of use

    source $HOME/venv/bin/activate

If you use another shell like `zsh` or `fish` use:

    source $HOME/venv/bin/activate.fish|zsh

Activating the virtualenv appends the recently install codalab binaries to your PATH so you do not need to invoke `codalab/bin/cl` every time you want to use the Codalab CLI. If for some reason you cannot activate the virtualenv, just replace all the calls to `cl` or `cl-worker` with `$HOME/codalab/bin/cl` and `$HOME/codalab/bin/cl-worker` respectively.

## Configure the database

CodaLab uses a MySQL database to store all the bundle
information.

Type 

    sudo mysql -u root -p

and at the MySQL prompt type the following to create a `codalab` user and a
`codalab_bundles` database:

    CREATE USER 'codalab'@'localhost' IDENTIFIED BY '<password>';
    CREATE DATABASE codalab_bundles;
    GRANT ALL ON codalab_bundles.* TO 'codalab'@'localhost';

Configure CodaLab to use the database:

    cl config server/engine_url mysql://codalab:<password>@localhost:3306/codalab_bundles

## Configure email service

To allow users to register and receive email from your CodaLab server, you
should specify where email will be sent from:

    cl config email/host <host>
    cl config email/user <username>
    cl config email/password <password>

One option is to [SendGrid](https://sendgrid.com).  Google doesn't let you use
gmail in this way, unfortunately.  If you don't do this, then the emails will
be printed to the console, and you (as the administrator) will have to relay
that information to users.

Separately, notifications/errors will be sent to an admin email address, which
you can specify as follows:

    cl config admin-email <email>

## Configure the workers

Workers actually execute the commands to run bundles.  First, [install
docker](Installing-Docker).

Then create the root `codalab` user as follows, who can run bundles on behalf
of all users:

    cd $HOME && scripts/create-root-user.py

Create a file `$CODALAB_HOME/root.password` with `codalab` on the first line
and the password on the second:

    codalab
    <password>

Make sure this file is only accessible to you:

    chmod 600 $CODALAB_HOME/root.password

If you want your workers to support jobs that use NVIDIA GPUs (e.g. CUDA, cuDNN, etc.), you should install the `nvidia-docker` plugin on your machine as well. 
Instructions for installation are found here:
https://github.com/NVIDIA/nvidia-docker 

## Start NGINX

We use NGINX to put all CodaLab servers (website, bundle service) behind one
endpoint.

The `codalab-worksheets` repo comes with a `nginx.conf` file that provides a basic nginx config for running the 
frontend Web server and the REST server.

This file is set to listen on port 80, but for local development purposes you might want to change that to port 8080 for example.

Please get in touch with us if you have a more advanced use case.

Also note that we're planning a release within the next two months of a Dockerized deployment that will automatically set up nginx and mysql for you.

Follow the instructions below corresponding to your installation, which will guide you through
how to install NGINX and to point it at the Codalab `nginx.conf`. **Make sure
that you install at least version 1.7.x!**

#### Ubuntu 
By default, `apt-get` on Ubuntu may install a version of NGINX older than 0.7.x. Follow these instructions to ensure that you have the latest stable version ([source](https://www.nginx.com/resources/wiki/start/topics/tutorials/install/#ubuntu-ppa)).

    sudo -s
    nginx=stable # use nginx=development for latest development version
    add-apt-repository ppa:nginx/$nginx
    apt-get update
    apt-get install nginx

The easiest way to get nginx running is to add the line 

    include $HOME/codalab/nginx.conf

to the `http` block of `/etc/nginx/nginx.conf`

#### Mac using Homebrew

    brew install nginx
    cd $HOME
    sudo ln -sf $PWD/nginx.conf /usr/local/etc/nginx/servers/codalab.conf
    sudo nginx -s reload

#### Mac using MacPorts

    sudo /opt/local/bin/port install nginx
    cd $HOME
    sudo ln -sf $PWD/nginx.conf /opt/local/etc/nginx/codalab.conf

Make NGINX use that file by editing `/opt/local/etc/nginx/nginx.conf` and adding the following into the `http` section:

    include /opt/local/etc/nginx/codalab.conf;

Restart:

    sudo /opt/local/bin/port unload nginx
    sudo /opt/local/bin/port load nginx

## Start CodaLab services

Now, having configured everything, we are ready to launch CodaLab.  The CodaLab
deployment actually consists of six processes.

_**Recommendation**_: Start each of the processes below in a separate window inside
[Tmux](https://github.com/tmux/tmux).  Press Ctrl+C to kill
any of the processes (though you might want to do this with some care).

### Step 1: Start the website

This is the React app that serves HTML and Javascript, but relies on the
backend for data.

    cd $HOME/frontend
    serve -s build -l 2700

### Step 2: Start the API service

The worksheets API service provides the `/rest` endpoints which power the website, the
CLI, and any third-party applications.

    cl server

### Step 3: Start the the bundle manager

The bundle manager checks for bundles that need to be run (in the `staged`
state) and schedules them onto workers:

    cl bundle-manager

### Step 4: Start a worker

You can run 

    cl-worker --server http://localhost:2900 --password $CODALAB_HOME/root.password --verbose

### Step 5: Start the monitoring script (optional)

This script backs up the database periodically and does basic sanity checks
(tries to run jobs) to make sure that everything is behaving properly:

    cd $HOME
    ./monitor.py

## Create initial worksheets

Create the default worksheets and populate with initial content:

    cl new home
    cl new dashboard

## Test it out

Navigate to [http://localhost](http://localhost) (or whatever port you configured nginx to listen on).
Try signing up for an account, creating some worksheets and bundles.

Try out the CLI:

    cl run date -t

That's it!
