#!/bin/bash
# Setup script for Linux.

# Exit immediately if any command fails
set -e

echo "=== Checking for virtualenv..."
if ! which virtualenv; then
  echo "Python virtualenv is not installed."
  echo "If you are using Ubuntu, run the following to install:"
  echo
  echo "  sudo apt-get install python-virtualenv"
  exit 1
fi
echo

codalabdir=`dirname $0`
env=$codalabdir/venv

if [ ! -e $env ]; then
  echo "=== Setup a Python virtual environment (in $env)..."
  virtualenv -p /usr/bin/python2.7 $env
  echo
fi

echo "=== Install Python packages into $env..."
$env/bin/pip install -r $codalabdir/requirements.txt

# Use sckoo@stanford.edu's patched argcomplete, until pull request has been accepted
# https://github.com/kislyuk/argcomplete/pull/118
$env/bin/pip install git+https://github.com/kashizui/argcomplete.git@allow-overridden-subparsersaction

( # try
    $env/bin/pip install psutil || exit 1
) || ( # catch
    echo
    echo "  psutil failed to install"
    echo "This is most likely happening because of missing python-dev"
    echo "If you are using Ubuntu, run the following to install:"
    echo
    echo "  sudo apt-get install python-dev"
    echo
    exit 3
)
if [ $? = 3 ]; then
  exit
fi

echo "=== Initializing the database..."
$env/bin/alembic stamp head

echo
echo "=== Add the following line to your .bashrc to put CodaLab in your path:"
echo
echo "  export PATH=\$PATH:$PWD/codalab/bin"
echo
echo "Then you can use Codalab with the single command:"
echo
echo "  cl"
