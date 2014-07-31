#!/bin/bash

# Setup script for Linux.

echo "=== Checking for virtualenv..."
if ! which virtualenv; then
  echo "Python virtualenv is not installed."
  echo "If you are using Ubuntu, run the following to install:"
  echo
  echo "  sudo apt-get install python-virtualenv"
  exit 1
fi
echo

env=`dirname $0`/codalab_env

if [ ! -e $env ]; then
  echo "=== Setup a Python virtual environment (in $env)..."
  virtualenv -p /usr/bin/python2.7 $env --no-site-packages || exit 1
  echo
fi

echo "=== Install Python packages into $env..."
$env/bin/pip install sqlalchemy || exit 1
echo

echo "=== Add the following line to your .bashrc to put CodaLab in your path:"
echo
echo "  export PATH=\$PATH:$PWD/codalab/bin"
echo
echo "Then you can use Codalab with the single command:"
echo
echo "  cl"
