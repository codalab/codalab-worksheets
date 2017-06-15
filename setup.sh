#!/bin/bash
# Setup script for Linux.

# Exit immediately if any command fails
set -e


# ======================================
#  COLORS
# ======================================
bold="\033[1m"
reset="\033[0m"

warning="${bold}\033[33m"  # yellow
info="${bold}\033[36m"     # cyan
success="${bold}\033[32m"  # green
# =======================================


if [ "$#" -ne 1 ] || ( [ "$1" != "client" ] && [ "$1" != "server" ] && [ "$1" != "dev" ] ); then
  echo "Usage:"
  echo "  $0 [client | server | dev]"
  exit 1
fi

echo -e "${info}[*] Checking for virtualenv...${reset}"
if ! which virtualenv; then
  echo -e "${warning}[!] Python virtualenv is not installed.${reset}"
  echo -e "${warning}[!] If you are using Ubuntu, run the following to install:${reset}"
  echo
  echo -e "${warning}  sudo apt-get install python-virtualenv${reset}"
  exit 1
fi
echo

codalabdir=`dirname $0`
env=$codalabdir/venv

if [ ! -e $env ]; then
  echo -e "${info}[*] Setting up a Python virtual environment (in $env)...${reset}"
  virtualenv -p /usr/bin/python2.7 $env
  echo
fi

$env/bin/pip install -U setuptools

echo -e "${info}[*] Installing Python packages into $env...${reset}"
if [ "$1" == "server" ]; then
  $env/bin/pip install -e $codalabdir/worker
  $env/bin/pip install -r $codalabdir/requirements-server.txt
elif [ "$1" == "dev" ]; then
  $env/bin/pip install -r $codalabdir/requirements-dev.txt
else
  $env/bin/pip install -r $codalabdir/requirements.txt
fi

( # try
    $env/bin/pip install psutil || exit 1
) || ( # catch
    echo
    echo -e "${warning}[!] psutil failed to install.${reset}"
    echo -e "${warning}[!] This is most likely happening because of missing python-dev.${reset}"
    echo -e "${warning}[!] If you are using Ubuntu, run the following to install:${reset}"
    echo
    echo -e "${info}  sudo apt-get install python-dev${reset}"
    echo
    exit 3
)
if [ $? = 3 ]; then
  exit
fi

#echo "=== Initializing the database..."
#$env/bin/alembic stamp head

echo
echo -e "${warning}[!] Add the following line to your .bashrc to put CodaLab in your path:${reset}"
echo -e "${warning}  export PATH=\$PATH:$PWD/codalab/bin${reset}"
echo
echo -e "${info}[*] Then you can use Codalab with the single command:${reset}"
echo -e "${info}  cl${reset}"
echo
echo -e "${success}[!] Successfully installed Codalab in $1 mode!${reset}"
