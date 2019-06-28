#!/bin/bash
# Setup script for Linux.

# Exit immediately if any command fails
set -e

# Note: should deprecate this file, since people should either use docker for
# development or just pip install codalab.

# ======================================
#  COLORS
# ======================================
bold="\033[1m"
reset="\033[0m"

warning="${bold}\033[33m"  # yellow
info="${bold}\033[36m"     # cyan
success="${bold}\033[32m"  # green
# =======================================

# Ensure proper command usage
if [ "$#" -ne 1 ] || ( [ "$1" != "client" ] && [ "$1" != "server" ] && [ "$1" != "frontend" ] ); then
  echo "Usage:"
  echo "  $0 [client | server | frontend]"
  exit 1
fi

# Virtualenv must be for standard Python 2.x distribution (not conda)
echo -e "${info}[*] Checking for virtualenv --> Must be for standard Python 2.x (not conda) ...${reset}"
if ! which virtualenv; then
  echo -e "${warning}[!] virtualenv is not found.${reset}"
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

$env/bin/pip install -U setuptools pip

echo -e "${info}[*] Installing Python packages into $env...${reset}"
if [ "$1" == "server" ]; then
  $env/bin/pip install -r $codalabdir/requirements-server.txt
elif [ "$1" == "client" ]; then
  $env/bin/pip install -r $codalabdir/requirements.txt
elif [ "$1" == "frontend" ]; then
  echo -e "${info}[*] Running npm build for frontend...${reset}"
  if ! which npm; then
    echo -e "${warning}[!] npm is not found.${reset}"
    echo -e "${warning}[!] You need npm if you want to set up the front end web server.${reset}"
    echo -e "${warning}[!] If you are using Ubuntu run the following to install:${reset}"
    echo
    echo -e "${warning}  sudo apt-get install ${reset}"
    exit 1
  else
    cd frontend
    npm install
    npm install -g serve
    npm run build
    echo -e "${info}  Frontend server installed. You can start server with the following command:${reset}"
    echo
    echo -e "${info}  serve -s build -l 2700${reset}"
    exit
  fi
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

echo
echo -e "${warning}[!] Add the following line to your .bashrc to put CodaLab in your path:${reset}"
echo -e "${warning}  export PATH=\$PATH:$PWD/codalab/bin${reset}"
echo
echo -e "${info}[*] Then you can use CodaLab with the single command:${reset}"
echo -e "${info}  cl${reset}"
echo
echo -e "${success}[!] Successfully installed CodaLab in $1 mode!${reset}"
