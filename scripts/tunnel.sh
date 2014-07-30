#!/bin/bash

if [ $# != 1 ]; then
  echo "Usage: `basename $0` <hostname>"
  echo
  echo "Opens a port on the local machine which ssh tunnels to the bundle service and website,"
  echo "assumed to be running on <hostname>.  Kills existing ssh tunnels on those ports."
  exit 1
fi

host=$1

# Bundle service
port=2800
remoteport=1$port
echo "Tunnel localhost:$remoteport => $host:$port (bundle service)"
pid=$(ps ax | grep ssh.*:$port | grep -v grep | awk '{print $1}')
if [ -n "$pid" ]; then kill $pid; fi
ssh -N -n -L $remoteport:localhost:$port $host &

# Website
port=8000
remoteport=1$port
echo "Tunnel localhost:$remoteport => $host:$port (website)"
pid=$(ps ax | grep ssh.*:$port | grep -v grep | awk '{print $1}')
if [ -n "$pid" ]; then kill $pid; fi
ssh -N -n -L $remoteport:localhost:$port $host &
