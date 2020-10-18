#!/usr/bin/env bash

while true
do
    # This IP address comes from:
    # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-interruptions.html#spot-instance-termination-notices
    # It's a special endpoint set up by AWS whereby AWS instances can view metadata about themselves.
    # One such piece of metadata is the termination time, which is only set when the spot instance is to be
    # pre-empted (you get a 404 otherwise).
    # This script was partially taken from https://stackoverflow.com/q/32613600/14089059 .
    if [ -z $(curl -Is http://169.254.169.254/latest/meta-data/spot/termination-time | head -1 | grep 404 | cut -d \  -f 2) ]
    then
        echo "EC2 spot instance scheduled for shutdown."
        echo "Sending SIGTERM to CodaLab workers"
        # Kill all cl-workers in the EC2 instance.
        pgrep -f "cl-worker" | xargs kill
    else
        # Instance not yet marked for termination, so sleep and check again in 5 seconds.
        sleep 5
    fi
done
