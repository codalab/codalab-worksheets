#!/usr/bin/env bash

# This script was partially taken from:
    # https://github.com/AmazonWebServices-Projects/ec2-spot-labs/blob/master/ec2-spot-interruption-handler/wait_x_seconds_before_interruption.sh

# This IP address comes from:
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-interruptions.html#spot-instance-termination-notices
# It's a special endpoint set up by AWS whereby AWS instances can view metadata about themselves.
# One such piece of metadata is the termination action + time, which is only set when the spot instance is to be
# pre-empted (you get a 404 otherwise).
while [ -n "$(curl -s http://169.254.169.254/latest/meta-data/spot/instance-action | grep 404)" ];
do
   sleep 5
done


echo "EC2 spot instance scheduled for shutdown."
echo "Sending SIGTERM to CodaLab workers"
# Kill all cl-workers in the EC2 instance.
pgrep -f "cl-worker" | xargs kill
echo "Sent SIGTERM to CodaLab workers"
