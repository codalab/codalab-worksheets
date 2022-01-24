# Worker Manager Setup

## AWS Worker Manager

### Configure AWS Batch (one-time setup)

1. Authenticate AWS on the command-line.
    1. Install the CLI: `pip install awscli`.
    1. Authenticate by running `aws configure` and fill out the form.
    1. Check that `config` and `credentials` at `~/.aws` is correctly populated.
1. Create a [launch template](https://docs.aws.amazon.com/batch/latest/userguide/launch-templates.html) 
   for EC2 instances by running:
   
   ```commandline
    aws ec2 --region <region> create-launch-template --cli-input-json file://lt.json
   ```
    
   Your launch template `lt.json` should look something like this:

   ```json
    {
        "LaunchTemplateName": "increase-root-volume",
        "LaunchTemplateData": {
            "BlockDeviceMappings": [
                {
                    "DeviceName": "/dev/xvda",
                    "Ebs": {
                        "Encrypted": true,
                        "VolumeSize": <Desired volume size in GB>,
                        "VolumeType": "gp2"
                    }
                }
            ]
        }
    }
   ```
   
1. Log on to the [AWS console](https://aws.amazon.com/console). 
1. In the upper right corner, select the region.
1. Type `Batch` in the search bar and click `Batch` under `Services`.
1. Create a compute environment:
    1. Click `Compute environments` and then `Create`.
    1. Specify a name for `Compute Environment Name`.
    1. Under `Instance Configuration`, select `On-Demand` or `Spot`.
    1. Specify the [type of EC2 instances](https://aws.amazon.com/ec2/instance-types/) under the 
       `Allowed Instance Types` dropdown menu.
    1. Under `Additional Settings`, select the launch template you created.
    1. Click `Create compute environment`.
1. Configure a job queue:
    1. Click `Job queues` and then `Create`.
    1. Give your job queue a name.
    1. Under `Connected compute environments`, select the compute environment from the previous step.
    1. Click `Create`.
1. Wait for the job queue and compute environment to have a status of `VALID`.

### Start a AWS Batch Worker Manager

Use the [AWS Batch Worker Manager](https://github.com/codalab/codalab-worksheets/blob/master/codalab/worker_manager/aws_batch_worker_manager.py),
to start the worker manager. Pass in the name of the job queue for `--job-queue`.
