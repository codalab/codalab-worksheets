"""
Script that's designed for individual users of a Slurm managed cluster to run on a machine
with srun access. Once the user starts the script and logs in, the script busy loops, querying
the given CodaLab instance for staged bundles belonging to the user. Every time there's a staged
bundle, its CodaLab resource requests (gpu, cpu, memory etc) are converted to srun options and a
new worker with that many resources is fired up on slurm. These workers die when they're idle.

Some values are Stanford NLP cluster specific (CodaLab instance used, worker binary locations),
but can be configured for other Slurm clusters. The script just requires codalab-worker to be installed
on all slurm machines on a consitent location.
"""

import math
import subprocess
import time

from codalab.lib.formatting import parse_duration

SERVER_INSTANCE = 'https://worksheets-dev.codalab.org'
SRUN_BINARY = 'srun'
CL_WORKER_BINARY = '/u/nlp/bin/cl-worker'
CL_BINARY = '/u/nlp/bin/cl'
DEFAULT_PASSWORD_FILE_LOCATION = '~/codalab.password'
WORKER_PREFIX = '~/worker-'
SLEEP_INTERVAL = 30
FIELDS = ['uuid', 'request_cpus', 'request_gpus', 'request_memory', 'request_time', 'tag']


def get_worker_invocation(
    server=SERVER_INSTANCE,
    password=DEFAULT_PASSWORD_FILE_LOCATION,
    work_dir=None,
    tag=None,
    num_cpus=1,
    num_gpus=0,
    verbose=False,
):
    """
    Return a command string that would invoke a one-run worker for the
    given parameters
    """
    flags = [
        '--exit-when-idle',
        '--server {}'.format(server),
        '--password {}'.format(password),
        '--cpuset {}'.format(','.join(range(num_cpus))),
        '--gpuset {}'.format(','.join(range(num_gpus))),
    ]

    if work_dir is not None:
        flags.append('--work-dir {}'.format(work_dir))
    if tag is not None:
        flags.append('--tag {}'.format(tag))
    if verbose:
        flags.append('--verbose')
    return '{} {}'.format(CL_WORKER_BINARY, ' '.join(flags))


def start_worker_for(run_number, run_fields):
    """
    Start a worker suitable to run the given run with run_fields
    with the given run_number for the worker directory.
    This function makes the actual command call to start the
    job on Slurm
    """
    if run_fields['request_gpus']:
        if 'jag_hi' in run_fields['tags']:
            partition = 'jag-hi'
            tag = 'jag-hi'
        else:
            partition = 'jag-lo'
    else:
        partition = 'john'

    srun_flags = [
        SRUN_BINARY,
        '--partition={}'.format(partition),
        '--cpus-per-task={}'.format(run_fields['request_cpus']),
        '--mem={}'.format(run_fields['request_memory']),
        '--gres=gpu:{}'.format(run_fields['request_gpus']),
    ]

    srun_command = ' '.join(srun_flags)
    worker_home = '{}{}'.format(WORKER_PREFIX, run_number)
    prepare_command = 'mkdir {}'.format(worker_home)
    cleanup_command = 'rm -rf {}'.format(worker_home)
    worker_invocation = get_worker_invocation(
        work_dir=worker_home,
        tag=tag,
        num_cpus=run_fields['num_cpus'],
        num_gpus=run_fields['num_gpus'],
        verbose=True,
    )
    final_command = '{} \'{}; {}; {}\''.format(
        srun_command, prepare_command, worker_invocation, cleanup_command
    )
    print(final_command)
    subprocess.check_call(final_command, shell=True)
    print('Started worker for run {}'.format(run_fields['uuid']))


def parse_field(field, val):
    """
    Parses cl's printed bundle info fields into typed constructs
    Currently converts request_time to minutes as that's what slurm
    takes in
    """
    if field == 'request_time':
        duration_in_secs = parse_duration(val)
        return int(math.ceil(duration_in_secs)) * 60
    elif field in ['request_cpus', 'request_gpus']:
        return int(val)
    else:
        return val


def main():
    # Log in with the server
    subprocess.check_call('{} work {}::'.format(CL_BINARY, SERVER_INSTANCE), shell=True)
    print('Logged in to {}'.format(SERVER_INSTANCE))

    # Infinite loop until user kills us
    num_runs = 0
    # Cache runs that we started workers for for one extra iteration in case they're still staged
    # during the next iteration as worker booting might take some time. Un-cache them after one
    # iteration to start booting new workers for them
    cooldown_runs = set()
    while True:
        run_lines = subprocess.check_output(
            '{} search .mine state=staged -u'.format(CL_BINARY), shell=True
        )
        uuids = run_lines.splitlines()
        for uuid in uuids:
            if uuid not in cooldown_runs:
                info_cmd = '{} info {} -f {}'.format(CL_BINARY, uuid, ','.join(FIELDS))
                field_values = subprocess.check_output(info_cmd, shell=True)
                run = {
                    field: parse_field(field, val)
                    for field, val in zip(FIELDS, field_values.split())
                }
                start_worker_for(num_runs, run)
                cooldown_runs.append(uuid)
                num_runs += 1
            else:
                print(
                    "Previous worker for run {} hasn't been successful, uncaching it".format(uuid)
                )
                cooldown_runs.remove(uuid)

        time.sleep(SLEEP_INTERVAL)


if __name__ == '__main__':
    main()
