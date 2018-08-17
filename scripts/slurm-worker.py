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
import os
import subprocess
import time

SERVER_INSTANCE = 'https://worksheets-dev.codalab.org'
SRUN_BINARY = 'srun'
CL_WORKER_BINARY = '/u/nlp/bin/cl-worker'
CL_BINARY = '/u/nlp/bin/cl'
DEFAULT_PASSWORD_FILE_LOCATION = '~/codalab.password'
WORKER_NAME_PREFIX = 'worker'
WORKER_DIR_LOCATION = '~'
SLEEP_INTERVAL = 30
FIELDS = ['uuid', 'request_cpus', 'request_gpus', 'request_memory', 'request_time', 'tags']


def parse_duration(dur):
    """
    s: <number>[<s|m|h|d|y>]
    Returns the number of minutes
    """
    try:
        if dur[-1].isdigit():
            return math.ceil(float(dur) / 60.0)
        n, unit = float(dur[0:-1]), dur[-1].lower()
        if unit == 's':
            return math.ceil(n / 60.0)
        if unit == 'm':
            return n
        if unit == 'h':
            return n * 60
        if unit == 'd':
            return n * 60 * 24
        if unit == 'y':
            return n * 60 * 24 * 365
    except (IndexError, ValueError):
        pass  # continue to next line and throw error
    raise ValueError('Invalid duration: %s, expected <number>[<s|m|h|d|y>]' % dur)


def write_worker_invocation(
    server=SERVER_INSTANCE,
    password=DEFAULT_PASSWORD_FILE_LOCATION,
    worker_name='worker',
    tag=None,
    num_cpus=1,
    num_gpus=0,
    verbose=False,
):
    """
    Return the name to a local file that has the commands that prepare the worker work dir,
    run the worker and clean up the work dir upon exit.
    """
    work_dir = os.path.join(WORKER_DIR_LOCATION, worker_name)
    prepare_command = 'mkdir -vp {0} && mkdir -vp {0}/runs;'.format(work_dir)
    cleanup_command = 'rm -rf {};'.format(work_dir)
    flags = [
        '--exit-when-idle',
        '--server {}'.format(server),
        '--password {}'.format(password),
        '--cpuset {}'.format(','.join(str(idx) for idx in range(num_cpus))),
        '--work-dir {}'.format(work_dir),
    ]

    if tag is not None:
        flags.append('--tag {}'.format(tag))
    if verbose:
        flags.append('--verbose')
    worker_command = '{} {} || true;'.format(CL_WORKER_BINARY, ' '.join(flags))
    with open('start-{}.sh'.format(worker_name), 'w') as script_file:
        script_file.write(prepare_command)
        script_file.write(worker_command)
        script_file.write(cleanup_command)
    return script_file.name


def start_worker_for(run_number, run_fields):
    """
    Start a worker suitable to run the given run with run_fields
    with the given run_number for the worker directory.
    This function makes the actual command call to start the job on Slurm.
    """
    current_directory = os.getcwd()
    worker_name = '{}-{}'.format(WORKER_NAME_PREFIX, run_number)
    tag = None
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
        '--chdir={}'.format(current_directory),
        '--nodes 1',
    ]

    srun_command = ' '.join(srun_flags)
    worker_command_script = write_worker_invocation(
        worker_name=worker_name,
        tag=tag,
        num_cpus=run_fields['request_cpus'],
        num_gpus=run_fields['request_gpus'],
        verbose=True,
    )
    final_command = '{} bash {}'.format(srun_command, worker_command_script)
    print('Starting worker for run {}'.format(run_fields['uuid']))
    try:
        subprocess.check_call(final_command, shell=True)
    except Exception as e:
        print('Anomaly in worker run: {}'.format(e))
    finally:
        try:
            os.remove(worker_command_script)
        except Exception as ex:
            print('Anomaly when trying to remove old worker script: {}'.format(ex))


def parse_field(field, val):
    """
    Parses cl's printed bundle info fields into typed constructs
    Currently converts request_time to minutes as that's what slurm
    takes in
    """
    if field == 'request_time':
        return parse_duration(val)
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
        try:
            # Make python3 compatible by decoding if we can
            run_lines = run_lines.decode('utf-8')
        except Exception:
            pass
        uuids = run_lines.splitlines()
        for uuid in uuids:
            if uuid not in cooldown_runs:
                info_cmd = '{} info {} -f {}'.format(CL_BINARY, uuid, ','.join(FIELDS))
                field_values = subprocess.check_output(info_cmd, shell=True)
                try:
                    # Make python3 compatible by decoding if we can
                    field_values = field_values.decode('utf-8')
                except Exception:
                    pass
                run = {
                    field: parse_field(field, val)
                    for field, val in zip(FIELDS, field_values.split())
                }
                start_worker_for(num_runs, run)
                cooldown_runs.add(uuid)
                num_runs += 1
            else:
                print(
                    "Previous worker for run {} hasn't been successful, uncaching it".format(uuid)
                )
                cooldown_runs.remove(uuid)

        time.sleep(SLEEP_INTERVAL)


if __name__ == '__main__':
    main()
