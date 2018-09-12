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

import argparse
import math
import os
import subprocess
import time
import stat
import sys


FIELDS = ['uuid', 'request_cpus', 'request_gpus', 'request_memory', 'request_time', 'tags']


def parse_args():
    parser = argparse.ArgumentParser(
        description='Script to automatically start slurm jobs to run Codalab bundles in'
    )
    parser.add_argument(
        '--password-file',
        type=str,
        help='Read the Codalab username and password from this file. Each should be on its own line',
        default='~/codalab.password',
    )
    parser.add_argument(
        '--server-instance',
        type=str,
        help='Codalab server to run the workers for (default Stanford NLP cluster instance), use https://worksheets.codalab.org for the public Codalab instance',
        default='https://worksheets-dev.codalab.org',
    )
    parser.add_argument(
        '--srun-binary',
        type=str,
        help='Where the binary for the srun command lives (default is the location on the Stanford NLP cluster)',
        default='srun',
    )
    parser.add_argument(
        '--cl-worker-binary',
        type=str,
        help='Where the cl-worker binary lives (default is the location on the Stanford NLP cluster',
        default='u/nlp/bin/cl-worker',
    )
    parser.add_argument(
        '--cl-binary',
        type=str,
        help='Where the cl (Codalab CLI) binary lives (default is the location on the Stanford NLP cluster',
        default='u/nlp/bin/cl',
    )
    parser.add_argument(
        '--worker-parent-dir',
        type=str,
        help='Where the temporary working directories for workers should be created (default home directory of user)',
        default='~',
    )
    parser.add_argument(
        '--worker-dir-prefix',
        type=str,
        help='Prefix to use for temporary worker directories (they are named \{prefix\}-\{worker number\})',
        default='worker',
    )
    parser.add_argument(
        '--sleep-interval',
        type=int,
        help='Number of seconds to wait between each time the server is polled for new runs (default 30)',
        default=30,
    )
    return parser.parse_args()


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


def get_user_info(password_file):
    if password_file:
        if os.stat(password_file).st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            print >>sys.stderr, """
Permissions on password file are too lax.
Only the user should be allowed to access the file.
On Linux, run:
chmod 600 %s""" % password_file
            exit(1)
        with open(password_file) as f:
            username = f.readline().strip()
            password = f.readline().strip()
    else:
        username = os.environ.get('CODALAB_USERNAME')
        password = os.environ.get('CODALAB_PASSWORD')
    return username, password


def login(args):
    username, password = get_user_info(args.password_file)
    if username and password:
        proc = subprocess.Popen(
            '{} work {}::'.format(args.cl_binary, args.server_instance),
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        proc.stdin.write('{}\n'.format(username))
        proc.stdin.flush()
        proc.stdin.write('{}\n'.format(password))
        proc.stdin.flush()
    subprocess.check_call('{} work {}::'.format(args.cl_binary, args.server_instance), shell=True)
    print('Logged in to {}'.format(args.server_instance))


def write_worker_invocation(
    worker_binary,
    server,
    password_file,
    worker_parent_dir,
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
    work_dir = os.path.join(worker_parent_dir, worker_name)
    prepare_command = 'mkdir -vp {0} && mkdir -vp {0}/runs;'.format(work_dir)
    cleanup_command = 'rm -rf {};'.format(work_dir)
    flags = [
        '--exit-when-idle',
        '--server {}'.format(server),
        '--password {}'.format(password_file),
        '--cpuset {}'.format(','.join(str(idx) for idx in range(num_cpus))),
        '--work-dir {}'.format(work_dir),
    ]

    if tag is not None:
        flags.append('--tag {}'.format(tag))
    if verbose:
        flags.append('--verbose')
    worker_command = '{} {};'.format(worker_binary, ' '.join(flags))
    with open('start-{}.sh'.format(worker_name), 'w') as script_file:
        script_file.write(prepare_command)
        script_file.write(worker_command)
        script_file.write(cleanup_command)
    return script_file.name


def start_worker_for(args, run_number, run_fields):
    """
    Start a worker suitable to run the given run with run_fields
    with the given run_number for the worker directory.
    This function makes the actual command call to start the job on Slurm.
    """
    current_directory = os.getcwd()
    worker_name = '{}-{}'.format(args.worker_dir_prefix, run_number)
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
        args.srun_binary,
        '--partition={}'.format(partition),
        '--mem={}'.format(run_fields['request_memory']),
        '--gres=gpu:{}'.format(run_fields['request_gpus']),
        '--chdir={}'.format(current_directory),
        '--nodes 1',
    ]

    if run_fields['request_cpus'] > 1:
        srun_flags.append('--cpus-per-task={}'.format(run_fields['request_cpus']))

    srun_command = ' '.join(srun_flags)
    worker_command_script = write_worker_invocation(
        args.cl_worker_binary,
        args.server_instance,
        args.password_file,
        args.worker_parent_dir,
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
    args = parse_args()

    login(args.password_file)
    # Infinite loop until user kills us
    num_runs = 0
    # Cache runs that we started workers for for one extra iteration in case they're still staged
    # during the next iteration as worker booting might take some time. Un-cache them after one
    # iteration to start booting new workers for them
    cooldown_runs = set()
    while True:
        run_lines = subprocess.check_output(
            '{} search .mine state=staged -u'.format(args.cl_binary), shell=True
        )
        try:
            # Make python3 compatible by decoding if we can
            run_lines = run_lines.decode('utf-8')
        except Exception:
            pass
        uuids = run_lines.splitlines()
        for uuid in uuids:
            if uuid not in cooldown_runs:
                info_cmd = '{} info {} -f {}'.format(args.cl_binary, uuid, ','.join(FIELDS))
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
                start_worker_for(args, num_runs, run)
                cooldown_runs.add(uuid)
                num_runs += 1
            else:
                print(
                    "Previous worker for run {} hasn't been successful, uncaching it".format(uuid)
                )
                cooldown_runs.remove(uuid)

        time.sleep(args.sleep_interval)


if __name__ == '__main__':
    main()
