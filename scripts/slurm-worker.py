import math
import subprocess
import time

from codalab.lib.formatting import parse_duration

SERVER_INSTANCE = 'https://worksheets-dev.codalab.org'
NLPRUN_BINARY = '/u/nlp/bin/nlprun'
CL_WORKER_BINARY = 'u/nlp/bin/cl-worker'
DEFAULT_PASSWORD_FILE_LOCATION = '~/codalab.password'
SLEEP_INTERVAL = 30
FIELDS = ['uuid', 'request_cpus', 'request_gpus', 'request_memory', 'request_time', 'tag']


def get_worker_invocation(
    server=SERVER_INSTANCE,
    password=DEFAULT_PASSWORD_FILE_LOCATION,
    work_dir=None,
    tag=None,
    verbose=False,
):
    flags = []
    flags.append('--server {}'.format(server))
    flags.append('--password {}'.format(password))
    if work_dir is not None:
        flags.append('--work-dir {}'.format(work_dir))
    if tag is not None:
        flags.append('--tag {}'.format(tag))
    if verbose:
        flags.append('--verbose')
    return '{} {}'.format(CL_WORKER_BINARY, ' '.join(flags))


def start_worker_for(run_number, run_fields, nlprun_binary=NLPRUN_BINARY):
    if 'jag_hi' in run_fields['tags']:
        queue = 'jag'
        priority = 'high'
        tag = 'jag-hi'
    elif run_fields['request_gpus']:
        queue = 'jag'
        priority = 'low'
    else:
        queue = 'john'
        priority = 'low'

    nlprun_flags = [
        nlprun_binary,
        '-q {}'.format(queue),
        '-c {}'.format(run_fields['num_cpus']),
        '-r {}'.format(run_fields['request_memory']),
        '-g {}'.format(run_fields['request_gpus']),
        '-p {}'.format(priority),
    ]

    run_command = ' '.join(nlprun_flags)
    worker_home = '~/worker-{}'.format(run_number)
    prepare_command = 'mkdir {}'.format(worker_home)
    cleanup_command = 'rm -rf {}'.format(worker_home)
    worker_invocation = get_worker_invocation(work_dir=worker_home, tag=tag, verbose=True)
    final_command = '{} \'{}; {}; {}\''.format(
        run_command, prepare_command, worker_invocation, cleanup_command
    )
    print(final_command)
    subprocess.check_call(final_command, shell=True)
    print('Started worker for run {}'.format(run_fields['uuid']))


def parse_field(field, val):
    if field == 'request_time':
        duration_in_secs = parse_duration(val)
        return int(math.ceil(duration_in_secs)) * 60
    elif field in ['request_cpus', 'request_gpus']:
        return int(val)
    else:
        return val


def main():
    # Log in with the server
    subprocess.check_call('cl work {}::'.format(SERVER_INSTANCE), shell=True)
    print('Logged in to {}'.format(SERVER_INSTANCE))

    # Infinite loop until user kills us
    num_runs = 0
    while True:
        run_lines = subprocess.check_output('cl search .mine state=staged -u', shell=True)
        uuids = run_lines.splitlines()
        runs = []
        for uuid in uuids:
            info_cmd = 'cl info {} -f {}'.format(uuid, ','.join(FIELDS))
            field_values = subprocess.check_output(info_cmd, shell=True)
            run_info = {
                field: parse_field(field, val) for field, val in zip(FIELDS, field_values.split())
            }
            runs.append(run_info)
        if runs:
            for run in runs:
                start_worker_for(num_runs, run)
                num_runs += 1

        time.sleep(SLEEP_INTERVAL)


if __name__ == '__main__':
    main()
