import math
import subprocess
import time

from codalab.lib.formatting import parse_duration

SERVER_INSTANCE = 'https://worksheets-dev.codalab.org'
BASE_NLPRUN_INVOCATION = '/u/nlp/bin/nlprun'
DEFAULT_PASSWORD_FILE_LOCATION = '~/codalab.password'
BASE_WORKER_INVOCATION = 'cl-worker --verbose --server {} --password {}'.format(SERVER_INSTANCE, DEFAULT_PASSWORD_FILE_LOCATION)
SLEEP_INTERVAL = 30
FIELDS = ['uuid', 'request_cpus', 'request_gpus', 'request_memory', 'request_time', 'tag']


def parse_field(field, val):
    if field == 'request_time':
        duration_in_secs = parse_duration(val)
        return str(int(math.ceil(duration_in_secs)) * 60)
    else:
        return val


def main():
    # Log in with the server
    subprocess.check_call('cl work {}::'.format(SERVER_INSTANCE), shell=True)
    print('Logged in to {}'.format(SERVER_INSTANCE))

    # Infinite loop until user kills us
    num_runs = 0
    while True:
        run_lines = subprocess.check_output('cl search .mine state=staged', shell=True)
        run_lines = run_lines.splitlines()[2:]
        runs = []
        for run in run_lines:
            uuid = run.split()[0]
            info_cmd = 'cl info {} -f {}'.format(uuid, ','.join(FIELDS))
            field_values = subprocess.check_output(info_cmd, shell=True)
            run_info = {
                field: parse_field(field, val) for field, val in zip(FIELDS, field_values.split())
            }
            runs.append(run_info)
        if runs:
            cpu_runs = [run for run in runs if run['request_gpus'] == '0']
            jag_hi_runs = [
                run for run in runs if run['request_gpus'] != '0' and 'jag-hi' in run['tags']
            ]
            jag_lo_runs = [
                run for run in runs if run['request_gpus'] != '0' and 'jag-hi' not in run['tags']
            ]
            for run in cpu_runs:
                run_command = '{0} -q john -r {request_memory} -c {request_cpus}'.format(
                    BASE_NLPRUN_INVOCATION, **run
                )
                worker_home = '~/worker-{}'.format(num_runs)
                num_runs += 1
                prepare_command = 'mkdir {}'.format(worker_home)
                cleanup_command = 'rm -rf {}'.format(worker_home)
                worker_invocation = '{} --work-dir {}'.format(BASE_WORKER_INVOCATION, worker_home)
                final_command = '{} \'{}; {}; {}\''.format(run_command, prepare_command, worker_invocation, cleanup_command)
                print(final_command)
                subprocess.check_call(final_command, shell=True)
                print('Started worker for run {uuid}'.format(**run))
            for run in jag_hi_runs:
                run_command = '{0} -q jag -p high -r {request_memory} -c {request_cpus} -g {request_gpus}'.format(
                    BASE_NLPRUN_INVOCATION, **run
                )
                worker_home = '~/worker-{}'.format(num_runs)
                num_runs += 1
                worker_invocation = '{} --work-dir {} --tag jag-hi'.format(BASE_WORKER_INVOCATION, worker_home)
                prepare_command = 'mkdir {}'.format(worker_home)
                cleanup_command = 'rm -rf {}'.format(worker_home)
                final_command = '{} \'{}; {}; {}\''.format(run_command, prepare_command, worker_invocation, cleanup_command)
                print(final_command)
                subprocess.check_call(final_command, shell=True)
                print('Started worker for run {uuid}'.format(**run))
            for run in jag_lo_runs:
                run_command = '{0} -q jag -p low -r {request_memory} -c {request_cpus} -g {request_gpus}'.format(
                    BASE_NLPRUN_INVOCATION, **run
                )
                worker_home = '~/worker-{}'.format(num_runs)
                num_runs += 1
                worker_invocation = '{} --work-dir {}'.format(BASE_WORKER_INVOCATION, worker_home)
                prepare_command = 'mkdir {}'.format(worker_home)
                cleanup_command = 'rm -rf {}'.format(worker_home)
                final_command = '{} \'{}; {}; {}\''.format(run_command, prepare_command, worker_invocation, cleanup_command)
                print(final_command)
                subprocess.check_call(final_command, shell=True)
                print('Started worker for run {uuid}'.format(**run))

        time.sleep(SLEEP_INTERVAL)


if __name__ == '__main__':
    main()
