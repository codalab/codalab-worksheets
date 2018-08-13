import math
import subprocess
import time

from codalab.lib.formatting import parse_duration

SERVER_INSTANCE = 'https://worksheets-dev.codalab.org'
BASE_NLPRUN_INVOCATION = '/u/nlp/bin/nlprun'
BASE_WORKER_INVOCATION = 'cl-worker --server {}'.format(SERVER_INSTANCE)
SLEEP_INTERVAL = 10
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
                worker_invocation = '{}'.format(BASE_WORKER_INVOCATION)
                final_command = '{} \'{}\''.format(run_command, worker_invocation)
                print(final_command)
                subprocess.check_call(final_command, shell=True)
                print('Started worker for run {uuid}'.format(run))
            for run in jag_hi_runs:
                run_command = '{0} -q jag -p high -r {request_memory} -c {request_cpus} -g {request_gpus}'.format(
                    BASE_NLPRUN_INVOCATION, **run
                )
                worker_invocation = '{} --tag jag-hi'.format(BASE_WORKER_INVOCATION)
                final_command = '{} \'{}\''.format(run_command, worker_invocation)
                print(final_command)
                subprocess.check_call(final_command, shell=True)
                print('Started worker for run {uuid}'.format(run))
            for run in jag_lo_runs:
                run_command = '{0} -q jag -p low -r {request_memory} -c {request_cpus} -g {request_gpus}'.format(
                    BASE_NLPRUN_INVOCATION, **run
                )
                worker_invocation = '{}'.format(BASE_WORKER_INVOCATION)
                final_command = '{} \'{}\''.format(run_command, worker_invocation)
                print(final_command)
                subprocess.check_call(final_command, shell=True)
                print('Started worker for run {uuid}'.format(run))

        time.sleep(SLEEP_INTERVAL)


if __name__ == '__main__':
    main()
