#!/usr/bin/env python3

from collections import defaultdict
from email.mime.text import MIMEText
from smtplib import SMTP
from typing import Dict

import argparse
import datetime
import os
import subprocess
import sys
import time

BASE_DIR = os.path.dirname(__file__)

# This script runs in a loop monitoring the health of the CodaLab instance.
# It reads config.json in your CODALAB_HOME (~/.codalab).
# Here are some of the things the script does:
# - Make sure we don't run out of disk space.
# - Backup the database.
# - Make sure runs finish in a reasonable amount of time.
# - Email if anything goes wrong (but bound the number of emails as not to spam).
# - Email a daily report.

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument(
    '--codalab-home',
    help='where the CodaLab instance lives',
    default=os.getenv('CODALAB_HOME', os.path.join(os.getenv('HOME', default="..."), '.codalab')),
)

# Where to write out information
parser.add_argument('--log-path', help='file to write the log', default='monitor.log')
parser.add_argument('--backup-path', help='directory to backup database', default='monitor.backup')

# How often to do things
parser.add_argument(
    '--ping-interval', help='ping the server every this many seconds', type=int, default=30
)
parser.add_argument(
    '--run-interval', help='run a job every this many seconds', type=int, default=5 * 60
)
parser.add_argument(
    '--email-interval',
    help='email a report every this many seconds',
    type=int,
    default=24 * 60 * 60,
)

args = parser.parse_args()

# Get MySQL username and password for bundles
bundles_host = os.environ['CODALAB_MYSQL_HOST']
bundles_port = os.environ['CODALAB_MYSQL_PORT']
bundles_database = os.environ['CODALAB_MYSQL_DATABASE']
bundles_username = os.environ['CODALAB_MYSQL_USERNAME']
bundles_password = os.environ['CODALAB_MYSQL_PASSWORD']
print(
    'user = {}, password = {}, db = {}, host = {}, port = {}'.format(
        bundles_username, '*' * len(bundles_password), bundles_database, bundles_host, bundles_port
    )
)

hostname = os.environ['HOSTNAME']

# Email
admin_email = os.environ['CODALAB_ADMIN_EMAIL']
sender_host = os.environ['CODALAB_EMAIL_HOST']
sender_username = os.environ['CODALAB_EMAIL_USERNAME']
sender_password = os.environ['CODALAB_EMAIL_PASSWORD']

# Create backup directory
if not os.path.exists(args.backup_path):
    os.mkdir(args.backup_path)

report = []  # Build up the current report to send in an email


def get_public_workers():
    # Comma-separated list of worker ids to monitor. Example: vm-clws-prod-worker-0,vm-clws-prod-worker-1
    return set(
        [
            worker.strip()
            for worker in os.environ['CODALAB_PUBLIC_WORKERS'].split(',')
            if worker.rstrip()
        ]
    )


# message is a list
def send_email(subject, message):
    log(
        'Sending an email to %s from %s@%s; subject: %s; message contains %d lines'
        % (admin_email, sender_username, sender_host, subject, len(message))
    )

    if not admin_email:
        return

    # Default to authless SMTP (supported by some servers) if user/password is unspecified.
    #   Default sender_username has to be a valid RFC 822 from-address string for transport (distinct from msg headers)
    #   Ref: https://docs.python.org/2/library/smtplib.html#smtplib.SMTP.sendmail
    do_login = sender_password is not None
    s = SMTP(sender_host, 587)
    s.ehlo()
    s.starttls()
    s.ehlo()
    msg = MIMEText('<pre style="font: monospace">' + '\n'.join(message) + '</pre>', 'html')
    msg['Subject'] = 'CodaLab on %s: %s' % (hostname, subject)
    msg['To'] = admin_email
    msg['From'] = 'noreply@codalab.org'
    if do_login:
        s.login(sender_username, sender_password)
    s.sendmail(sender_username, admin_email, msg.as_string())
    s.quit()


def log(line):
    current_datetime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    line = '[%s] %s' % (current_datetime, line)
    print(line)
    sys.stdout.flush()
    report.append(line)
    out = open(args.log_path, 'a')
    print(line, file=out)
    out.close()


def logs(s):
    for line in s.split('\n'):
        log(line)


num_errors: Dict[str, int] = defaultdict(int)
last_sent: Dict[str, float] = defaultdict(int)


def error_logs(error_type, s):
    logs(s)

    num_errors[error_type] += 1
    n = num_errors[error_type]

    last_t = last_sent[error_type]
    t = time.time()

    # Send email only every 4 hours
    if t > last_t + 60 * 60 * 4:
        send_email('%s [%d times]' % (error_type, n), s.split('\n'))
        last_sent[error_type] = t


args_to_durations: Dict[str, list] = defaultdict(list)  # Command => durations for that command


def run_command(args, soft_time_limit=15, hard_time_limit=60, include_output=True):
    # We cap the running time to hard_time_limit, but print out an error if we exceed soft_time_limit.
    start_time = time.time()
    args = ['timeout', '%ss' % hard_time_limit] + args
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
    output, err_output = proc.communicate()
    exitcode = proc.returncode
    end_time = time.time()

    # Add to the list
    duration = end_time - start_time
    durations = args_to_durations[str(args)]
    durations.append(duration)
    while len(durations) > 1000:  # Keep the list bounded
        durations.pop(0)
    average_duration = sum(durations) // len(durations)
    max_duration = max(durations)

    # Abstract away the concrete uuids
    simple_args = ['0x*' if arg.startswith('0x') else arg for arg in args]

    # Include stderr in output if returncode != 0
    full_output = output
    if exitcode != 0:
        full_output += err_output

    message = '>> %s (exit code %s, time %.2fs [limit: %ds,%ds]; avg %.2fs; max %.2fs)\n%s' % (
        ' '.join(args),
        exitcode,
        duration,
        soft_time_limit,
        hard_time_limit,
        average_duration,
        max_duration,
        full_output if include_output else '',
    )
    if exitcode == 0:
        logs(message)
    else:
        error_logs('command failed: ' + ' '.join(simple_args), message)

    if duration > soft_time_limit:
        error_logs('command too slow: ' + ' '.join(simple_args), message)

    return output.rstrip()


timer = 0


def ping_time():
    global timer
    return timer % args.ping_interval == 0


def run_time():
    global timer
    return timer % args.run_interval == 0


def email_time():
    global timer
    return timer % args.email_interval == 0


def backup_db():
    log('Backup DB (note that errors are not detected due to shell pipes)')
    mysql_conf_path = os.path.join(args.codalab_home, 'monitor-mysql.cnf')
    with open(mysql_conf_path, 'w') as f:
        print('[client]', file=f)
        print('host="%s"' % bundles_host, file=f)
        print('port="%s"' % bundles_port, file=f)
        print('user="%s"' % bundles_username, file=f)
        print('password="%s"' % bundles_password, file=f)

    # Only save a backup for every month to save space
    month = datetime.datetime.utcnow().strftime('%Y-%m')
    path = '%s/%s-%s.mysqldump.gz' % (args.backup_path, bundles_database, month)
    run_command(
        [
            'bash',
            '-c',
            'mysqldump --defaults-file=%s --single-transaction --quick %s | gzip > %s'
            % (mysql_conf_path, bundles_database, path),
        ],
        600,
        600,
    )  # Backup might take a while.
    os.unlink(mysql_conf_path)
    size = os.path.getsize(path)
    log('Size of backup {} is {}'.format(path, size))
    if size < 100:
        log('Size is suspiciously small!')


def check_disk_space(paths):
    """Checks whether there is low disk space in the given paths, and
    prints out a report of "df -h" run on the given paths."""

    # Run "df -h" first, so that human-readable output is logged in the reports.
    # We run "df" afterwards (and don't log the output) so that we can numerically
    # check whether there is low disk space.
    run_command(['df', '-h'] + paths)
    lines = run_command(['df'] + paths, include_output=False).split('\n')[1:]
    results = [int(line.split()[3]) for line in lines]
    # Flag an error if disk space running low
    total = sum(results)
    if total < 1000 * 1024:
        error_logs(
            'low disk space',
            'Only %s MB of disk space left on %s!' % (total // 1024, ' '.join(paths)),
        )


def poll_online_workers():
    public_workers = get_public_workers()
    if len(public_workers) == 0:
        log("Environment variable CODALAB_PUBLIC_WORKERS is empty.")
        return
    lines = run_command(['cl', 'workers']).split('\n')
    workers_info = lines[2:]
    online_workers = set()
    for line in workers_info:
        online_workers.add(line.split()[0].strip())

    workers_intersection = public_workers.intersection(online_workers)
    offline_public_workers = public_workers - workers_intersection
    if len(offline_public_workers) > 0:
        error_logs(
            'worker offline',
            'The following public workers are offline:\n{}.'.format(
                '\n'.join(offline_public_workers)
            ),
        )


# Make sure we can connect (might prompt for username/password)
if subprocess.call(['cl', 'work']) != 0:
    sys.exit(1)

# Begin monitoring loop
while True:
    del report[:]
    if ping_time():
        log('=== BEGIN REPORT')

    try:
        # Backup DB
        if email_time():
            backup_db()

        # Check remaining disk space
        if ping_time():
            check_disk_space(['/'])  # Always bad if root partition is low
            check_disk_space(['/var/lib/docker'])  # Docker images
            base_path = os.path.join(args.codalab_home, 'partitions')
            paths = [os.path.join(base_path, fname) for fname in os.listdir(base_path)]
            check_disk_space(paths)

        # Get statistics on bundles
        if ping_time():
            # Simple things
            run_command(['cl', 'workers'])
            run_command(['cl', 'work'])
            run_command(['cl', 'search', '.count'])

        # Get online workers and contact administrators when there are public workers offline.
        if ping_time():
            poll_online_workers()

        if run_time():
            # More intense
            run_command(['cl', 'search', 'size=.sum'], 20)
            run_command(['cl', 'search', 'size=.sort-', '.limit=5'], 20)
            run_command(['cl', 'search', '.last', '.limit=5'])

        # Try uploading, downloading and running a job with a dependency.
        if run_time() and get_public_workers():
            upload_uuid = run_command(
                ['cl', 'upload', os.path.join(BASE_DIR, 'scripts', 'stress-test.pl')]
            )
            cat_result = run_command(['cl', 'cat', upload_uuid], include_output=False)
            if 'BYTES_IN_MB' not in cat_result:
                error_logs(
                    'download failed',
                    'Uploaded file should contain the string BYTES_IN_MB, contents:\n' + cat_result,
                )
            uuid = run_command(
                ['cl', 'run', 'stress-test.pl:' + upload_uuid, 'perl stress-test.pl 5 10 10']
            )
            run_command(
                ['cl', 'wait', uuid], 600, 3600
            )  # Running might take a while (includes staged time)
            run_command(['cl', 'rm', upload_uuid, uuid])

    except Exception as e:
        error_logs('exception', 'Exception: %s' % e)

    if ping_time():
        log('=== END REPORT')

    # Email the report
    if email_time():
        send_email('report', report)

    if ping_time():
        print()

    # Update timer
    time.sleep(1)
    timer += 1
