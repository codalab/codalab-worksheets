#!/usr/bin/env python

import os, sys
import datetime
from collections import defaultdict
from smtplib import SMTP
from email.mime.text import MIMEText
import subprocess
import re
import time
import socket
import argparse
import json

from multiprocessing.dummy import Pool as ThreadPool

CODALAB_CLI = os.path.dirname(__file__)

# This script runs in a loop monitoring the health of the CodaLab instance.
# It reads config.json and website-config.json in your CodaLab Home (~/.codalab).
# Here are some of the things the script does:
# - Make sure we don't run out of disk space.
# - Backup the database.
# - Make sure runs finish in a reasonable amount of time.
# - Email if anything goes wrong.
# - Email a daily report.

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--codalab-home', help='where the CodaLab instance lives',
    default=os.getenv('CODALAB_HOME', os.path.join(os.getenv('HOME'), '.codalab')))

# Where to write out information
parser.add_argument('--log-path', help='file to write the log', default='stress-test.log')

args = parser.parse_args()

hostname = socket.gethostname()

# Get MySQL username and password for bundles
config_path = os.path.join(args.codalab_home, 'config.json')
config = json.loads(open(config_path).read())
engine_url = config['server']['engine_url']
m = re.match('mysql://(.+):(.+)@localhost(?::3306)?/(.+)', engine_url)
if not m:
    print 'Can\'t extract server.engine_url from %s' % config_path
    sys.exit(1)
bundles_db = m.group(3)
bundles_user = m.group(1)
bundles_password = m.group(2)
print 'bundles DB: %s; user: %s' % (bundles_db, bundles_user)

# Email
#recipient = config['server'].get('admin_email')
recipient = ''
sender_info = config.get('email')

report = []  # Build up the current report to send in an email

# message is a list
def send_email(subject, message):
    # Not enough information to send email
    if not recipient or not sender_info:
        print 'send_email; subject: %s; message contains %d lines' % (subject, len(message))
        return

    sender_host = sender_info['host']
    sender_user = sender_info['user']
    sender_password = sender_info['password']
    print 'send_email to %s from %s@%s; subject: %s; message contains %d lines' % \
        (recipient, sender_user, sender_host, subject, len(message))
    s = SMTP(sender_host, 587)
    s.ehlo()
    s.starttls()
    s.ehlo()
    msg = MIMEText('<pre style="font: monospace">' + '\n'.join(message) + '</pre>', 'html')
    msg['Subject'] = 'CodaLab on %s: %s' % (hostname, subject)
    msg['To'] = recipient
    msg['From'] = 'noreply@codalab.org'
    s.login(sender_user, sender_password)
    s.sendmail(sender_user, recipient, msg.as_string())
    s.quit()

def get_date():
    # Only save a backup for every month
    return datetime.datetime.now().strftime('%Y-%m')
    #return datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')

def log(line, newline=True):
    line = '[%s] %s' % (get_date(), line)
    if newline:
        print line
    else:
        print line,
    report.append(line)
    out = open(args.log_path, 'a')
    print >>out, line
    out.close()

def logs(s):
    for line in s.split('\n'):
        log(line)

num_errors = defaultdict(int)
last_sent = defaultdict(int)
def error_logs(error_type, s):
    logs(s)

    num_errors[error_type] += 1
    n = num_errors[error_type]

    last_t = last_sent[error_type]
    t = time.time()

    # Send email only every 4 hours
    if t > last_t + 60*60*4:
        send_email('%s [%d times]' % (error_type, n), s.split('\n'))
        last_sent[error_type] = t

durations = defaultdict(list)  # Command => durations for that command
def run_command(args, soft_time_limit=15, hard_time_limit=60, include_output=True):
    # We cap the running time to hard_time_limit, but print out an error if we exceed soft_time_limit.
    start_time = time.time()
    try:
        args = ['timeout', '%ss' % hard_time_limit] + args
        output = subprocess.check_output(args)
        exitcode = 0
    except subprocess.CalledProcessError, e:
        output = e.output
        exitcode = e.returncode
    end_time = time.time()

    # Add to the list
    duration = end_time - start_time
    l = durations[str(args)]
    l.append(duration)
    while len(l) > 1000:  # Keep the list bounded
        l.pop(0)
    average_duration = sum(l) / len(l)
    max_duration = max(l)

    # Abstract away the concrete uuids
    simple_args = ['0x*' if arg.startswith('0x') else arg for arg in args]

    message = '>> %s (exit code %s, time %.2fs [limit: %ds,%ds]; avg %.2fs; max %.2fs)\n%s' % \
        (' '.join(args), exitcode, duration, soft_time_limit, hard_time_limit,
        average_duration, max_duration, output if include_output else '')
    if exitcode == 0:
        logs(message)
    else:
        error_logs('command failed: ' + ' '.join(simple_args), message)

    if duration > soft_time_limit:
        error_logs('command too slow: ' + ' '.join(simple_args), message)

    return output.rstrip()


def check_disk_space(path):
    result = int(run_command(['df', path]).split('\n')[1].split()[3])
    # Flag an error if disk space running low
    if result < 1000 * 1024:
        error_logs('low disk space', 'Only %s MB of disk space left!' % (result / 1024))

# Make sure we can connect (might prompt for username/password)
if subprocess.call(['cl', 'work', 'localhost::']) != 0:
    sys.exit(1)

# Begin stress test (no loop)
def stress_test(name):
    del report[:]
    log('=== BEGIN REPORT')

    try:
        for i in range(20):
            # Get statistics on bundles
            # Simple things
            run_command(['cl', 'work', 'localhost::'])
            run_command(['cl', 'search', '.count'])
            # More intense
            run_command(['cl', 'search', 'size=.sum'], 20)
            run_command(['cl', 'search', 'size=.sort-', '.limit=5'], 20)
            run_command(['cl', 'search', '.last', '.limit=5'])

        for i in range(10):
            # Try uploading, downloading and running a job with a dependency.
            upload_uuid = run_command(['cl', 'upload', os.path.join(CODALAB_CLI, 'scripts', 'stress-test.pl')])
            cat_result = run_command(['cl', 'cat', upload_uuid], include_output=False)
            if 'BYTES_IN_MB' not in cat_result:
                error_logs('download failed', 'Uploaded file should contain the string BYTES_IN_MB, contents:\n' + cat_result)
            uuid = run_command(['cl', 'run', 'stress-test.pl:' + upload_uuid, 'perl stress-test.pl 10 50 50'])
            run_command(['cl', 'wait', uuid], 15, 90)  # Running might take a while
            run_command(['cl', 'rm', upload_uuid, uuid])

    except Exception, e:
        error_logs('exception', 'Exception: %s' % e)

    log('=== END REPORT')

    # Email the report
    send_email('codalab stress-test report', report)

    time.sleep(1)


N = 4
array = [None] * N
pool = ThreadPool(N)
results = pool.map(stress_test, array)
print 'finish:', results
