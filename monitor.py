#!/usr/bin/env python

import os, sys
import datetime
from collections import defaultdict
from smtplib import SMTP
from email.mime.text import MIMEText
from getpass import getpass
import subprocess
import re
import time
import socket
import argparse
import json

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
parser.add_argument('--log-path', help='file to write the log', default='monitor.log')
parser.add_argument('--backup-path', help='directory to backup database', default='backup')

# How often to do things
parser.add_argument('--ping-interval', help='ping the server every this many seconds', type=int, default=30)
parser.add_argument('--run-interval', help='run a job every this many seconds', type=int, default=5*60)
parser.add_argument('--email-interval', help='email a report every this many seconds', type=int, default=24*60*60)
args = parser.parse_args()

hostname = socket.gethostname()

# Get MySQL username and password for bundles
config_path = os.path.join(args.codalab_home, 'config.json')
config = json.loads(open(config_path).read())
engine_url = config['server']['engine_url']
m = re.match('mysql://(.+):(.+)@localhost:3306/(.+)', engine_url)
if not m:
    print 'Can\'t extract server.engine_url from %s' % config_path
    sys.exit(1)
bundles_db = m.group(3)
bundles_user = m.group(1)
bundles_password = m.group(2)
print 'bundles DB: %s; user: %s' % (bundles_db, bundles_user)

# Get MySQL username and password for website (this should go away when we merge the DBs!)
config_path = os.path.join(args.codalab_home, 'website-config.json')
config = json.loads(open(config_path).read())
db_info = config['database']
website_db = db_info['NAME']
website_user = db_info['USER']
website_password = db_info['PASSWORD']
print 'website DB: %s, user: %s' % (website_db, website_user)

# Email
recipient = config['django']['admin-email']
sender_info = config['email']

# Create backup directory
if not os.path.exists(args.backup_path):
    os.mkdir(args.backup_path)

report = []  # Build up the current report to send in an email

# message is a list
def send_email(subject, message):
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
    return datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')

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

def is_power_of_two(n):
    while n % 2 == 0:
        n /= 2
    return n == 1

num_errors = defaultdict(int)
def error_logs(error_type, s):
    logs(s)
    num_errors[error_type] += 1
    n = num_errors[error_type]
    if is_power_of_two(n):  # Send email only on powers of two to prevent sending too many emails
        send_email('%s [%d times]' % (error_type, n), s.split('\n'))

durations = defaultdict(list)  # Command => durations for that command
def run_command(args, soft_time_limit=5, hard_time_limit=60):
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

    message = '>> %s (exit code %s, time %.2fs [limit: %ds,%ds]; avg %.2fs; max %.2fs)\n%s' % \
        (' '.join(args), exitcode, duration, soft_time_limit, hard_time_limit,
        average_duration, max_duration, output)
    if exitcode == 0:
        logs(message)
    else:
        error_logs('command failed: ' + ' '.join(args), message)

    if duration > soft_time_limit:
        error_logs('command too slow: ' + ' '.join(args), message)

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

def backup_db(db, user, password):
    date = get_date()
    mysql_conf_path = os.path.join(args.codalab_home, 'monitor-mysql.cnf')
    with open(mysql_conf_path, 'w') as f:
        print >>f, '[client]'
        print >>f, 'user="%s"' % user
        print >>f, 'password="%s"' % password
    run_command(['bash', '-c', 'mysqldump --defaults-file=%s %s > %s/%s-%s.mysqldump' % \
        (mysql_conf_path, db, args.backup_path, db, date)])
    os.unlink(mysql_conf_path)

# Begin monitoring loop
run_command(['cl', 'work', 'local::'])
while True:
    del report[:]
    if ping_time():
        log('=== BEGIN REPORT')

    try:
        # Backup DB
        if email_time():
            log('Backup DB')
            backup_db(website_db, website_user, website_password)
            backup_db(bundles_db, bundles_user, bundles_password)

        # Check remaining disk space
        if ping_time():
            result = int(run_command(['df', os.path.join(args.codalab_home, 'bundles')]).split('\n')[1].split()[3])
            if result < 500 * 1024:  # Less than 500 MB, start to worry
                error_logs('low disk space', 'Only %s MB of disk space left!' % (result / 1024))

        # Get statistics on bundles
        if ping_time():
            # Simple things
            run_command(['cl', 'work'])
            run_command(['cl', 'search', '.count'])
        if run_time():
            # More intense
            run_command(['cl', 'search', 'size=.sum'], 20)
            run_command(['cl', 'search', 'size=.sort-', '.limit=5'], 20)
            run_command(['cl', 'search', '.last', '.limit=5'])

        # Try to run a job
        if run_time():
            uuid = run_command(['cl', 'run', 'echo hello'])
            run_command(['cl', 'wait', uuid], 30, 300)  # Running might take a while
            run_command(['cl', 'rm', uuid])
    except Exception, e:
        error_logs('exception', 'Exception: %s' % e)

    if ping_time():
        log('=== END REPORT')

    # Email the report
    if email_time():
        send_email('report', report)

    if ping_time():
        print

    # Update timer
    time.sleep(1)
    timer += 1
