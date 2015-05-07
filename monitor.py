#!/usr/bin/env python

import os, sys
import datetime
from collections import defaultdict
from smtplib import SMTP_SSL, SMTP
from email.mime.text import MIMEText
from getpass import getpass
import subprocess
import re
import time
import socket
import argparse

# This script runs in a loop monitoring the health of the CodaLab instance.
# Here are some of the things the script does:
# - Make sure we don't run out of disk space.
# - Backup the database.
# - Make sure runs finish in a reasonable amount of time.
# - Email if anything goes wrong.
# - Email a daily report.

# Usage:
#  ./monitor.py --sender <your email>

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--sender', help='email address to send to')
parser.add_argument('--recipient', help='email address to send from')
parser.add_argument('--data-path', help='where the CodaLab data is stored', default=os.getenv('HOME') + '/.codalab/data')
parser.add_argument('--log-path', help='email address to send to', default='monitor.log')
parser.add_argument('--backup-path', help='directory to backup database', default='backup')
parser.add_argument('--mysql-conf-path', help='contains username/password for database', default='mysql.cnf')
parser.add_argument('--ping-interval', help='ping the server every this many seconds', default=30)
parser.add_argument('--run-interval', help='run a job every this many seconds', default=5*60)
parser.add_argument('--email-interval', help='email a report every this many seconds', default=24*60*60)
args = parser.parse_args()

if not os.path.exists(args.mysql_conf_path):
    print 'The mysql configuration file %s doesn\'t exist' % args.mysql_conf_path
    print 'This file should contain:'
    print '''
[client]
user="..."
password="..."
'''
    sys.exit(1)

if not os.path.exists(args.backup_path):
    os.mkdir(args.backup_path)

hostname = socket.gethostname()
if args.sender:
    password = getpass('Password for %s: ' % args.sender)
report = []  # Build up the current report to send in an email

# message is a list
def send_email(subject, message):
    sender, recipient = args.sender, args.recipient
    if not recipient:
        recipient = sender
    print 'send_email to %s; subject: %s; message contains %d lines' % (recipient, subject, len(message))
    if not sender:
        return
    s = SMTP("smtp.gmail.com", 587)
    s.ehlo()
    s.starttls()
    s.ehlo()
    msg = MIMEText('\n'.join(message))
    msg['Subject'] = 'CodaLab on %s: %s' % (hostname, subject)
    msg['To'] = recipient
    s.login(sender, password)
    s.sendmail(sender, recipient, msg.as_string())
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

num_errors = defaultdict(int)
def error_logs(error_type, s):
    logs(s)
    num_errors[error_type] += 1
    if num_errors[error_type] == 1:  # Send email only on first error of the type
        send_email(error_type, s.split('\n'))

def run_command(args, time_limit=5):
    start_time = time.time()
    try:
        args = ['timeout', '%ss' % time_limit] + args
        output = subprocess.check_output(args)
        exitcode = 0
    except subprocess.CalledProcessError, e:
        output = e.output
        exitcode = e.returncode
    end_time = time.time()

    message = '>> %s (exit code %s, time %.2fs)\n%s' % (' '.join(args), exitcode, end_time - start_time, output)
    if exitcode == 0:
        logs(message)
    else:
        error_logs('command failed', message)
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

# Begin monitoring loop
run_command(['cl', 'work', 'local::codalab'])
while True:
    if ping_time():
        log('=== BEGIN REPORT')

    try:
        # Backup files
        if email_time():
            log('Backup files')
            date = get_date()
            run_command(['bash', '-c', 'mysqldump --defaults-file=%s codalab_website > %s/codalab_website-%s.mysqldump' % (args.mysql_conf_path, args.backup_path, date)])
            run_command(['bash', '-c', 'mysqldump --defaults-file=%s codalab_bundles > %s/codalab_bundles-%s.mysqldump' % (args.mysql_conf_path, args.backup_path, date)])
        
        # Check remaining space
        if ping_time():
            result = int(run_command(['df', args.data_path]).split('\n')[1].split()[3])
            if result < 500 * 1024:  # Less than 500 MB, start to worry
                error_logs('low disk space', 'Only %s MB of disk space left!' % (result / 1024))

        # Get statistics
        if ping_time():
            run_command(['cl', 'search', '.count'])
        if run_time():
            run_command(['cl', 'search', 'size=.sum'], 20)
            run_command(['cl', 'search', 'size=.sort-', '.limit=5'], 20)
            run_command(['cl', 'search', '.last', '.limit=5'])
        
        # Try to run a job
        if run_time():
            uuid = run_command(['cl', 'run', 'echo hello'])
            run_command(['cl', 'wait', uuid], 60)
            run_command(['cl', 'rm', uuid])
    except Exception, e:
        error_logs('exception', 'Exception: %s' % e)
    
    if ping_time():
        log('=== END REPORT')
    
    # Email the report
    if email_time():
        send_email('report', report)
        del report[:]

    if ping_time():
        print

    # Update timer
    time.sleep(1)
    timer += 1
