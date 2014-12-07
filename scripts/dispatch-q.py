#!/usr/bin/env python

# Wrapper for fig's simple workqueue system.
# https://github.com/percyliang/fig/blob/master/bin/q
# Each command outputs JSON.

import sys, os, json, re
import subprocess

def get_output(command):
    print >>sys.stderr, 'dispatch-q.py: ' + command
    return subprocess.check_output(command, shell=True)

if len(sys.argv) <= 1:
    print 'Usage:'
    print '  start [-mem 5g] [-time 10h] <script>'
    print '    => {handle: ...}'
    print '  info <handle>'
    print '    => {handle: ..., remote: ..., memory: ..., raw: ...}'
    print '  kill <handle>'
    print '    => {handle: ...}'
    print '  cleanup <handle>'
    print '    => {handle: ...}'
    sys.exit(1)

result = {}
handle = None

mode = sys.argv[1]
if mode == 'start':
    script = sys.argv[2]
    stdout = get_output('q -shareWorkingPath -add bash %s' % script)
    m = re.match(r'Job (J-.+) added successfully', stdout)
    if m:
        handle = m.group(1)
elif mode == 'info':
    handle = sys.argv[2]
    stdout = get_output('q -list %s -tabs' % handle)
    # Example output
    # handle	worker	status	exitcode	time	mem	disk	outName	command
    # J-ifnrj9	mazurka-37 mazurka	done	0	1m40s	1m	-1m		sleep 100
    tokens = stdout.strip().split("\t")
    hostname = tokens[1]
    if hostname != '':
        result['hostname'] = hostname.split()[-1]
    exitcode = tokens[3]
    if exitcode != '':
        result['exitcode'] = int(exitcode)
elif mode == 'kill':
    handle = sys.argv[2]
    stdout = get_output('q -kill %s' % handle)
elif mode == 'cleanup':
    handle = sys.argv[2]
    stdout = get_output('q -del %s' % handle)
else:
    print 'Invalid mode: %s' % mode
    sys.exit(1)

result['handle'] = handle
result['raw'] = stdout

print json.dumps(result)
