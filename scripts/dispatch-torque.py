#!/usr/bin/env python

# Wrapper for the Torque Resource Manager (PBS).
# http://docs.adaptivecomputing.com/torque/4-1-4/Content/topics/commands/qsub.htm
# Each command outputs JSON.

import sys, os, json, re
import subprocess

def get_output(command):
    print >>sys.stderr, 'dispatch-torque.py: ' + command
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
    stdout = get_output('qsub %s -o /dev/null -e /dev/null' % script)
    handle = stdout.strip()
elif mode == 'info':
    handle = sys.argv[2]
    stdout = get_output('qstat -f %s' % handle)
    for line in stdout.split("\n"):
        m = re.match(r'\s*([^ ]+) = (.+)', line)
        if not m:
            continue
        key = m.group(1)
        value = m.group(2)
        #print key, value
        if key == 'exec_host':
            result['hostname'] = value
        elif key == 'exit_status':
            result['exitcode'] = int(value)
elif mode == 'kill':
    handle = sys.argv[2]
    stdout = get_output('qdel %s' % handle)
elif mode == 'cleanup':
    handle = sys.argv[2]
    stdout = ''
else:
    print 'Invalid mode: %s' % mode
    sys.exit(1)

result['handle'] = handle
result['raw'] = stdout

print json.dumps(result)
