#!/usr/bin/env python

# Wrapper for the Torque Resource Manager (PBS).
# http://docs.adaptivecomputing.com/torque/4-1-4/Content/topics/commands/qsub.htm
# Each command outputs JSON.

import sys, os, json, re
import subprocess
import argparse

def get_output(command):
    print >>sys.stderr, 'dispatch-torque.py: ' + command,
    output = subprocess.check_output(command, shell=True)
    print >>sys.stderr, ('=> %d lines' % len(output.split('\n')))
    return output

if len(sys.argv) <= 1:
    print 'Usage:'
    print '  start [--request-time <seconds>] [--request-memory <bytes>] <script>'
    print '    => {handle: ...}'
    print '  info <handle>*'
    print '    => {..., infos: [{handle: ..., hostname: ..., memory: ...}, ...]}'
    print '  kill <handle>'
    print '    => {handle: ...}'
    print '  cleanup <handle>'
    print '    => {handle: ...}'
    sys.exit(1)

result = {}
handle = None

mode = sys.argv[1]
if mode == 'start':
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', type=str, help='user who is running this job')
    parser.add_argument('--request_time', type=float, help='request this much computation time (in seconds)')
    parser.add_argument('--request_memory', type=float, help='request this much memory (in bytes)')
    parser.add_argument('--request_cpus', type=int, help='request this many CPUs')
    parser.add_argument('--request_gpus', type=int, help='request this many GPUs')
    parser.add_argument('--request_queue', type=str, help='submit job to this queue')
    parser.add_argument('script', type=str, help='script to run')

    args = parser.parse_args(sys.argv[2:])

    resource_args = ''
    if args.username:
        resource_args += ' -N codalab-%s' % args.username
    if args.request_cpus:
        resource_args += ' -l nodes=1:ppn=%d' % args.request_cpus
    if args.request_memory:
        resource_args += ' -l mem=%d' % int(args.request_memory)
    if args.request_queue:
        resource_args += ' -q %s' % args.request_queue

    stdout = get_output('qsub -o /dev/null -e /dev/null%s %s' % (resource_args, args.script))
    handle = stdout.strip()
    response = {'raw': stdout, 'handle': handle}
elif mode == 'info':
    handles = sys.argv[2:]  # If empty, then get info about everything
    list_args = ''
    if len(handles) > 0:
        list_args += ' ' + ' '.join(handles)
    stdout = get_output('qstat -f%s' % list_args)
    response = {'raw': ''}

    infos = []
    for line in stdout.split("\n"):
        # Job Id: ...
        m = re.match('^Job Id: (.+)', line)
        if m:
            info = {'handle': m.group(1)}
            completed = False
            continue

        if line == '':
            # Ensure exitcode if job is completed
            if completed and 'exitcode' not in result:
                result['exitcode'] = -1

            # Flush
            if info: infos.append(info)
            info = None
            continue

        m = re.match(r'\s*([^ ]+) = (.+)', line)
        if not m:
            continue
        key = m.group(1)
        value = m.group(2)
        #print key, value
        if key == 'exec_host':
            info['hostname'] = value
        elif key == 'exit_status':
            info['exitcode'] = int(value)
        elif key == 'job_state':
            if value == 'C':
                completed = True
            result['state'] = {'R': 'running'}.get(value, 'queued')
        elif key == 'resources_used.mem':
            m = re.match(r'(\d+)kb', value)
            if m:
                info['memory'] = int(m.group(1)) * 1024
        elif key == 'resources_used.walltime':
            m = re.match('(\d+):(\d+):(\d+)', value)
            if m:
                info['time'] = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
    response['infos'] = infos

elif mode == 'kill':
    handle = sys.argv[2]
    response = {
        'handle': handle,
        'raw': get_output('qdel %s' % handle)
    }
elif mode == 'cleanup':
    # Do nothing
    handle = sys.argv[2]
    response = {
        'handle': handle,
        'raw': '',
    }
else:
    print 'Invalid mode: %s' % mode
    sys.exit(1)

print json.dumps(response)
