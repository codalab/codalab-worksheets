#!/usr/bin/env python

# Wrapper for the Torque Resource Manager (PBS).
# http://docs.adaptivecomputing.com/torque/4-1-4/Content/topics/commands/qsub.htm
# Each command outputs JSON.
# This is adapted for the Stanford NLP cluster.

import sys, os, json, re
import subprocess
import argparse

def get_output(command):
    print >>sys.stderr, 'dispatch-torque.py: %s' % ' '.join(command),
    output = subprocess.check_output(command)
    print >>sys.stderr, ('=> %d lines' % (len(output.split('\n')) - 1))
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

handle = None

def ssh(args):
    # ssh into scail to run all PBS commands
    args = ['"' + arg.replace('"', '\\"') + '"' for arg in args]  # Quote arguments
    return ['ssh', '-oBatchMode=yes', '-x', 'scail'] + args

mode = sys.argv[1]
if mode == 'start':
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', type=str, help='user who is running this job')
    parser.add_argument('--request-time', type=float, help='request this much computation time (in seconds)')
    parser.add_argument('--request-memory', type=float, help='request this much memory (in bytes)')
    parser.add_argument('--request-disk', type=float, help='request this much memory (in bytes)')
    parser.add_argument('--request-cpus', type=int, help='request this many CPUs')
    parser.add_argument('--request-gpus', type=int, help='request this many GPUs')
    parser.add_argument('--request-queue', type=str, help='submit job to this queue', default='john')
    parser.add_argument('--request-priority', type=int, help='priority of this job (higher is more important)')
    parser.add_argument('script', type=str, help='script to run')

    args = parser.parse_args(sys.argv[2:])

    resource_args = []
    if args.username != None:
        resource_args.extend(['-N', 'codalab-%s' % args.username])
    if args.request_cpus != None:
        resource_args.extend(['-l', 'nodes=1:ppn=%d' % args.request_cpus])
    if args.request_memory != None:
        resource_args.extend(['-l', 'mem=%d' % int(args.request_memory)])
    if args.request_queue != None:
        # Either host=<host-name> or <queue-name>
        m = re.match('^host=(.+)$', args.request_queue)
        if m:
            resource_args.extend(['-l', 'host=' + m.group(1)])
        else:
            resource_args.extend(['-q', args.request_queue])
    if args.request_priority != None:
        resource_args.extend(['-p', args.request_priority])

    stdout = get_output(ssh(['/usr/bin/env', 'qsub', '-o', '/dev/null', '-e', '/dev/null'] + resource_args + [args.script]))
    handle = stdout.strip()
    response = {'raw': stdout, 'handle': handle}
elif mode == 'info':
    handles = sys.argv[2:]  # If empty, then get info about everything
    stdout = get_output(ssh(['/usr/bin/env', 'qstat', '-f'] + handles))
    response = {'raw': ''}  # Suppress output

    infos = []
    for line in stdout.split("\n"):
        # Job Id: ...
        m = re.match('^Job Id: (.+)', line)
        if m:
            info = {'handle': m.group(1)}
            completed = False
            continue

        if line == '' and info:
            # Ensure exitcode if job is completed
            if completed and 'exitcode' not in info:
                info['exitcode'] = -1
            if completed:
                info['state'] = 'ready' if info['exitcode'] == 0 else 'failed'

            # Flush
            if info.get('job_name', '').startswith('codalab-'):
                infos.append(info)
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
                completed = True  # Don't know if it's ready or failed yet
            elif value == 'R':
                info['state'] = 'running'
            else:  # 'Q' (or anything else)
                info['state'] = 'queued'
        elif key == 'resources_used.mem':
            m = re.match(r'(\d+)kb', value)
            if m:
                info['memory'] = int(m.group(1)) * 1024
        elif key == 'resources_used.walltime':
            m = re.match('(\d+):(\d+):(\d+)', value)
            if m:
                info['time'] = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
        elif key == 'Job_Name':
            info['job_name'] = value
    response['infos'] = infos

elif mode == 'kill':
    handle = sys.argv[2]
    response = {
        'handle': handle,
        'raw': get_output(ssh(['/usr/bin/env', 'qdel', handle]))
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
