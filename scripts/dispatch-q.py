#!/usr/bin/env python

# Wrapper for fig's simple workqueue system.
# https://github.com/percyliang/fig/blob/master/bin/q
# Each command outputs JSON.

import sys, os, json, re
import subprocess
import argparse

def get_output(command):
    print >>sys.stderr, 'dispatch-q.py: ' + command,
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

mode = sys.argv[1]
if mode == 'start':
    parser = argparse.ArgumentParser()
    parser.add_argument('--request_time', type=float, help='request this much computation time (in seconds)')
    parser.add_argument('--request_memory', type=float, help='request this much memory (in bytes)')
    parser.add_argument('--request_cpus', type=int, help='request this many CPUs')
    parser.add_argument('--request_gpus', type=int, help='request this many GPUs')
    parser.add_argument('--request_queue', type=int, help='submit job to this queue')
    parser.add_argument('script', type=str, help='script to run')
    args = parser.parse_args(sys.argv[2:])

    resource_args = ''
    if args.request_time:
        resource_args += ' -time %ds' % args.request_time
    if args.request_memory:
        resource_args += ' -mem %dm' % (args.request_memory / (1024*1024)) # convert to MB

    stdout = get_output('q%s -shareWorkingPath -add bash %s' % (resource_args, args.script))
    m = re.match(r'Job (J-.+) added successfully', stdout)
    handle = m.group(1) if m else None
    response = {'raw': stdout, 'handle': handle}
elif mode == 'info':
    handles = sys.argv[2:]  # If empty, then get info about everything
    list_args = ''
    if len(handles) > 0:
        list_args += ' ' + ' '.join(handles)
    stdout = get_output('q -list%s -tabs' % list_args)
    response = {'raw': stdout}
    # Example output:
    # handle    worker              status  exitcode   time    mem    disk    outName     command
    # J-ifnrj9  mazurka-37 mazurka  done    0          1m40s   1m     -1m                 sleep 100
    infos = []
    for line in stdout.strip().split("\n"):
        if line == '': continue
        tokens = line.split("\t")
        info = {'handle': tokens[0]}

        hostname = tokens[1]
        if hostname != '':
            info['hostname'] = hostname.split()[-1]  # worker => hostname

        info['state'] = {'running': 'running'}.get(tokens[2], 'queued')

        exitcode = tokens[3]
        if exitcode != '':
            info['exitcode'] = int(exitcode)

        time = tokens[4]
        if time:
            info['time'] = int(time)

        memory = tokens[5]
        if memory:
            info['memory'] = int(memory) * 1024 * 1024  # Convert to bytes

        infos.append(info)
    response['infos'] = infos
elif mode == 'kill':
    handle = sys.argv[2]
    response = {
        'handle': handle,
        'raw': get_output('q -kill %s' % handle)
    }
elif mode == 'cleanup':
    handle = sys.argv[2]
    response = {
        'handle': handle,
        'raw': get_output('q -del %s' % handle)
    }
else:
    print 'Invalid mode: %s' % mode
    sys.exit(1)

print json.dumps(response)
