#!/usr/bin/env python

# Wrapper for fig's simple workqueue system.
# https://github.com/percyliang/fig/blob/master/bin/q
# Each command outputs JSON.

import sys, os, json, re
import subprocess
import argparse

q_path = os.path.join(os.path.dirname(__file__), 'q')
if not os.path.exists(q_path):
    print 'Missing %s' % q_path
    sys.exit(1)

def get_output(command):
    print >>sys.stderr, 'dispatch-q.py: %s' % ' '.join(command),
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

mode = sys.argv[1]
if mode == 'start':
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', type=str, help='user who is running this job')
    parser.add_argument('--request-time', type=float, help='request this much computation time (in seconds)')
    parser.add_argument('--request-memory', type=float, help='request this much memory (in bytes)')
    parser.add_argument('--request-disk', type=float, help='request this much memory (in bytes)')
    parser.add_argument('--request-cpus', type=int, help='request this many CPUs')
    parser.add_argument('--request-gpus', type=int, help='request this many GPUs')
    parser.add_argument('--request-queue', type=str, help='submit job to this queue')
    parser.add_argument('--request-priority', type=int, help='priority of this job (higher is more important)')
    parser.add_argument('--share-working-path', help='whether we should run the job directly in the script directory', action='store_true')
    parser.add_argument('script', type=str, help='script to run')
    args = parser.parse_args(sys.argv[2:])

    resource_args = []
    if args.request_time != None:
        resource_args.extend(['-time', '%ds' % int(args.request_time)])

    # TODO: if running in docker container, this doesn't do anything since q
    # doesn't know about docker, and the script is not the thing actually
    # taking memory.
    if args.request_memory != None:
        resource_args.extend(['-mem', '%dm' % int(args.request_memory / (1024*1024))]) # convert to MB

    if args.request_priority != None:
        resource_args.extend(['-priority', '--', '%d' % (-args.request_priority)])  # Note: need to invert

    if args.share_working_path:
        # Run directly in the same directory.
        resource_args.extend(['-shareWorkingPath', 'true'])
        launch_script = args.script
    else:
        # q will run the script in a <scratch> directory.
        # args.script: <path>/<uuid>.sh
        # Tell q to copy everything related to <uuid> back.
        orig_path = os.path.dirname(args.script)
        uuid = os.path.basename(args.script).split('.')[0]
        resource_args.extend(['-shareWorkingPath', 'false'])
        resource_args.extend(['-inPaths'] + ['%s/%s' % (orig_path, f) for f in os.listdir(orig_path) if f.startswith(uuid)])
        resource_args.extend(['-realtimeInPaths', '%s/%s.action' % (orig_path, uuid)])  # To send messages (e.g., kill)
        resource_args.extend(['-outPath', '%s' % orig_path])
        resource_args.extend(['-outFiles', 'full:%s*' % uuid])
        # Need to point to new script
        if args.script.startswith('/'):
            # Strip leading / to make path relative.
            # This way, q will run the right script.
            os.chdir('/')
            launch_script = args.script[1:]
        else:
            launch_script = args.script

    stdout = get_output([q_path] + resource_args + ['-add', 'bash', launch_script, 'use_script_for_temp_dir'])
    m = re.match(r'Job (J-.+) added successfully', stdout)
    handle = m.group(1) if m else None
    response = {'raw': stdout, 'handle': handle}
elif mode == 'info':
    handles = sys.argv[2:]  # If empty, then get info about everything
    stdout = get_output([q_path, '-list'] + handles + ['-tabs'])
    response = {'raw': stdout}
    # Example output:
    # handle    worker              status  exitCode   exitReason   time    mem    disk    outName     command
    # J-ifnrj9  mazurka-37 mazurka  done    0          ....         1m40s   1m     -1m                 sleep 100
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

        exitreason = tokens[4]
        if exitreason != '':
            info['exitreason'] = exitreason

        time = tokens[5]
        if time and time != '-1':
            info['time'] = int(time)

        memory = tokens[6]
        if memory and memory != '-1':
            info['memory'] = int(memory) * 1024 * 1024  # Convert to bytes

        infos.append(info)
    response['infos'] = infos
elif mode == 'kill':
    handle = sys.argv[2]
    response = {
        'handle': handle,
        'raw': get_output([q_path, '-kill', handle])
    }
elif mode == 'cleanup':
    handle = sys.argv[2]
    response = {
        'handle': handle,
        'raw': get_output([q_path, '-del', handle])
    }
else:
    print 'Invalid mode: %s' % mode
    sys.exit(1)

print json.dumps(response)
