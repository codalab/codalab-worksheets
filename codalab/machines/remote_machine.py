import os
import subprocess
import json
import tempfile
import time
import sys
import traceback
import pwd

from codalab.lib import (
  canonicalize,
  path_util,
  formatting,
)
from codalab.lib.bundle_action import BundleAction

from codalab.objects.machine import Machine

'''
To execute a run, we do the following:

1) Copy all files to a temp directory (ideally on the local directory of the
machine we're going to run on, but this is impossible to know in advance).

2) Invoke a designated command (e.g., qsub) to start the job in that temp
directory.  There needs to be a standard way to specify time, memory, etc.
This call is non-blocking and will return a job handle.

3) If we want to kill the process, then we can call a designated command to
kill that job with the handle.

4) When constantly poll to see if the job has finished.

The above steps depend on a dispatch_command, which is set in the config.json.

Convention: command is a string, args is a list of arguments
'''
class RemoteMachine(Machine):
    def __init__(self, config):
        self.verbose = config.get('verbose', 1)

        # Mandatory configuration
        self.dispatch_command = config['dispatch_command']
        self.default_docker_image = config['docker_image']
        def parse(to_value, field):
            return to_value(config[field]) if field in config else None
        self.default_request_time = parse(formatting.parse_duration, 'default_request_time')
        self.default_request_memory = parse(formatting.parse_size, 'default_request_memory')
        self.default_request_disk = parse(formatting.parse_size, 'default_request_disk')
        self.max_request_time = parse(formatting.parse_duration, 'max_request_time')
        self.max_request_memory = parse(formatting.parse_size, 'max_request_memory')
        self.max_request_disk = parse(formatting.parse_size, 'max_request_disk')

        self.default_request_cpus = config.get('request_cpus')
        self.default_request_gpus = config.get('request_gpus')
        self.default_request_queue = config.get('request_queue')
        self.default_request_priority = config.get('request_priority')
        self.default_request_network = config.get('request_network')

    def run_command_get_stdout(self, args):
        if self.verbose >= 3: print "=== run_command_get_stdout: %s" % (args,)
        proc = subprocess.Popen(args, stdout=subprocess.PIPE)
        stdout, _ = proc.communicate()
        exit_code = proc.returncode 
        if self.verbose >= 4: print "=== run_command_get_stdout: exitcode = %s" % exit_code
        if exit_code != 0:
            print '=== run_command_get_stdout failed: %s' % (args,)
            raise SystemError('Command failed (exitcode = %s): %s' % (exit_code, ' '.join(args)))
        return stdout

    def run_command_get_stdout_json(self, args):
        stdout = self.run_command_get_stdout(args)
        try:
            return json.loads(stdout)
        except:
            print "=== INVALID JSON from %s:" % (args)
            print stdout
            raise

    def start_bundle(self, bundle, bundle_store, parent_dict, username):
        '''
        Sets up all the temporary files and then dispatches the job.
        username: the username of the owner of the bundle
        Returns the bundle information.
        '''
        # Create a temporary directory
        temp_dir = canonicalize.get_current_location(bundle_store, bundle.uuid)
        temp_dir = os.path.realpath(temp_dir)  # Follow symlinks
        path_util.make_directory(temp_dir)

        # Copy all the dependencies to that temporary directory.
        pairs = bundle.get_dependency_paths(bundle_store, parent_dict, temp_dir)
        print >>sys.stderr, 'RemoteMachine.start_bundle: copying dependencies of %s to %s' % (bundle.uuid, temp_dir)
        for (source, target) in pairs:
            path_util.copy(source, target, follow_symlinks=False)

        # Set defaults for the dispatcher.
        docker_image = bundle.metadata.request_docker_image or self.default_docker_image
        # Parse |request_string| using |to_value|, but don't exceed |max_value|.
        def parse_and_min(to_value, request_string, default_value, max_value):
            # Use default if request value doesn't exist
            if request_string:
                request_value = to_value(request_string)
            else:
                request_value = default_value
            if request_value and max_value:
                return int(min(request_value, max_value))
            elif request_value:
                return int(request_value)
            elif max_value:
                return int(max_value)
            else:
                return None
        request_time = parse_and_min(formatting.parse_duration, bundle.metadata.request_time, self.default_request_time, self.max_request_time)
        request_memory = parse_and_min(formatting.parse_size, bundle.metadata.request_memory, self.default_request_memory, self.max_request_memory)
        request_disk = parse_and_min(formatting.parse_size, bundle.metadata.request_disk, self.default_request_disk, self.max_request_disk)

        request_cpus = bundle.metadata.request_cpus or self.default_request_cpus
        request_gpus = bundle.metadata.request_gpus or self.default_request_gpus
        request_queue = bundle.metadata.request_queue or self.default_request_queue
        request_priority = bundle.metadata.request_priority or self.default_request_priority
        request_network = bundle.metadata.request_network or self.default_request_network

        script_file = temp_dir + '.sh'  # main entry point
        ptr_temp_dir = '$temp_dir'
        # 1) If no argument to script_file, use the temp_dir (e.g., Torque, master/worker share file system).
        # 2) If argument is 'use_script_for_temp_dir', use the script to determine temp_dir (e.g., qsub, no master/worker do not share file system).
        set_temp_dir_header = 'if [ -z "$1" ]; then temp_dir=' + temp_dir + '; else temp_dir=`readlink -f $0 | sed -e \'s/\\.sh$//\'`; fi\n'

        # Write the command to be executed to a script.
        internal_script_file = temp_dir + '-internal.sh'  # run inside the docker container
        # These paths depend on $temp_dir, an environment variable which will be set (referenced inside script_file)
        ptr_container_file = ptr_temp_dir + '.cid'  # contains the docker container id
        ptr_action_file = ptr_temp_dir + '.action'  # send actions to the container (e.g., kill)
        ptr_status_dir = ptr_temp_dir + '.status'  # receive information from the container (e.g., memory)
        ptr_script_file = ptr_temp_dir + '.sh'  # main entry point
        ptr_internal_script_file = ptr_temp_dir + '-internal.sh'  # run inside the docker container
        # Names of file inside the docker container
        docker_temp_dir = '/' + bundle.uuid
        docker_internal_script_file = '/' + bundle.uuid + '-internal.sh'

        # 1) script_file starts the docker container and runs internal_script_file in docker.
        # --rm removes the docker container once the job terminates (note that this makes things slow)
        # -v mounts the internal and user scripts and the temp directory
        # Trap SIGTERM and forward it to docker.
        with open(script_file, 'w') as f:
            f.write(set_temp_dir_header)

            # Monitor CPU/memory/disk
            # Used to copy status about the docker container.
            def copy_if_exists(source_template, arg, target):
                source = source_template % arg
                # -f because target might be read-only
                return 'if [ -e %s ] && [ -e %s ]; then cp -f %s %s; fi' % (arg, source, source, target)

            def get_field(path, col):
                return 'cat %s | cut -f%s -d\'%s\'' % (path, col, BundleAction.SEPARATOR)

            monitor_commands = [
                # Report on status (memory, cpu, etc.)
                'mkdir -p %s' % ptr_status_dir,
                'if [ -e /cgroup ]; then cgroup=/cgroup; else cgroup=/sys/fs/cgroup; fi',  # find where cgroup is
                copy_if_exists('$cgroup/cpuacct/docker/$(cat %s)/cpuacct.stat', ptr_container_file, ptr_status_dir),
                copy_if_exists('$cgroup/memory/docker/$(cat %s)/memory.usage_in_bytes', ptr_container_file, ptr_status_dir),
                copy_if_exists('$cgroup/blkio/docker/$(cat %s)/blkio.throttle.io_service_bytes', ptr_container_file, ptr_status_dir),
                # Enforce memory limits
                '[ -e "%s/memory.usage_in_bytes" ] && mem=$(cat %s/memory.usage_in_bytes)' % (ptr_status_dir, ptr_status_dir),
                'echo "memory: $mem (max %s)"' % request_memory,
                'if [ -n "$mem" ] && [ "$mem" -gt "%s" ]; then echo "[CodaLab] Memory limit exceeded: $mem > %s, terminating." >> %s/stderr; docker kill $(cat %s); break; fi' % \
                    (request_memory, request_memory, ptr_temp_dir, ptr_container_file),
                # Enforce disk limits
                'disk=$(du -sb %s | cut -f1)' % ptr_temp_dir,
                'echo "disk: $disk (max %s)"' % request_disk,
                'if [ -n "$disk" ] && [ "$disk" -gt "%s" ]; then echo "[CodaLab] Disk limit exceeded: $disk > %s, terminating." >> %s/stderr; docker kill $(cat %s); break; fi' % \
                    (request_disk, request_disk, ptr_temp_dir, ptr_container_file),
                # Execute "kill"
                'if [ -e %s ] && [ "$(cat %s)" == "kill" ]; then echo "[CodaLab] Received kill command, terminating." >> %s/stderr; docker kill $(cat %s); rm %s; break; fi' % \
                    (ptr_action_file, ptr_action_file, ptr_temp_dir, ptr_container_file, ptr_action_file),
                # Execute "write <subpath> <contents>"
                'if [ -e %s ] && [ "$(%s)" == "write" ]; then echo Writing...; %s > %s/$(%s); rm %s; fi' % \
                    (ptr_action_file, get_field(ptr_action_file, 1),
                    get_field(ptr_action_file, '3-'), ptr_temp_dir, get_field(ptr_action_file, 2),
                    ptr_action_file),
                # Sleep
                'sleep 1',
            ]
            f.write('while [ -e %s ]; do\n  %s\ndone &\n' % (ptr_temp_dir, '\n  '. join(monitor_commands)))

            resource_args = ''
            # Limiting memory in docker is not (always) supported. So we rely on bash (see above).
            # http://programster.blogspot.com/2014/09/docker-implementing-container-memory.html
            #if request_memory:
            #    resource_args += ' -m %s' % int(formatting.parse_size(request_memory))
            # TODO: would constrain --cpuset=0, but difficult because don't know the CPU ids

            # Attach all GPUs if any. Note that only the 64-bit version of
            # libcuda.so is picked up.
            f.write('devices=$(/bin/ls /dev/nvidia* 2>/dev/null)\n')
            f.write('if [ -n "$devices" ]; then devices=$(for d in $devices; do echo --device $d:$d; done); fi\n')
            f.write('libcuda=$(/sbin/ldconfig -p 2>/dev/null | grep "libcuda.so$" | grep "x86-64" | head -n 1 | cut -d " " -f 4)\n')
            f.write('if [ -n "$libcuda" ]; then libcuda=" -v $libcuda:/usr/lib/x86_64-linux-gnu/libcuda.so:ro"; fi\n')
            resource_args += ' $devices$libcuda'

            # Enable network?
            if not request_network:
                resource_args += ' --net=none'

            f.write("docker run%s --rm --cidfile %s -u %s -v %s:%s -v %s:%s -e HOME=%s %s bash %s >%s/stdout 2>%s/stderr & wait $!\n" % (
                resource_args,
                ptr_container_file,
                os.geteuid(),
                ptr_temp_dir, docker_temp_dir,
                ptr_internal_script_file, docker_internal_script_file,
                docker_temp_dir,
                docker_image,
                docker_internal_script_file,
                ptr_temp_dir, ptr_temp_dir))

        # 2) internal_script_file runs the actual command inside the docker container
        with open(internal_script_file, 'w') as f:
            # Make sure I have a username
            username = pwd.getpwuid(os.getuid())[0]  # do this because os.getlogin() doesn't always work
            f.write("[ -w /etc/passwd ] && echo %s::%s:%s::/:/bin/bash >> /etc/passwd\n" % (username, os.geteuid(), os.getgid()))
            # Do this because .bashrc isn't sourced automatically (even with --login, though it works with docker -t -i, strange...)
            f.write("[ -e .bashrc ] && . .bashrc\n")
            # Go into the temp directory
            f.write("cd %s &&\n" % docker_temp_dir)
            # Run the actual command
            f.write('(%s) >>stdout 2>>stderr\n' % bundle.command)

        # Determine resources to request
        resource_args = []
        if request_time:
            resource_args.extend(['--request-time', request_time])
        if request_memory:
            resource_args.extend(['--request-memory', request_memory])
        if request_disk:
            resource_args.extend(['--request-disk', request_disk])
        if request_cpus:
            resource_args.extend(['--request-cpus', request_cpus])
        if request_gpus:
            resource_args.extend(['--request-gpus', request_gpus])
        if request_queue:
            resource_args.extend(['--request-queue', request_queue])
        if request_priority:
            resource_args.extend(['--request-priority', request_priority])
        if username:
            resource_args.extend(['--username', username])

        # Start the command
        args = self.dispatch_command.split() + ['start'] + map(str, resource_args) + [script_file]
        if self.verbose >= 1: print '=== start_bundle(): running %s' % args
        result = json.loads(self.run_command_get_stdout(args))
        if self.verbose >= 1: print '=== start_bundle(): got %s' % result

        if not result['handle']:
            raise SystemError('Starting bundle failed')

        # Return the information about the job.
        return {
            'bundle': bundle,
            'temp_dir': temp_dir,
            'job_handle': result['handle'],
            'docker_image': docker_image,
            'request_time': str(request_time) if request_time else None,
            'request_memory': str(request_memory) if request_memory else None,
            'request_disk': str(request_disk) if request_disk else None,
            'request_cpus': request_cpus,
            'request_gpus': request_gpus,
            'request_queue': request_queue,
            'request_priority': request_priority,
            'request_network': request_network,
        }

    def get_bundle_statuses(self):
        '''
        Return a list of bundle metadata information.
        '''
        try:
            # Get status
            args = self.dispatch_command.split() + ['info']
            response = self.run_command_get_stdout_json(args)
            if self.verbose >= 2: print '=== get_bundle_statuses: %s' % response
            statuses = []
            for info in response['infos']:
                status = {
                    'exitcode': info.get('exitcode'),
                    'failure_message': 'Exit reason: ' + info.get('exitreason') if info.get('exitreason') else None,
                    'remote': info.get('hostname'),
                    'time': info.get('time'),
                    'memory': info.get('memory'),
                    'state': info.get('state'),
                    'job_handle': info.get('handle'),
                }

                # This only applies when we are running inside docker.
                # We don't have access to the temp_dir at this point, so make a
                # function when given a directory, will retrieve the
                # appropriate information.
                def bundle_handler(bundle):
                    # Read out information from the status file
                    # (See start_bundle for what's printed out)
                    temp_dir = getattr(bundle.metadata, 'temp_dir', None)
                    if temp_dir == None: return {}
                    status_dir = temp_dir + '.status'
                    status_update = {}

                    # Get CPU usage (in contrast, time is wall clock, including CodaLab's time)
                    try:
                        for line in open(os.path.join(status_dir, 'cpuacct.stat')):
                            key, value = line.split(" ")
                            # NOTE: there is a bug in /cgroup where the first values are garbage (way too high).
                            # Convert jiffies to seconds
                            if key == 'user':
                                status_update['time_user'] = int(value) / 100.0
                            elif key == 'system':
                                status_update['time_system'] = int(value) / 100.0
                    except:
                        pass

                    # Get memory usage (seems reliable)
                    try:
                        status_update['memory'] = int(open(os.path.join(status_dir, 'memory.usage_in_bytes')).read())
                    except:
                        pass

                    # Get disk usage (not reliable probably since we're writing to a volume outside docker)
                    try:
                        for line in open(os.path.join(status_dir, 'blkio.throttle.io_service_bytes')):
                            _, key, value = line.split(" ")
                            if key == 'Read':
                                status_update['disk_read'] = int(value)
                            elif key == 'Write':
                                status_update['disk_write'] = int(value)
                    except:
                        pass
                    return status_update

                status['bundle_handler'] = bundle_handler
                    
                status['success'] = status['exitcode'] == 0 if status['exitcode'] is not None else None
                statuses.append(status)
            return statuses
        except Exception, e:
            print '=== INTERNAL ERROR: %s' % e
            traceback.print_exc()
            return []

    def send_bundle_action(self, bundle, action_string):
        """
        Write a command out to the action file for |bundle|.
        Return whether the action was executed.
        """
        if self.verbose >= 1: print >>sys.stderr, '=== RemoteMachine.send_bundle_action(%s, %s)' % (bundle.uuid, action_string)
        if not self._exists(bundle):
            return False

        try:
            # Write the kill action for the worker to pick up.
            action_file = bundle.metadata.temp_dir + '.action'
            with open(action_file, 'w') as f:
                print >>f, action_string
        except Exception, e:
            print >>sys.stderr, '=== INTERNAL ERROR: %s' % e
            traceback.print_exc()
        return True

    def finalize_bundle(self, bundle):
        if self.verbose >= 1: print >>sys.stderr, '=== RemoteMachine.finalize_bundle(%s)' % bundle.uuid
        if not self._exists(bundle):
            return

        try:
            args = self.dispatch_command.split() + ['cleanup', bundle.metadata.job_handle]
            result = self.run_command_get_stdout_json(args)
            # Sync this with files created in start_bundle
            temp_dir = bundle.metadata.temp_dir
            container_file = temp_dir + '.cid'
            action_file = temp_dir + '.action'
            status_dir = temp_dir + '.status'
            script_file = temp_dir + '.sh'
            internal_script_file = temp_dir + '-internal.sh'
            temp_files = [container_file, action_file, status_dir, script_file, internal_script_file]
            for f in temp_files:
                if os.path.exists(f):
                    path_util.remove(f)
        except Exception, e:
            print >>sys.stderr, '=== INTERNAL ERROR: %s' % e
            traceback.print_exc()

    def _exists(self, bundle):
        if not getattr(bundle.metadata, 'job_handle', None):
            if self.verbose >= 1: print >>sys.stderr, 'remote_machine._exists: bundle %s does not have job handle (yet)' % bundle.uuid
            return False
        return True
