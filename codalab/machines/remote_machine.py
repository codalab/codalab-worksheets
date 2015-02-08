import os
import subprocess
import json
import tempfile
import time
import sys
import traceback

from codalab.lib import (
  canonicalize,
  path_util,
  formatting,
)

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
        self.dispatch_command = config['dispatch_command']
        self.default_docker_image = config.get('docker_image')

    def run_command_get_stdout(self, args):
        if self.verbose >= 3: print "=== run_command_get_stdout: %s" % (args,)
        proc = subprocess.Popen(args, stdout=subprocess.PIPE)
        stdout, _ = proc.communicate()
        exit_code = proc.returncode 
        if self.verbose >= 4: print "=== run_command_get_stdout: exitcode = %s" % exit_code
        if exit_code != 0:
            print '=== run_command_get_stdout failed: %s' % (args,)
            raise SystemError('Command failed (exitcode = %s): %s' % (exit_code, args))
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

        # Set docker image
        docker_image = self.default_docker_image
        if bundle.metadata.request_docker_image:
            docker_image = bundle.metadata.request_docker_image

        # Write the command to be executed to a script.
        if docker_image:
            container_file = temp_dir + '.cid'
            status_dir = temp_dir + '.status'
            script_file = temp_dir + '.sh'
            internal_script_file = temp_dir + '-internal.sh'
            docker_temp_dir = bundle.uuid
            docker_internal_script_file = bundle.uuid + '-internal.sh'

            # 1) script_file starts the docker container and runs internal_script_file in docker.
            # --rm removes the docker container once the job terminates (note that this makes things slow)
            # -v mounts the internal and user scripts and the temp directory
            # Trap SIGTERM and forward it to docker.
            with open(script_file, 'w') as f:
                # NOTE: trap doesn't quite work reliably with Torque.
                f.write('trap \'echo Killing docker container $(cat %s); docker kill $(cat %s); echo Killed: $?; exit 143\' TERM\n' % (container_file, container_file))
                # inspect doesn't tell us a lot
                #f.write('while [ -e %s ]; do docker inspect $(cat %s) > %s; sleep 1; done &\n' % (temp_dir, container_file, status_dir))
                
                # Monitor CPU/memory/disk
                monitor_commands = [
                    'mkdir -p %s' % status_dir,
                    'if [ -e /cgroup ]; then cgroup=/cgroup; else cgroup=/sys/fs/cgroup; fi',  # find where cgroup is
                    'cp -f $cgroup/cpuacct/docker/$(cat %s)/cpuacct.stat %s' % (container_file, status_dir),
                    'cp -f $cgroup/memory/docker/$(cat %s)/memory.usage_in_bytes %s' % (container_file, status_dir),
                    'cp -f $cgroup/blkio/docker/$(cat %s)/blkio.throttle.io_service_bytes %s' % (container_file, status_dir),
                ]
                f.write('while [ -e %s ]; do %s; sleep 1; done &\n' % (temp_dir, '; '. join(monitor_commands)))

                # Constrain resources
                resource_args = ''
                if bundle.metadata.request_memory:
                    resource_args += ' -m %s' % formatting.parse_size(bundle.metadata.request_memory)
                # TODO: would constrain --cpuset=0, but difficult because don't know the CPU ids

                f.write("docker run%s --rm --cidfile %s -u %s -v %s:/%s -v %s:/%s %s bash %s & wait $!\n" % (
                    resource_args,
                    container_file, os.geteuid(),
                    temp_dir, docker_temp_dir,
                    internal_script_file, docker_internal_script_file,
                    docker_image, docker_internal_script_file))

            # 2) internal_script_file runs the actual command inside the docker container
            with open(internal_script_file, 'w') as f:
                # Make sure I have a username
                f.write("echo %s::%s:%s::/:/bin/bash >> /etc/passwd\n" % (os.getlogin(), os.geteuid(), os.getgid()))
                # Do this because .bashrc isn't sourced automatically (even with --login, though it works with docker -t -i, strange...)
                f.write(". .bashrc || exit 1\n")
                # Go into the temp directory
                f.write("cd %s &&\n" % docker_temp_dir)
                # Run the actual command
                f.write('(%s) > stdout 2>stderr\n' % bundle.command)
        else:
            # Just run the command regularly without docker
            script_file = temp_dir + '.sh'
            with open(script_file, 'w') as f:
                f.write("cd %s &&\n" % temp_dir)
                f.write('(%s) > stdout 2>stderr\n' % bundle.command)

        # Determine resources to request
        resource_args = []
        if bundle.metadata.request_time:
            resource_args.extend(['--request_time', formatting.parse_duration(bundle.metadata.request_time)])
        if bundle.metadata.request_memory:
            resource_args.extend(['--request_memory', formatting.parse_size(bundle.metadata.request_memory)])
        if bundle.metadata.request_cpus:
            resource_args.extend(['--request_cpus', bundle.metadata.request_cpus])
        if bundle.metadata.request_gpus:
            resource_args.extend(['--request_gpus', bundle.metadata.request_gpus])
        if bundle.metadata.request_queue:
            resource_args.extend(['--request_queue', bundle.metadata.request_queue])

        # Start the command
        args = self.dispatch_command.split() + ['start', '--username', username] + map(str, resource_args) + [script_file]
        if self.verbose >= 1: print '=== start_bundle(): running %s' % args
        result = json.loads(self.run_command_get_stdout(args))
        if self.verbose >= 1: print '=== start_bundle(): got %s' % result

        # Return the information about the job.
        return {
            'bundle': bundle,
            'temp_dir': temp_dir,
            'job_handle': result['handle'],
            'docker_image': docker_image,
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
                    'remote': info.get('hostname'),
                    'time': info.get('time'),
                    'memory': info.get('memory'),
                    'state': info.get('state'),
                    'job_handle': info.get('handle'),
                }

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
                    try:
                        status_update['memory'] = int(open(os.path.join(status_dir, 'memory.usage_in_bytes')).read())
                    except:
                        pass
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
                    
                status['success'] = status['exitcode'] == 0 if status['exitcode'] != None else None
                statuses.append(status)
            return statuses
        except Exception, e:
            print '=== INTERNAL ERROR: %s' % e
            traceback.print_exc()
            return []

    def kill_bundle(self, bundle):
        if self.verbose >= 1: print '=== kill_bundle(%s)' % (bundle.uuid)
        try:
            args = self.dispatch_command.split() + ['kill', bundle.metadata.job_handle]
            result = self.run_command_get_stdout_json(args)
            return True
        except Exception, e:
            print '=== INTERNAL ERROR: %s' % e
            traceback.print_exc()
            return False

    def finalize_bundle(self, bundle):
        if self.verbose >= 1: print '=== finalize_bundle(%s)' % bundle.uuid
        try:
            args = self.dispatch_command.split() + ['cleanup', bundle.metadata.job_handle]
            result = self.run_command_get_stdout_json(args)
            # Sync this with files created in start_bundle
            temp_dir = bundle.metadata.temp_dir
            if bundle.metadata.docker_image:
                container_file = temp_dir + '.cid'
                status_dir = temp_dir + '.status'
                script_file = temp_dir + '.sh'
                internal_script_file = temp_dir + '-internal.sh'
                temp_files = [container_file, status_dir, script_file, internal_script_file]
            else:
                script_file = temp_dir + '.sh'
                temp_files = [script_file]
            for f in temp_files:
                if os.path.exists(f):
                    path_util.remove(f)
            ok = True
        except Exception, e:
            print '=== INTERNAL ERROR: %s' % e
            traceback.print_exc()
            ok = False

        return ok
