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
        self.docker_image = config.get('docker_image')

        # State for the current run
        self.bundle = None  # Bundle 
        self.temp_dir = None  # Where the job is being run (moved on upload).
        self.temp_files = None  # Files used to run the job (we need to delete).
        self.handle = None  # Job handle used to communicate with the dispatcher.

    # Not used right now
    def run_command(self, args):
        if self.verbose >= 3: print "=== run_command: %s" % (args,)
        # Prints everything to stdout
        exit_code = subprocess.call(args)
        if self.verbose >= 4: print "=== run_command: exitcode = %s" % exit_code
        if exit_code != 0:
            print '=== run_command failed: %s' % (args,)
            raise SystemError('Command failed (exitcode = %s): %s' % (exit_code, args))

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

    # Sets up remote environment, starts bundle command
    def start_bundle(self, bundle, bundle_store, parent_dict):
        if self.bundle != None: raise InternalError('Bundle already started')

        # Create a temporary directory
        temp_dir = canonicalize.get_current_location(bundle_store, bundle.uuid)
        temp_dir = os.path.realpath(temp_dir)  # Follow symlinks
        path_util.make_directory(temp_dir)

        # Copy all the dependencies to that temporary directory.
        pairs = bundle.get_dependency_paths(bundle_store, parent_dict, temp_dir)
        print >>sys.stderr, 'RemoteMachine.start_bundle: copying dependencies of %s to %s' % (bundle.uuid, temp_dir)
        for (source, target) in pairs:
            path_util.copy(source, target, follow_symlinks=False)

        # Write the command to be executed to a script.
        if self.docker_image:
            container_file = temp_dir + '.cid'
            script_file = temp_dir + '.sh'
            internal_script_file = temp_dir + '-internal.sh'
            docker_temp_dir = bundle.uuid
            docker_internal_script_file = bundle.uuid + '-internal.sh'
            temp_files = [container_file, script_file, internal_script_file]

            # 1) script_file starts the docker container and runs internal_script_file in docker.
            # --rm removes the docker container once the job terminates (note that this makes things slow)
            # -v mounts the internal and user scripts and the temp directory
            # Trap SIGTERM and forward it to docker.
            with open(script_file, 'w') as f:
                # TODO: this doesn't quite work reliably with Torque.
                f.write('trap \'echo Killing docker container $(cat %s); docker kill $(cat %s); echo Killed: $?; exit 143\' TERM\n' % (container_file, container_file))
                f.write("docker run --rm --cidfile %s -u %s -v %s:/%s -v %s:/%s %s bash %s & wait $!\n" % (
                    container_file, os.geteuid(),
                    temp_dir, docker_temp_dir,
                    internal_script_file, docker_internal_script_file,
                    self.docker_image, docker_internal_script_file))

            # 2) internal_script_file runs the actual command
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
            temp_files = [script_file]
            with open(script_file, 'w') as f:
                f.write("cd %s &&\n" % temp_dir)
                f.write('(%s) > stdout 2>stderr\n' % bundle.command)

        # Start the command.
        args = self.dispatch_command.split() + ['start', script_file]
        if self.verbose >= 1: print '=== start_bundle(): running %s' % args
        result = json.loads(self.run_command_get_stdout(args))
        if self.verbose >= 1: print '=== start_bundle(): got %s' % result

        # Save the state
        self.bundle = bundle
        self.temp_dir = temp_dir
        self.temp_files = temp_files
        self.handle = result['handle']

        return True

    def get_bundle_statuses(self):
        if not self.handle: return []
        exception = None
        info = {}
        try:
            # Get status
            args = self.dispatch_command.split() + ['info', self.handle]
            info = self.run_command_get_stdout_json(args)
            if self.verbose >= 2: print '=== get_bundle_statuses(%s): %s' % (self.bundle.uuid, info)
        except Exception, e:
            print '=== INTERNAL ERROR: %s' % e
            traceback.print_exc()
            exception = e
            info['exitcode'] = -1  # Fail due to internal error

        status = {
            'bundle': self.bundle,
            'temp_dir': self.temp_dir,
            'exitcode': info.get('exitcode'),
            'docker_image': self.docker_image,
            'remote': info.get('hostname', '?') + ':' + self.temp_dir,
        }
        status['success'] = status['exitcode'] == 0 if status['exitcode'] != None else None
        if exception:
            status['internal_error'] = str(exception)
        return [status]

    def kill_bundle(self, uuid):
        if not self.bundle or self.bundle.uuid != uuid: return False
        if self.verbose >= 1: print '=== kill_bundle(%s)' % (uuid)
        try:
            args = self.dispatch_command.split() + ['kill', self.handle]
            result = self.run_command_get_stdout_json(args)
            return True
        except Exception, e:
            print '=== INTERNAL ERROR: %s' % e
            traceback.print_exc()
            return False

    def finalize_bundle(self, uuid):
        if not self.bundle or self.bundle.uuid != uuid: return False
        if self.verbose >= 1: print '=== finalize_bundle(%s)' % self.bundle.uuid

        try:
            args = self.dispatch_command.split() + ['cleanup', self.handle]
            result = self.run_command_get_stdout_json(args)
            for f in self.temp_files:
                if os.path.exists(f):
                    path_util.remove(f)
            ok = True
        except Exception, e:
            print '=== INTERNAL ERROR: %s' % e
            traceback.print_exc()
            ok = False

        self.bundle = None
        self.temp_dir = None
        self.temp_files = None
        self.handle = None

        return ok
