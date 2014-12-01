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

        # State for the current run
        self.bundle = None  # Bundle 
        self.temp_dir = None  # Where the job is being run.
        self.script_file = None  # Script file to invoke the run.
        self.handle = None  # Handle from the dispatcher.

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
        temp_dir = canonicalize.get_current_location(bundle_store, bundle.uuid)
        path_util.make_directory(temp_dir)

        pairs = bundle.get_dependency_paths(bundle_store, parent_dict, temp_dir)
        print >>sys.stderr, 'RemoteMachine.start_bundle: copying dependencies of %s to %s' % (bundle.uuid, temp_dir)
        for (source, target) in pairs:
            path_util.copy(source, target, follow_symlinks=False)

        # Write the command to be executed.
        script_file = os.path.join(temp_dir + '.sh')
        with open(script_file, 'w') as f:
            f.write("cd %s &&\n" % temp_dir)
            f.write('(%s) > stdout 2>stderr\n' % bundle.command)
            f.close()

        # Start the command.
        args = self.dispatch_command.split() + ['start', script_file]
        if self.verbose >= 1: print '=== start_bundle(): running %s' % args
        result = json.loads(self.run_command_get_stdout(args))
        if self.verbose >= 1: print '=== start_bundle(): got %s' % result

        self.bundle = bundle
        self.temp_dir = temp_dir
        self.script_file = script_file
        self.handle = result['handle']
        return True

    def poll(self):
        if not self.handle: return None
        exception = None
        exitcode = -1
        result = {}
        try:
            # Get status
            args = self.dispatch_command.split() + ['info', self.handle]
            result = self.run_command_get_stdout_json(args)
            exitcode = result.get('exitcode')
            if exitcode == None:
                return None  # Not done yet
            if self.verbose >= 0: print '=== poll(%s): %s' % (self.bundle.uuid, result)
        except Exception, e:
            print '=== INTERNAL ERROR: %s' % e
            traceback.print_exc()
            exception = e

        # Return the results back
        result = {
            'bundle': self.bundle,
            'success': exitcode == 0,
            'temp_dir': self.temp_dir,
            'exitcode': exitcode,
            'docker_image': result.get('docker_image', ''),
            'remote': result.get('hostname', '?') + ':' + self.temp_dir,
        }
        if exception:
            result['internal_error'] = str(exception)
        return result

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
            path_util.remove(self.script_file)
            ok = True
        except Exception, e:
            print '=== INTERNAL ERROR: %s' % e
            traceback.print_exc()
            ok = False

        self.bundle = None
        self.temp_dir = None
        self.script_file = None
        self.handle = None

        return ok
