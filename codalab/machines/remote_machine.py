import os
import subprocess
import json
import tempfile
import time

from codalab.lib import (
  canonicalize,
  path_util,
)

from codalab.objects.machine import Machine

'''
Run commands by ssh'ing into other machines and running docker.
Convention: command is a string, args is a list of arguments
'''
class RemoteMachine(Machine):
    def __init__(self, config):
        self.user = config.get('user')
        self.host = config['host']
        self.verbose = config.get('verbose', 1)
        self.docker_image = config.get('docker_image', '5a76b45a534f')  # codalab/ubuntu
        self.remote_directory = config.get('working_directory', '/tmp/codalab')  # This is the base directory where bundles are stored
        # State
        self.bundle = None
        self.container = None  # id of the container that's running
        self.created_local_dir = False
        self.created_remote_dir = False

    def get_host_string(self):
        return (self.user + '@' if self.user else '') + self.host
    def get_remote_dir(self):
        return os.path.join(self.remote_directory, self.bundle.uuid)
    def get_remote_sh_file(self):
        return self.get_remote_dir() + '.sh'

    def get_ssh_args(self):
        return ['ssh', '-x', self.get_host_string()]

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

    def make_remote_dir(self):
        command = 'mkdir -p %s' % self.get_remote_dir()
        self.run_command(self.get_ssh_args() + [command])

    def remove_remote_dir(self):
        command = 'rm -rf %s %s' % (self.get_remote_dir(), self.get_remote_sh_file())
        self.run_command(self.get_ssh_args() + [command])

    def rsync(self, source, dest):
        # Copy the contents of source into the contents of dest (assume both exist).
        flags = '-az'
        if self.verbose >= 10: flags += 'v'
        args = ["rsync", flags, '--delete', '--exclude', '.nfs*', '-e', 'ssh -x', source, dest]
        self.run_command(args)

    def copy_local_to_remote(self):
        source = os.path.join(self.temp_dir, '')
        dest = self.get_host_string() + ':' + self.get_remote_dir()
        self.rsync(source, dest)
        # Need to give global permissions so these files can be accessed inside docker
        # TODO: combine this with the rsync command
        self.run_command(self.get_ssh_args() + ['chmod -R go=u ' + self.get_remote_dir()])

    def copy_remote_to_local(self, copy_all):
        if copy_all:
            files = ''
        else:
            # Come up with more systematic way to figure out which files to copy back
            # Punt on this because this is too annoying.
            #files = '{stdout,stderr,exec/stats,exec/options.map,exec/output.map}'
            files = ''

        # Copy from remote directory to the local directory
        source = os.path.join(self.get_host_string() + ':' + self.get_remote_dir(), files)
        dest = self.temp_dir
        try:
            self.rsync(source, dest)
        except:
            # Need to do this because some of the files created by docker
            # aren't accessible by the outside.
            print 'WARNING: rsync failed, but ignoring (files might be missing)'
            pass

    # Sets up remote environment, starts bundle command
    def start_bundle(self, bundle, bundle_store, parent_dict):
        if self.bundle != None: raise InternalError('Bundle already started')
        self.bundle = bundle
        self.temp_dir = canonicalize.get_current_location(bundle_store, bundle.uuid)

        # TODO: rsync bundles in the bundle store to remote directly; remote keeps own bundle store

        # Copy to temp directory
        if self.verbose >= 1:
            print '=== start_bundle(): preparing temporary directory %s' % self.temp_dir
        path_util.make_directory(self.temp_dir)
        self.created_local_dir = True
        pairs = bundle.get_dependency_paths(bundle_store, parent_dict, self.temp_dir)
        for (source, target) in pairs:
            # Don't follow random symlinks because people could link to random
            # files on the system and also we need to preserve the local
            # symlink structure.
            path_util.copy(source, target, follow_symlinks=False)

        # Copy from temp directory to remote
        if self.verbose >= 1:
            print '=== start_bundle(): copying to remote %s:%s' % (self.get_host_string(), self.get_remote_dir())
        self.make_remote_dir()
        self.created_remote_dir = True
        self.copy_local_to_remote()

        # Write the command to be executed and copy it as a .sh file
        # This way, we avoid annoying quoting issues
        fd, path = tempfile.mkstemp()
        os.close(fd)
        with open(path, 'w') as f:
            f.write("cd %s &&\n" % self.bundle.uuid)
            f.write('(%s) > stdout 2>stderr\n' % self.bundle.command)
            f.close()
        # Copy the script over
        remote_sh_file = self.get_remote_sh_file()
        container_sh_file = os.path.basename(remote_sh_file)
        if self.verbose >= 10:
            print '---', container_sh_file, '---'
            print open(path).read()
        self.rsync(path, self.get_host_string() + ":" + remote_sh_file)
        os.unlink(path)

        # Create the command to ssh into the machine and run the docker command
        # (-d detaches, -v sets up mount points)
        args = self.get_ssh_args() + ['docker', 'run', '-d']
        args += ['-v',  remote_sh_file + ':/' + container_sh_file + ':ro']
        args += ['-v', self.get_remote_dir() + ':/' + bundle.uuid]
        args += [self.docker_image, 'bash', container_sh_file]

        # Run the command
        if self.verbose >= 1: print '=== start_bundle(): running %s' % args
        stdout = self.run_command_get_stdout(args)

        self.container = stdout.strip()
        if self.verbose >= 2: print '=== start_bundle(): container = %s' % self.container
        return True

    def cleanup(self):
        if self.verbose >= 1: print '=== cleanup(%s)' % self.bundle.uuid
        # Remove local
        if self.created_local_dir:
            path_util.remove(self.temp_dir)
            self.created_local_dir = False
        # Remove remote
        if self.created_remote_dir:
            # Might not have enough permissions to do this since files created
            # in docker are owned by root, so have to run docker to delete the file
            #self.remove_remote_dir()
            args = self.get_ssh_args() + ['docker', 'run', '--rm']
            args += ['-v', self.remote_directory + ':/scratch']
            args += [self.docker_image, 'rm', '-r', '/scratch/' + self.bundle.uuid]
            self.run_command(args)
            self.created_remote_dir = False
        # Remove container
        if self.container:
            stdout = self.run_command_get_stdout(self.get_ssh_args() + ['docker', 'rm', self.container])
            self.container = None
        self.bundle = None
        self.temp_dir = None

    def poll(self):
        if not self.container: return None
        exception = None
        exitcode = -1
        try:
            # Get status
            stdout = self.run_command_get_stdout(self.get_ssh_args() + ['docker', 'inspect', self.container])
            contents = json.loads(stdout)
            state = contents[0]['State']
            if state['Running']:
                # Still running: don't need to copy everything back
                self.copy_remote_to_local(copy_all=False)
                return None
            else:
                # Done: copy all files back
                self.copy_remote_to_local(copy_all=True)
                exitcode = state['ExitCode']

                if self.verbose >= 1: print '=== poll(%s): exitcode = %s' % (self.bundle.uuid, exitcode)
        except Exception as e:
            exception = e

        # Return the results back
        result = {
            'bundle': self.bundle,
            'success': exitcode == 0,
            'temp_dir': self.temp_dir,
            'exitcode': exitcode,
            'docker_image': self.docker_image,
            'remote': self.get_host_string() + ':' + self.get_remote_dir()
        }
        if exception:
            result['internal_error'] = str(exception)
        return result

    def kill_bundle(self, uuid):
        if not self.bundle or self.bundle.uuid != uuid: return False
        if self.verbose >= 1: print '=== kill_bundle(%s): container = %s' % (uuid, self.container)
        try:
            self.run_command(self.get_ssh_args() + ['docker', 'kill', self.container])
            return True
        except:
            return False

    def finalize_bundle(self, uuid):
        if not self.bundle or self.bundle.uuid != uuid: return False
        self.cleanup()
        return True
