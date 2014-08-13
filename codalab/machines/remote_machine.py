import os
import subprocess

from codalab.lib import (
  canonicalize,
  path_util,
)

from codalab.objects.machine import Machine

# Convention: command is a string, args is a list of arguments

class RemoteMachine(Machine):
    def __init__(self, target, username, remote_directory, verbose=1):
        self.username = username
        self.target = target
        self.verbose = verbose
        self.process = None
        self.remote_directory = remote_directory  # This is the base directory where bundles are stored

    def get_host_string(self):
        return "{username}@{target}".format(username=self.username, target=self.target)
    def get_remote_dir(self):
        return os.path.join(self.remote_directory, self.bundle.uuid)

    def get_ssh_args(self, command):
        # Use -t so we can kill the remote process
        # Use -o LogLevel=Quiet to suppress "Connection to ... closed." message
        return ['ssh', '-t', '-o', 'LogLevel=Quiet', self.get_host_string(), command]

    def run_local(self, args):
        if self.verbose >= 2: print "=== run_local: %s" % (args,)
        exit_code = subprocess.call(args)
        #print exit_code
        return exit_code == 0

    # TODO fix erroneous stderr output
    def start_command(self):
        '''
        Start running the command.'
        '''
        # TODO: be careful of quoting
        cd_command = 'cd ' + self.get_remote_dir()
        remote_command = cd_command + ' && touch stdout stderr && (' + self.bundle.command + ') >stdout 2>stderr'

        args = self.get_ssh_args(remote_command)
        def quote(s):
            return '\'' + s.replace('\'', '\\\'') + '\''
        command = ' '.join(map(quote, args))
        if self.verbose >= 1: print '=== start_command: %s' % command
        #return subprocess.Popen(command, stdout=self.stdout, stderr=self.stderr, shell=True)
        # TODO: running ssh screws up the terminal; fix this
        return subprocess.Popen(command, shell=True)

    def make_remote_dir(self):
        command = 'mkdir -p %s' % self.get_remote_dir()
        args = self.get_ssh_args(command)
        self.run_local(args)

    def remove_remote_dir(self):
        command = 'rm -rf %s' % self.get_remote_dir()
        args = self.get_ssh_args(command)
        self.run_local(args)

    def rsync(self, source, dest):
        # Copy the contents of source into the contents of dest (assume both exist).
        flags = '-az'
        #flags += 'v'
        args = ["rsync", flags, source, dest]
        self.run_local(args)

    def copy_local_to_remote(self):
        source = os.path.join(self.temp_dir, '')
        dest = self.get_host_string()+":"+self.get_remote_dir()
        self.rsync(source, dest)

    def copy_remote_to_local(self, copy_all):
        if copy_all:
            files = ''
        else:
            files = '{stdout,stderr}'
        source = os.path.join(self.get_host_string()+":"+self.get_remote_dir(), files)
        dest = self.temp_dir
        self.rsync(source, dest)

    # Sets up remote environment, starts bundle command
    def start_bundle(self, bundle, bundle_store, parent_dict):
        self.bundle = bundle
        self.temp_dir = canonicalize.get_current_location(bundle_store, bundle.uuid)

        # Prepare a temporary directory and copy it to remote
        # TODO: in the future, should copy files directly to remote
        path_util.make_directory(self.temp_dir)
        pairs = bundle.get_dependency_paths(bundle_store, parent_dict, self.temp_dir)
        for (source, target) in pairs:
            path_util.copy(source, target)
        self.make_remote_dir()
        self.copy_local_to_remote()

        # Start process
        #with path_util.chdir(self.temp_dir):
        #    self.stdout = open('stdout', 'wb')
        #    self.stderr = open('stderr', 'wb')
        self.process = self.start_command()

        return True

    def poll(self):
        if not self.process: return None
        self.process.poll()

        if self.process.returncode != None:
            self.copy_remote_to_local(copy_all=True)
            if self.verbose >= 1: print '=== poll(): returncode = %s' % self.process.returncode
            success = self.process.returncode == 0
            return (self.bundle, success, self.temp_dir)
        else:
            self.copy_remote_to_local(copy_all=False)
            return None

    def kill_bundle(self, uuid):
        if self.bundle.uuid == uuid:
            result = self.process.terminate()
            if self.verbose >= 1: print '=== kill_bundle %s => %s' % (self.process, result)
            return True
        else:
            return False

    def finalize_bundle(self, uuid):
        if self.bundle.uuid == uuid:
            print 'finalize_bundle %s' % uuid
            path_util.remove(self.temp_dir) # Remove local directory
            self.remove_remote_dir()  # Remove remote directory

            # Clean up
            #self.stdout.close()
            #self.stderr.close()
            self.process = None
            self.bundle = None
            return True
        else:
            return False
