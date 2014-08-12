import os
import subprocess

from codalab.lib import (
  canonicalize,
  path_util,
)

from codalab.objects.machine import Machine

class RemoteMachine(Machine):
    def __init__(self, target, username, remote_directory):
        self.username = username
        self.target = target

        self.process = None
        self.remote_directory = remote_directory

    def get_host_string(self):
        return "{username}@{target}".format(
                username=self.username,
                target=self.target)
    def get_remote_directory(self):
        return os.path.join(self.remote_directory, self.bundle.uuid)

    def start_ssh_command(self):
        cd_command = 'cd ' + self.get_remote_directory()
        mkdir_command = 'mkdir output'
        remote_command = ' && '.join([
            cd_command,
            mkdir_command,
            self.bundle.command])

        # Use -t so we can kill the remote process
        # Use -o option to suppress "Connection to ... closed." message
        command = 'ssh -t -o LogLevel=Quiet {host} "{command}"'.format(
                host=self.get_host_string(),
                command=remote_command.replace("\"", "\\\""))
        return subprocess.Popen(command, stdout=self.stdout, stderr=self.stderr, shell=True)

    def call_rsync_command(self):
        # Adds trailing slash. Needed for correct rsync behavior.
        local_dir_arg  = os.path.join(self.temp_dir, '') 
        remote_dir_arg = self.get_remote_directory()

        host_string = self.get_host_string()
        args = ["rsync", "-avz", local_dir_arg, host_string+":"+remote_dir_arg]

        # Hide stdout
        with open(os.devnull, 'wb') as devnull:
            subprocess.call(args, stdout=devnull)

    def call_scp_command(self):
        command = 'scp -r {host}:{remote_dir}/output {local_dir}'.format(
                host=self.get_host_string(),
                remote_dir=self.get_remote_directory(),
                local_dir=self.temp_dir)

        # Hide stdout
        with open(os.devnull, 'wb') as devnull:
            subprocess.call(command, stdout=devnull, shell=True)

    # Sets up remote environment, starts bundle command
    def run_bundle(self, bundle, bundle_store, parent_dict):
        temp_dir = canonicalize.get_current_location(bundle_store, bundle.uuid)

        # Store objects
        self.bundle = bundle
        self.temp_dir = temp_dir

        # Copy input files
        path_util.try_make_directory(temp_dir)
        pairs = bundle.get_dependency_paths(bundle_store, parent_dict, temp_dir)
        for (source, target) in pairs:
            path_util.try_copy(source, target)
        self.call_rsync_command()

        # Start process
        with path_util.chdir(self.temp_dir):
            self.stdout = open('stdout', 'wb')
            self.stderr = open('stderr', 'wb')
            self.process = self.start_ssh_command()

        return True

    def poll(self):
        if self.process:
            self.process.poll()
        if self.process and self.process.returncode != None:
            return self.result()
        else:
            return None

    def result(self):
        success = self.process.returncode == 0

        # Copy output directory from remote host into temp_dir
        self.call_scp_command()

        return (self.bundle, success, self.temp_dir)

    def kill(self, uuid):
        if self.bundle.uuid == uuid:
            self.process.kill()
            return self.result()
        else:
            return None

    def finalize(self, uuid):
        if self.bundle.uuid == uuid:
            path_util.remove(self.temp_dir)
            # TODO clean up remote files
            self.stdout.close()
            self.stderr.close()
            self.process = None
            self.bundle = None
            return True
        else:
            return False
