import paramiko
import subprocess
import os

from codalab.lib import (
  canonicalize,
  path_util,
)

from codalab.objects.machine import Machine

class RemoteMachine(Machine):
    def __init__(self, target, username, remote_directory):
        client = paramiko.SSHClient()
        # TODO probably remove this line
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.load_system_host_keys()
        try:
            client.connect(target, username=username)
        # TODO probably remove this
        except paramiko.PasswordRequiredException:
            password = getpass.getpass('RSA key password: ')
            client.connect(target, username=username, password=password)

        self.username = username
        self.target = target

        self.client = client
        self.channel = None
        self.remote_directory = remote_directory

    def call_rsync(self, local_dir):
        args = ["rsync", "-avz", local_dir, self.username+'@'+self.target+":"+self.remote_directory]
        subprocess.call(args)

    def run_bundle(self, bundle, bundle_store, parent_dict):
        # Get directories straight
        temp_dir = canonicalize.get_current_location(bundle_store, bundle.uuid)
        path_util.try_make_directory(temp_dir)
        pairs = bundle.get_dependency_paths(bundle_store, parent_dict, temp_dir)

        # Copy files
        for (source, target) in pairs:
            path_util.try_copy(source, target)
        self.call_rsync(temp_dir)

        shell = False
        # Capture stdout/err and start process
        if shell:
            c = self.client.invoke_shell()
        else:
            c = self.client.get_transport().open_session()
            c.get_pty()

        # Store objects
        self.bundle = bundle
        self.channel = c
        self.out = c.makefile()
        self.err = c.makefile_stderr()
        self.temp_dir = temp_dir

        # Run command
        # TODO chdir into remote enviroment directory

        cd_command = 'cd ' + os.path.join(self.remote_directory, bundle.uuid)
        if shell:
            c.send(cd_command)
            c.send(bundle.command)
        else:
            c.exec_command(cd_command + ' && ' + bundle.command)
        return True

    def poll(self):
        if self.channel and self.channel.exit_status_ready():
            return self.result()
        else:
            return None

    def result(self):
        # This call blocks
        success = self.channel.recv_exit_status() == 0
        # Read process output
        # TODO update this during execution
        with path_util.chdir(self.temp_dir):
            path_util.try_make_directory('output')
            with open('stdout', 'wb') as stdout, open('stderr', 'wb') as stderr:
                stdout.write(self.out.read());
                stderr.write(self.err.read());

        return (self.bundle, success, self.temp_dir)

    def kill(self, uuid):
        if self.bundle.uuid == uuid:
            self.channel.close();
            return self.result()
        else:
            return None

    def finalize(self, uuid):
        if self.bundle.uuid == uuid:
            #path_util.remove(self.temp_dir)
            # TODO clean up remote files
            self.out.close()
            self.err.close()
            self.channel = None
            return True
        else:
            return False
