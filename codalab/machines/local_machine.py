import os
import subprocess

from codalab.lib import (
  canonicalize,
  path_util,
)

from codalab.objects.machine import Machine

class LocalMachine(Machine):
    '''
    Run commands on the local machine.  This is for simple testing only, since
    there is no security at all.
    '''
    def start_bundle(self, bundle, bundle_store, parent_dict):
        '''
        Start a bundle in the background.
        '''
        temp_dir = canonicalize.get_current_location(bundle_store, bundle.uuid)
        path_util.make_directory(temp_dir)
        pairs = bundle.get_dependency_paths(bundle_store, parent_dict, temp_dir)

        if bundle.command:
            with path_util.chdir(temp_dir):
                # Make sure we follow symlinks and copy all the files (might be a
                # bit slow but is safer in case we accidentally clobber any
                # existing bundles).
                for (source, target) in pairs:
                    path_util.copy(source, target, follow_symlinks=True)
                with open('stdout', 'wb') as stdout, open('stderr', 'wb') as stderr:
                    process = subprocess.Popen(bundle.command, stdout=stdout, stderr=stderr, shell=True)
        else:
            process = None

        self.bundle = bundle
        self.process = process
        self.temp_dir = temp_dir
        return True

    def kill_bundle(self, uuid):
        if self.bundle.uuid == uuid:
            self.process.kill()
            return True
        else:
            return False

    def poll(self):
        if self.process == None:
            return (self.bundle, True, self.temp_dir)

        self.process.poll()
        if self.process.returncode != None:
            success = self.process.returncode == 0
            return (self.bundle, success, self.temp_dir)
        else:
            return None

    def finalize_bundle(self, uuid):
        if self.bundle.uuid == uuid:
            path_util.remove(self.temp_dir)
            return True
        else:
            return False
