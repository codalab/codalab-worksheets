import os
import subprocess

from codalab.lib import (
  canonicalize,
  path_util,
)

from codalab.objects.machine import Machine

class LocalMachine(Machine):

    def run_bundle(self, bundle, bundle_store, parent_dict):
        temp_dir = canonicalize.get_current_location(bundle_store, bundle.uuid)
        path_util.try_make_directory(temp_dir)
        pairs = bundle.get_dependency_paths(bundle_store, parent_dict, temp_dir)

        with path_util.chdir(temp_dir):
            for (source, target) in pairs:
                path_util.copy(source, target)

            # Only stuff written to the output directory is copied back.
            os.mkdir('output')

            with open('stdout', 'wb') as stdout, open('stderr', 'wb') as stderr:
                process = subprocess.Popen(bundle.command, stdout=stdout,
                                             stderr=stderr, shell=True)

        self.bundle = bundle
        self.process = process
        self.temp_dir = temp_dir

        return True

    def result(self):
        success = self.process.returncode == 0
        return (self.bundle, success, self.temp_dir)

    def kill(self, uuid):
        if self.bundle.uuid == uuid:
            self.process.kill()
            return self.result()
        else:
            return None

    def poll(self):
        self.process.poll()
        if self.process.returncode != None:
            return self.result()
        else:
            return None

    def finalize(self, uuid):
        if self.bundle.uuid == uuid:
            path_util.remove(self.temp_dir)
            return True
        else:
            return False

class PoolMachine(Machine):
    def __init__(self, constructor=LocalMachine, limit=None):
        self.limit = limit
        self.constructor = constructor
        self.machines = {}

    def run_bundle(self, bundle, bundle_store, parent_dict):
        if self.limit and len(self.machines) >= self.limit:
            return False

        machine = self.constructor()
        self.machines[bundle.uuid] = machine
        
        return machine.run_bundle(bundle, bundle_store, parent_dict)

    def kill(self, uuid):
        for key, machine in self.machines.items():
            result = machine.kill(uuid)
            if result:
                del self.machines[key]
                return result
        return None

    def poll(self):
        for key, machine in self.machines.items():
            result = machine.poll()
            # Machine is done running
            if result:
                return result
        return None

    def finalize(self, uuid):
        for key, machine in self.machines.items():
            if machine.finalize(uuid):
                del self.machines[key]
                return True
        return False
