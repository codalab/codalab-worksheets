import os
import subprocess

from codalab.lib import (
  canonicalize,
  path_util,
)

from codalab.objects.machine import Machine
from codalab.machines.local_machine import LocalMachine

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

