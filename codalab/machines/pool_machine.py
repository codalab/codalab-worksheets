import os
import subprocess

from codalab.lib import (
  canonicalize,
  path_util,
)

from codalab.objects.machine import Machine
from codalab.machines.local_machine import LocalMachine

class PoolMachine(Machine):
    '''
    Maintains a pool of up to |limit| machines and dispatches all requests to
    those machines.
    '''
    def __init__(self, construct_func, limit):
        '''
        construct_func: call to return an instance of a machine
        limit: maximum number of processes to run'
        '''
        self.construct_func = construct_func
        self.limit = limit
        self.machines = {}

    def start_bundle(self, bundle, bundle_store, parent_dict):
        if self.limit and len(self.machines) >= self.limit:
            return False

        machine = self.construct_func()
        self.machines[bundle.uuid] = machine
        
        return machine.start_bundle(bundle, bundle_store, parent_dict)

    def kill_bundle(self, uuid):
        for key, machine in self.machines.items():
            if machine.kill_bundle(uuid):
                return True
        return False

    def poll(self):
        for key, machine in self.machines.items():
            result = machine.poll()
            # Machine is done with the run
            if result:
                return result
        return None

    def finalize_bundle(self, uuid):
        for key, machine in self.machines.items():
            if machine.finalize_bundle(uuid):
                del self.machines[key]
                return True
        return False

