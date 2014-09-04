import os
import subprocess

from codalab.lib import (
  canonicalize,
  path_util,
)

from codalab.machines import (
  local_machine,
  remote_machine,
)

from codalab.objects.machine import Machine

'''
Main entry point for creating a machine based on a configuration.
'''
def parse_machine(config, name):
    if name == 'local':
        return local_machine.LocalMachine()
    if name in config:
        sub_config = config[name]
        if 'host' not in sub_config: sub_config['host'] = name
        return PoolMachine(sub_config)
    return None

'''
Used to create machine instances and keep track of general statistics
'''
class MachineSpec:
    def __init__(self, construct_func, max_instances):
        self.construct_func = construct_func
        self.num_instances = 0
        self.max_instances = max_instances

class PoolMachine(Machine):
    '''
    Maintains a pool of machines and dispatches all requests to
    those machines.
    '''
    def __init__(self, config):
        '''
        config is a mapping from type to a dict with the following fields:
        - max_instances: number of instances of this machine we can start up
        - children: mapping from type to 
        - Fields specific to RemoteMachine: user, host (defaults to key), docker_image
        '''

        # Create a machine pool
        machine_specs = []
        def create_specs(config):
            max_instances = config.get('max_instances', 1)
            verbose = config.get('verbose', 1)
            if 'children' not in config:
                construct_func = lambda : remote_machine.RemoteMachine(config)
                print 'PoolMachine: %s (%d)' % (config.get('host'), max_instances)
                machine_specs.append(MachineSpec(construct_func, max_instances))
            else:
                for name, sub_config in config['children'].items():
                    # Set defaults
                    if 'host' not in sub_config: sub_config['host'] = name
                    if 'max_instances' not in sub_config: sub_config['max_instances'] = max_instances
                    if 'verbose' not in sub_config: sub_config['verbose'] = verbose
                    create_specs(sub_config)
        create_specs(config)

        self.machine_specs = machine_specs
        self.bundles = {}  # map from bundle uuid to (machine_spec, machine)

    def start_bundle(self, bundle, bundle_store, parent_dict):
        # Choose a machine spec (currently just loop through and see if there are any free).
        # TODO: do this based on further knowledge of the machines (see how much availability there is)
        for spec in self.machine_specs:
            if spec.num_instances >= spec.max_instances: continue

            # Construct a new machine and try to start the bundle
            machine = spec.construct_func()
            success = machine.start_bundle(bundle, bundle_store, parent_dict)
            if success:
                spec.num_instances += 1
                self.bundles[bundle.uuid] = (spec, machine)
            return success

        return False

    def kill_bundle(self, uuid):
        if uuid not in self.bundles:
            print 'ERROR: bundle %s not on a machine, skipping...' % uuid
            return False
        spec, machine = self.bundles.get(uuid)
        return machine.kill_bundle(uuid)

    def poll(self):
        for (spec, machine) in self.bundles.values():
            result = machine.poll()
            # Machine is done with the run
            if result: return result
        return None

    def finalize_bundle(self, uuid):
        if uuid not in self.bundles:
            print 'ERROR: bundle %s not on a machine, skipping...' % uuid
            return False
        spec, machine = self.bundles.pop(uuid)
        spec.num_instances -= 1
        return machine.finalize_bundle(uuid)
