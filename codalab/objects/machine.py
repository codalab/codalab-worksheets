'''
Machine is a class that manages execution of bundle(s) that need to be run.

Subclasses implement local execution (in a separate process) and
remote execution (through ssh).

Does not interact with bundle model.  Uses BundleStore to resolve dependencies.
'''

class Machine(object):
    '''
    Utility for running bundle commands.
    '''

    def start_bundle(self, bundle, bundle_store, parent_dict):
        '''
        Attempts to begin bundle execution.
        Returns True/False, indicating whether the bundle was started.
        '''
        raise NotImplementedError

    def poll(self):
        '''
        Checks to see if any run bundle has finished.
        Returns (bundle, success flag, directory with output) if one has or None
        '''
        raise NotImplementedError

    def finalize_bundle(self, uuid):
        '''
        Removes all temporary data.
        Returns True/False
        '''
        raise NotImplementedError

    def kill_bundle(self, uuid):
        '''
        Stops process associated with the bundle.
        Returns (bundle, success flag, directory with output) or None
        '''
        raise NotImplementedError
