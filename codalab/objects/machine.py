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

    def run_bundle(self, bundle_store, parent_dict, temp_dir):
        '''
        Attempts to begin bundle execution.
        Returns True/False, indicating success.
        '''
        raise NotImplementedError

    def poll(self):
        '''
        Checks to see if anything is done executing.
        Returns (bundle, success flag, directory with output) or None
        '''
        raise NotImplementedError
    def finalize(self, uuid):
        '''
        Removes all temporary data.
        Returns True/False
        '''
        raise NotImplementedError
    def kill(self, uuid):
        '''
        Stops process.
        Returns (bundle, success flag, directory with output) or None
        '''
        raise NotImplementedError
