'''
Machine is a class that manages execution of bundle(s) that need to be run.
'''

class Machine(object):
    def start_bundle(self, bundle, bundle_store, parent_dict):
        '''
        Attempts to begin bundle execution.
        Returns True/False, indicating whether the bundle was started.
        '''
        raise NotImplementedError

    def get_bundle_statuses(self):
        '''
        Checks the status of bundles.
        Returns a list of bundle statuses (dicts).
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
