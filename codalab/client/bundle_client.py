'''
Abstract base class that describes the client interface for interacting with
the CodaLab bundle system.

There are three categories of BundleClient commands:
  - Commands that create and edit bundles: upload, make, run and update.
  - Commands for browsing bundles: info, ls, cat, search, and wait.
  - Various utility commands for pulling bundles back out of the system.

There are a couple of implementations of this class:
  - LocalBundleClient - interacts directly with a BundleStore and BundleModel.
  - RemoteBundleClient - shells out to a BundleRPCServer to implement its API.
'''
# TODO: We should probably implement grep at some point. grep will take a
# target (like the target passed to ls or cat) and a list of command-line args.
# The RemoteBundleClient implementation of grep will have to use the FileServer
# file-handle API to stream the results back.
import time
from sys import stdout

class BundleClient(object):
    # See LocalBundleClient for most of the functions and RemoteBundleClient
    # for some subset of them.

    def login(self, grant_type, username, key):
        '''
        Generate OAuth access token from username/password or from a refresh token.

        grant_type: Type of grant requested: 'credentials' or 'refresh_token'.
        username: Name of user to authenticate.
        key: User's secret which is a password for the 'credentials' grant type
            or a refresh token for the 'refresh_token' grant type.

        If the grant succeeds, the method returns a dictionary of the form:
        { 'token_type': 'Bearer',
          'access_token': <token>,
          'expires_in': <span in seconds>,
          'refresh_token': <token> }
        If the grant fails because of invalid credentials, None is returned.
        '''
        if not hasattr(self, 'auth_handler'):
            raise NotImplementedError
        return self.auth_handler.generate_token(grant_type, username, key)
