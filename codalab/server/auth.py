'''
AuthHandler encapsulates the logic to authenticate users on the server-side.
'''
import base64
import json
import threading
import urllib
import urllib2


# TODO(sckoo): clean up auth logic across:
#  - this class
#  - CodaLabManager._authenticate
#  - CodaLabManager.client
#  - JsonApiClient._get_access_token
class RestOAuthHandler(threading.local):
    '''
    Handles user authentication with the REST bundle service server. Fetches
    other user records from the local database.

    Inherits from threading.local, which makes all instance attributes thread-local.
    When an OAuthHandler instance is used from a new thread, __init__ will be called
    again, and from thereon all attributes may be different between threads.
    https://hg.python.org/cpython/file/2.7/Lib/_threading_local.py
    '''
    def __init__(self, address, model):
        '''
        address: the address of the server
        model: BundleModel instance
        '''
        super(RestOAuthHandler, self).__init__(model)
        self._address = address

    def generate_token(self, grant_type, username, key):
        '''
        Generate OAuth access token from username/password or from a refresh token.

        If the grant succeeds, the method returns a dictionary of the form:
        { 'token_type': 'Bearer',
          'access_token': <token>,
          'expires_in': <span in seconds>,
          'refresh_token': <token> }
        If the grant fails because of invalid credentials, None is returned.
        '''
        if grant_type == 'credentials':
            return self._make_token_request({
                'grant_type': 'password',
                'username': username,
                'password': key})
            return self._generate_token_from_credentials(username, key)
        if grant_type == 'refresh_token':
            return self._make_token_request({
                'grant_type': 'refresh_token',
                'refresh_token': key})
        raise ValueError("Bad request: grant_type is not valid.")

    def _make_token_request(self, data):
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic ' + base64.b64encode('codalab_cli_client:'),
            'X-Requested-With': 'XMLHttpRequest'}
        request = urllib2.Request(
            self._address + '/rest/oauth2/token',
            headers=headers,
            data=urllib.urlencode(data))
        try:
            response = urllib2.urlopen(request)
            result = json.load(response)
            return result
        except urllib2.HTTPError as e:
            if e.code == 401:
                return None
            raise
