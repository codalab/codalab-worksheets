"""
AuthHandler encapsulates the logic to authenticate users on the server-side.
"""
import base64
import json

import urllib.request
import urllib.parse
import urllib.error
from codalab.common import LoginPermissionError, urlopen_with_retry


# TODO(sckoo): clean up auth logic across:
#  - this class
#  - CodaLabManager._authenticate
#  - CodaLabManager.client
#  - JsonApiClient._get_access_token
class RestOAuthHandler(object):
    """
    Handles user authentication with the REST bundle service server. Fetches
    other user records from the local database.
    """

    def __init__(self, address, extra_headers={}):
        """
        address: the address of the server
        model: BundleModel instance
        """
        self._address = address
        self._extra_headers = extra_headers

    def generate_token(self, grant_type, username, key):
        """
        Generate OAuth access token from username/password or from a refresh token.

        If the grant succeeds, the method returns a dictionary of the form:
        { 'token_type': 'Bearer',
          'access_token': <token>,
          'expires_in': <span in seconds>,
          'refresh_token': <token> }
        If the grant fails because of invalid credentials, None is returned.
        """
        if grant_type == 'credentials':
            return self._make_token_request(
                {'grant_type': 'password', 'username': username, 'password': key}
            )
        if grant_type == 'refresh_token':
            return self._make_token_request({'grant_type': 'refresh_token', 'refresh_token': key})
        raise ValueError("Bad request: grant_type is not valid.")

    def _make_token_request(self, data):
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic ' + base64.b64encode(b'codalab_cli_client:').decode('utf-8'),
            'X-Requested-With': 'XMLHttpRequest',
        }
        headers.update(self._extra_headers)
        request = urllib.request.Request(
            self._address + '/rest/oauth2/token',
            headers=headers,
            data=urllib.parse.urlencode(data).encode('utf-8'),
        )
        try:
            response = urlopen_with_retry(request)
            result = json.loads(response.read().decode())
            return result
        except urllib.error.HTTPError as e:
            if e.code == 401:
                return None
            elif e.code == 404:
                raise LoginPermissionError("Invalid username or password.")
            raise e
