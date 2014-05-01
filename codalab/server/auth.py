'''
AuthHandler encapsulates the logic to authenticate users on the server-side.
'''
import json
import time
import urllib
import urllib2
from base64 import encodestring

from codalab.common import UsageError

class User(object):
    '''
    Defines a registered user with a unique name and a unique (int) identifier.
    '''
    def __init__(self, name, unique_id):
        self.name = name
        self.unique_id = unique_id


class MockAuthHandler(object):
    '''
    A mock handler, which makes it easy to run a server when no real
    authentication is required. The implementation is such that this
    handler will accept any combination of username and password, but
    it will always resolve to the same user: User('root', 0).
    '''
    def __init__(self):
        self._user = User('root', 0)

    def generate_token(self, grant_type, username, key):
        '''
        Always returns token information.
        '''
        return {
            'token_type': 'Bearer',
            'access_token': '__mock_token__',
            'expires_in': 3600 * 24 * 365,
            'refresh_token': '__mock_token__',
        }

    def validate_token(self, token):
        '''
        Always returns True. The specified token is ignored.
        '''
        return True


    def get_users(self, key_type, keys):
        '''
        Resolves user names (key_type='names') or user IDs (key_type='ids') to
        corresponding User objects.

        key_type: The type of input keys: names or ids.
        keys: The set of names/ids to resolve.

        Returns a dictionary where keys are keys input to this method and
        values are either a User object or None if the key does not have
        a matching user (either the user does not exist or exists but is
        not active).
        '''
        if key_type == 'names':
            return {'name': self._user if key == 'root' else None for key in keys}
        if key_type == 'ids':
            return {'id': self._user if key == 0 else None for key in keys}
        raise ValueError('Invalid key_type')

    def current_user(self):
        return self._user


class OAuthHandler(object):
    '''
    Handles user authentication with an OAuth authorization server.
    '''
    def __init__(self, address, app_id, app_key):
        '''
        address: the address of the OAuth authorization server
                 (e.g. https://www.codalab.org).
        app_id: OAuth application identifier.
        app_key: OAuth application key.
        '''
        self._address = address
        self._app_id = app_id
        self._app_key = app_key
        self.min_username_length = 1
        self.min_key_length = 6
        self._user = None
        self._access_token = None
        self._expires_at = 0.0

    def _get_token_url(self):
        return "{0}/clients/token/".format(self._address)

    def _get_validation_url(self):
        return "{0}/clients/validation/".format(self._address)

    def _get_user_info_url(self):
        return "{0}/clients/info/".format(self._address)

    def _generate_new_token(self, username, password):
        '''
        Get OAuth2 token using Resource Owner Password Credentials Grant.
        '''
        appname = 'cli_client_{0}'.format(username)
        headers = {'Authorization': 'Basic {0}'.format(encodestring('%s:' % appname).replace('\n', ''))}
        data = [('grant_type', 'password'),
                ('username', username),
                ('password', password)]
        request = urllib2.Request(self._get_token_url(), urllib.urlencode(data, True), headers)
        try:
            response = urllib2.urlopen(request)
            token_info = json.load(response)
            return token_info
        except urllib2.HTTPError as e:
            if e.code == 400:
                return None
            raise e

    def _refresh_token(self, username, refresh_token):
        '''
        Refresh OAuth2 token.
        '''
        appname = 'cli_client_{0}'.format(username)
        headers = {'Authorization': 'Basic {0}'.format(encodestring('%s:' % appname).replace('\n', ''))}
        data = [('grant_type', 'refresh_token'),
                ('refresh_token', refresh_token)]
        request = urllib2.Request(self._get_token_url(), urllib.urlencode(data, True), headers)
        try:
            response = urllib2.urlopen(request)
            token_info = json.load(response)
            return token_info
        except urllib2.URLError as e:
            if e.code == 400:
                return None
            raise e

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
            if len(username) < self.min_username_length or len(key) < self.min_key_length:
                raise UsageError("Invalid username or password.")
            return self._generate_new_token(username, key)
        if grant_type == 'refresh_token':
            return self._refresh_token(username, key)
        raise ValueError("Bad request: grant_type is not valid.")

    def _generate_app_token(self):
        '''
        Helper to authenticate this app with the OAuth authorization server.
        '''
        app_sig = '%s:%s' % (self._app_id, self._app_key)
        headers = {'Authorization': 'Basic {0}'.format(encodestring(app_sig).replace('\n', ''))}
        data = [('grant_type', 'client_credentials'),
                ('scope', 'token-validation')]
        request = urllib2.Request(self._get_token_url(), urllib.urlencode(data, True), headers)
        response = urllib2.urlopen(request)
        token_info = json.load(response)
        self._access_token = token_info['access_token']
        self._expires_at = (time.time() + float(token_info['expires_in'])) - 60.0

    def validate_token(self, token):
        '''
        Validate OAuth authorization information.

        token: The token to validate. This value may be None to indicate that no
            Authorization header was specified. In such case this method will
            return true and set the current user to None.

        Returns True if the request is authorized to proceed. The current_user
            property of this class provides the user associated with the token.
        '''
        self._user = None
        if token is None:
            return True
        if len(token) <= 0:
            return False

        if self._access_token is None or self._expires_at < time.time():
            self._generate_app_token()
        headers = {'Authorization': 'Bearer {0}'.format(self._access_token)}
        data = [('token', token)]
        request = urllib2.Request(self._get_validation_url(), urllib.urlencode(data, True), headers)
        response = urllib2.urlopen(request)
        result = json.load(response)
        status_code = result['code'] if 'code' in result else 500
        if status_code == 200:
            self._user = User(result['user']['name'], int(result['user']['id']))
            return True
        elif status_code == 403 or status_code == 404:
            return False # 'User credentials are not valid'
        else:
            return False # 'The token translation failed.'

    def get_users(self, key_type, keys):
        '''
        Resolves user names (key_type='names') or user IDs (key_type='ids') to
        corresponding User objects.

        key_type: The type of input keys: names or ids.
        keys: The set of names/ids to resolve.

        Returns a dictionary where keys are keys input to this method and
        values are either a User object or None if the key does not have
        a matching user (either the user does not exist or exists but is
        not active).
        '''
        if key_type not in ('names', 'ids'):
            raise ValueError('Invalid key_type')
        if self._access_token is None or self._expires_at < time.time():
            self._generate_app_token()
        headers = {'Authorization': 'Bearer {0}'.format(self._access_token)}
        request = urllib2.Request(self._get_user_info_url(),
                                  urllib.urlencode([(key_type, keys)], True),
                                  headers)
        response = urllib2.urlopen(request)
        result = json.load(response)
        status_code = result['code'] if 'code' in result else 500
        user_dict = None
        if status_code == 200:
            user_dict = {}
            key_type_key = 'name' if key_type == 'names' else 'id'
            for user in result['users']:
                key = user[key_type_key]
                if 'active' in user and user['active'] == True:
                    user_dict[key] = User(user['name'], user['id'])
                else:
                    user_dict[key] = None
        return user_dict

    def current_user(self):
        '''
        Returns the current user as set by validate_token.
        '''
        return self._user




