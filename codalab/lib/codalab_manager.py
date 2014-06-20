'''
A CodaLabManager instance stores all the information needed for the CLI, which
is synchronized with a set of JSON files in the CodaLab directory.  It contains
two types of information:

- Configuration (permanent):
  * Aliases: name (e.g., "main") -> address (e.g., http://codalab.org:2800)
- State (transient):
  * address -> username, auth_info
  * session_name -> address, worksheet_uuid

This class provides helper methods that initialize each of the main CodaLab
classes based on the configuration in this file:

  codalab_home: returns the CodaLab home directory
  bundle_store: returns a BundleStore
  cli: returns a BundleCLI
  client: returns a BundleClient
  model: returns a BundleModel
  rpc_server: returns a BundleRPCServer

Imports in this file are deferred to as late as possible because some of these
modules (ex: the model) depend on heavy-weight library imports (ex: sqlalchemy).

As an added benefit of the lazy importing and initialization, note that a config
file that specifies enough information to construct some of these classes is
still valid. For example, the config file for a remote client will not need to
include any server configuration.
'''
import getpass
import json
import os
import sys
import time

from codalab.common import UsageError

def cached(fn):
    def inner(self):
        if fn.__name__ not in self.cache:
            self.cache[fn.__name__] = fn(self)
        return self.cache[fn.__name__]
    return inner

def write_pretty_json(data, path):
    out = open(path, 'w')
    print >>out, json.dumps(data, sort_keys=True, indent=4, separators=(',', ': '))
    out.close()

def read_json_or_die(path):
    try:
        with open(path, 'rb') as f:
            string = f.read()
        return json.loads(string)
    except ValueError:
        print "Invalid JSON in %s:\n%s" % (path, string)
        sys.exit(1)

class CodaLabManager(object):
    def __init__(self):
        self.cache = {}

        # Read config file, creating if it doesn't exist.
        config_path = self.config_path()
        if not os.path.exists(config_path):
            write_pretty_json({
                'cli': {'verbose': False},
                'server': {'class': 'SQLiteModel', 'host': 'localhost', 'port': 2800,
                           'auth': {'class': 'MockAuthHandler'}},
                'aliases': {
                    'dev': 'https://qaintdev.cloudapp.net/bundleservice', # TODO: replace this with something official when it's ready
                    'localhost': 'http://localhost:2800',
                },
            }, config_path)
        self.config = read_json_or_die(config_path)

        # Read state file, creating if it doesn't exist.
        state_path = self.state_path()
        if not os.path.exists(state_path):
            write_pretty_json({
                'auth': {},      # address -> {username, auth_token}
                'sessions': {},  # session_name -> {address, worksheet_uuid, last_modified}
            }, state_path)
        self.state = read_json_or_die(state_path)

        self.clients = {}  # map from address => client

    @cached
    def config_path(self): return os.path.join(self.codalab_home(), 'config.json')

    @cached
    def state_path(self): return os.path.join(self.codalab_home(), 'state.json')

    @cached
    def codalab_home(self):
        from codalab.lib import path_util
        # Default to this directory in the user's home directory.
        # In the future, allow customization based on.
        result = path_util.normalize("~/.codalab")
        path_util.make_directory(result)
        return result

    @cached
    def bundle_store(self):
        codalab_home = self.codalab_home()
        from codalab.lib.bundle_store import BundleStore
        return BundleStore(codalab_home)

    def apply_alias(self, key):
        return self.config['aliases'].get(key, key)

    @cached
    def session_name(self):
        '''
        Return the current session name.
        '''
        if sys.platform == 'win32' and not hasattr(os, 'getppid'):

            from ctypes.wintypes import DWORD, POINTER, ULONG, LONG
            from ctypes import c_char, byref, sizeof, Structure, windll
            from os import getpid

            # See http://msdn2.microsoft.com/en-us/library/ms686701.aspx

            TH32CS_SNAPPROCESS = 0x00000002
            MAX_PATH = 260
            class PROCESSENTRY32(Structure):
                _fields_ = [('dwSize', DWORD),
                            ('cntUsage', DWORD),
                            ('th32ProcessID', DWORD),
                            ('th32DefaultHeapID', POINTER(ULONG)),
                            ('th32ModuleID', DWORD),
                            ('cntThreads', DWORD),
                            ('th32ParentProcessID', DWORD),
                            ('pcPriClassBase', LONG),
                            ('dwFlags', DWORD),
                            ('szExeFile', c_char * MAX_PATH)]

            def getppid():
                '''
                Returns the parent's process id.
                '''
                hProcessSnap = windll.kernel32.CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0)
                try:
                    pid = getpid()
                    pe32 = PROCESSENTRY32()
                    pe32.dwSize = sizeof(PROCESSENTRY32)
                    if windll.kernel32.Process32First(hProcessSnap, byref(pe32)) != 0:
                        while True:
                            if pid == pe32.th32ProcessID:
                                return pe32.th32ParentProcessID
                            if windll.kernel32.Process32Next(hProcessSnap, byref(pe32)) == 0:
                                break
                finally:
                    windll.kernel32.CloseHandle(hProcessSnap)
                return 0

            os.getppid = getppid

        return str(os.getppid())

    @cached
    def session(self):
        '''
        Return the current session.
        '''
        sessions = self.state['sessions']
        name = self.session_name()
        if name not in sessions:
            sessions[name] = {'address': 'local'}  # Default: use local
        return sessions[name]

    @cached
    def model(self):
        '''
        Return a model.  Called by the server.
        '''
        model_class = self.config['server']['class']
        if model_class == 'MySQLModel':
            arguments = ('username', 'password', 'address', 'database')
            kwargs = {arg: self.config['server'][arg] for arg in arguments}
            from codalab.model.mysql_model import MySQLModel
            return MySQLModel(**kwargs)
        if model_class == 'SQLiteModel':
            codalab_home = self.codalab_home()
            from codalab.model.sqlite_model import SQLiteModel
            return SQLiteModel(codalab_home)
        else:
            raise UsageError('Unexpected model class: %s, expected MySQLModel or SQLiteModel' % (model_class,))

    @cached
    def auth_handler(self):
        '''
        Returns a class to authenticate users on the server-side.  Called by the server.
        '''
        auth_config = self.config['server']['auth']
        handler_class = auth_config['class']
        if handler_class == 'OAuthHandler':
            arguments = ('address', 'app_id', 'app_key')
            kwargs = {arg: auth_config[arg] for arg in arguments}
            from codalab.server.auth import OAuthHandler
            return OAuthHandler(**kwargs)
        if handler_class == 'MockAuthHandler':
            from codalab.server.auth import MockAuthHandler
            return MockAuthHandler()
        raise UsageError('Unexpected auth handler class: %s, expected OAuthHandler or MockAuthHandler' % (handler_class,))

    def current_client(self): return self.client(self.session()['address'])
    def client(self, address):
        '''
        Return a client given the address.  Note that this can either be called by the CLI or the server.
        Cache the Client if necessary.
        '''
        if address in self.clients:
            return self.clients[address]
        if address == 'local':
            bundle_store = self.bundle_store()
            model = self.model()
            from codalab.client.local_bundle_client import LocalBundleClient
            self.clients[address] = LocalBundleClient(bundle_store, model)
        else:
            from codalab.client.remote_bundle_client import RemoteBundleClient
            auth = self.state['auth']
            if address not in auth:
                self.authenticate(address)
            self.clients[address] = RemoteBundleClient(address, lambda command: self.authenticate(address))
        return self.clients[address]

    def authenticate(self, address):
        '''
        Authenticate with the given address. This will prompt user for password
        unless valid credentials are already available. Client state will be
        updated if new tokens are generated.

        Returns an access token.
        '''
        def _cache_token(token_info, username=None):
            '''
            Helper to update state with new token info and optional username.
            Returns the latest access token.
            '''
            token_info['expires_at'] = time.time() + float(token_info['expires_in']) - 60.0
            del token_info['expires_in']
            auth['token_info'] = token_info
            if username is not None:
                auth['username'] = username
            self.save_state()
            return token_info['access_token']

        # Check the cache for a valid token
        from codalab.client.remote_bundle_client import RemoteBundleClient
        auth_info = self.state['auth'].get(address, {})
        if 'token_info' in auth_info:
            token_info = auth_info['token_info']
            expires_at = token_info.get('expires_at', 0.0)
            if expires_at > time.time():
                # Token is usable but check if it's nearing expiration
                if expires_at >= (time.time() + 900.0):
                    return token_info['access_token']
                # Try to refresh token
                remote_client = RemoteBundleClient(address, lambda command: None)
                token_info = remote_client.login('refresh_token',
                                                 token_info['refresh_token'],
                                                 auth_info['username'])
                if token_info is not None:
                    return _cache_token(token_info)

        # If we get here, a valid token is not already available.
        auth = self.state['auth'][address] = {}
        print 'Requesting access at %s' % address
        print 'Username: ',
        username = sys.stdin.readline().rstrip()
        password = getpass.getpass()
        remote_client = RemoteBundleClient(address, lambda command: None)
        token_info = remote_client.login('credentials', username, password)
        if token_info is None:
            raise UsageError("Invalid username or password")
        return _cache_token(token_info, username)

    def get_current_worksheet_uuid(self):
        '''
        Return a worksheet_uuid for the current worksheet, or None if there is none.

        This method uses the current parent-process id to return the same result
        across multiple invocations in the same shell.
        '''
        session = self.session()
        client = self.client(session['address'])
        worksheet_uuid = session.get('worksheet_uuid', None)
        return (client, worksheet_uuid)

    def set_current_worksheet_uuid(self, client, worksheet_uuid):
        '''
        Set the current worksheet to the given worksheet_uuid.
        '''
        session = self.session()
        session['address'] = client.address
        if worksheet_uuid:
            session['worksheet_uuid'] = worksheet_uuid
        else:
            del session['worksheet_uuid']
        self.save_state()

    def save_state(self):
        write_pretty_json(self.state, self.state_path())
