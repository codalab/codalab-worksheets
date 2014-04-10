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
import json
import os
import sys
import base64
import getpass
import urllib
import urllib2

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
                'server': {'class': 'SQLiteModel', 'host': '', 'port': 2800},
                'aliases': {
                    'dev': 'https://qaintdev.cloudapp.net', # TODO: replace this with something official when it's ready
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
        if sys.platform == 'win32':
            return str(1)  # TODO: get some identifier of the shell
        else:
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
            # Authentication
            # TODO: check if token expired
            auth = self.state['auth']
            if address not in auth:
                self.authenticate(address)
            self.clients[address] = RemoteBundleClient(address, auth[address]['token_info'])
        return self.clients[address]

    def authenticate(self, address):
        '''
        Authenticate with the given address.
        Prompt user for password.
        Save tokens to state.
        '''
        # Get user information
        if 'qaintdev' not in address:  # TODO: temporary hack to bypass authentication since it doesn't exist
            auth = self.state['auth'][address] = {}
            auth['username'] = 'pliang'
            auth['token_info'] = {}
            self.save_state()
            return
        get_token_url = address + '/clients/token/'  # TODO: standardize on location of auth server with respect to bundle service
        print 'Requesting access at %s' % get_token_url
        print 'Username: ',
        username = sys.stdin.readline().rstrip()
        password = getpass.getpass()

        # Get OAuth2 token using Resource Owner Password Credentials Grant 
        appname = 'cli_client_{0}'.format(username)
        headers = { 
            'Authorization': 'Basic {0}'.format(base64.encodestring('%s:' % appname).replace('\n',''))
        }
        data = [
            ('grant_type', 'password'),
            ('username', username),
            ('password', password)
        ]
        #print get_token_url
        #print urllib.urlencode(data, True)
        #print headers
        request = urllib2.Request(get_token_url, urllib.urlencode(data, True), headers)
        try:
            response = urllib2.urlopen(request)
            token_info = json.load(response)
            print 'Token type: %s' % token_info['token_type']
            print 'Access token %s' % token_info['access_token']
            print 'Refresh token %s' % token_info['refresh_token']
            print 'Expires in %s' % token_info['expires_in']
            auth = self.state['auth'][address] = {}
            auth['username'] = username
            auth['token_info'] = token_info
            self.save_state()
        except urllib2.HTTPError as e:
            print 'Couldn\'t authenticate.'
            print e
            sys.exit(1)

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
