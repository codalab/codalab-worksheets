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
import psutil
import tempfile
import textwrap
from distutils.util import strtobool
from urlparse import urlparse

from codalab.client import is_local_address
from codalab.client.json_api_client import JsonApiClient
from codalab.common import UsageError, PermissionError, precondition
from codalab.server.auth import User
from codalab.lib.bundle_store import (
    MultiDiskBundleStore,
)
from codalab.lib.crypt_util import get_random_string
from codalab.lib.download_manager import DownloadManager
from codalab.lib.emailer import SMTPEmailer, ConsoleEmailer
from codalab.lib.print_util import pretty_print_json
from codalab.lib.upload_manager import UploadManager
from codalab.lib import formatting
from codalab.model.worker_model import WorkerModel


def cached(fn):
    def inner(self):
        if fn.__name__ not in self.cache:
            self.cache[fn.__name__] = fn(self)
        return self.cache[fn.__name__]
    return inner

def write_pretty_json(data, path):
    with open(path, 'w') as f:
        pretty_print_json(data, f)

def read_json_or_die(path):
    try:
        with open(path, 'rb') as f:
            string = f.read()
        return json.loads(string)
    except ValueError as e:
        print "Invalid JSON in %s:\n%s" % (path, string)
        print e
        sys.exit(1)

def prompt_bool(prompt, default=None):
    if default is None:
        prompt = "%s [yn] " % prompt
    elif default is True:
        prompt = "%s [Yn] " % prompt
    elif default is False:
        prompt = "%s [yN] " % prompt
    else:
        raise ValueError("default must be None, True, or False")

    while True:
        response = raw_input(prompt).strip()
        if default is not None and len(response) == 0:
            return default
        try:
            return bool(strtobool(response))
        except ValueError:
            print "Please enter y(es) or n(o)."
            continue

def prompt_str(prompt, default=None):
    if default is not None:
        prompt = "%s [default: %s] " % (prompt, default)
    else:
        prompt = "%s " % (prompt,)

    while True:
        response = raw_input(prompt).strip()
        if len(response) > 0:
            return response
        elif default is not None:
            return default

def print_block(text):
    print textwrap.dedent(text)

class CodaLabManager(object):
    '''
    temporary: don't use config files
    '''
    def __init__(self, temporary=False, config=None, clients=None):
        self.cache = {}
        self.temporary = temporary

        if self.temporary:
            self.config = config
            self.state = {'auth': {}, 'sessions': {}}
            self.clients = clients
            return

        # Read config file, creating if it doesn't exist.
        if not os.path.exists(self.config_path):
            self.init_config()
        self.config = read_json_or_die(self.config_path)

        # Substitute environment variables
        codalab_cli = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        def replace(x):
            if isinstance(x, basestring):
                return x.replace('$CODALAB_CLI', codalab_cli)
            if isinstance(x, dict):
                return dict((k, replace(v)) for k, v in x.items())
            return x
        self.config = replace(self.config)

        # Read state file, creating if it doesn't exist.
        if not os.path.exists(self.state_path):
            write_pretty_json({
                'auth': {},      # address -> {username, auth_token}
                'sessions': {},  # session_name -> {address, worksheet_uuid, last_modified}
            }, self.state_path)
        self.state = read_json_or_die(self.state_path)

        self.clients = {}  # map from address => client

    def init_config(self, dry_run=False):
        '''
        Initialize configuration for a simple client.
        For the server, see config_gen in the codalab-worksheets repo.
        '''
        print_block(r"""
           ____          _       _            _
         / ____|___   __| | __ _| |     T T  | |__
        | |    / _ \ / _` |/ _` | |     |o|  | '_ \
        | |___| (_) | (_| | (_| | |___ /__o\ | |_) |
         \_____\___/ \__,_|\__,_|_____/_____\|_.__/

        Welcome to the CodaLab CLI!

        Your CodaLab configuration and state will be stored in: {0.codalab_home}
        """.format(self))

        main_bundle_service = 'https://worksheets.codalab.org/bundleservice'

        config = {
            'cli': {
                'default_address': main_bundle_service,
                'verbose': 1,
            },
            'server': {
                'host': 'localhost',
                'port': 2800,
                'rest_host': 'localhost',
                'rest_port': 2900,
                'class': 'MySQLModel',
                'engine_url': 'mysql://codalab@localhost:3306/codalab_bundles',
                'auth': {
                    'class': 'RestOAuthHandler'
                },
                'verbose': 1,
            },
            'aliases': {
                'main': main_bundle_service,
                'localhost': 'http://localhost:2800',
            },
            'workers': {
                'default_docker_image': 'codalab/ubuntu:1.9',
            }
        }

        # Generate secret key
        config['server']['secret_key'] = get_random_string(
            48, "=+/abcdefghijklmnopqrstuvwxyz"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

        if not dry_run:
            write_pretty_json(config, self.config_path)

        return config

    @property
    @cached
    def config_path(self):
        return os.getenv('CODALAB_CONFIG', 
                         os.path.join(self.codalab_home, 'config.json'))

    @property
    @cached
    def state_path(self):
        return os.getenv('CODALAB_STATE',
                         os.path.join(self.codalab_home, 'state.json'))

    @property
    @cached
    def codalab_home(self):
        from codalab.lib import path_util
        # Default to this directory in the user's home directory.
        # In the future, allow customization based on.
        home = os.getenv('CODALAB_HOME', '~/.codalab')
        home = path_util.normalize(home)
        path_util.make_directory(home)
        # Global setting!  Make temp directory the same as the bundle store
        # temporary directory.  The default /tmp generally doesn't have enough
        # space.
        # TODO: Fix this, this is bad
        tempfile.tempdir = os.path.join(home, MultiDiskBundleStore.MISC_TEMP_SUBDIRECTORY)
        return home

    @property
    @cached
    def worker_socket_dir(self):
        from codalab.lib import path_util
        directory = os.path.join(self.codalab_home, 'worker_sockets')
        path_util.make_directory(directory)
        return directory

    @cached
    def bundle_store(self):
        """
        Returns the bundle store backing this CodaLab instance. The type of the store object
        depends on what the user has configured, but if no bundle store is configured manually then it defaults to a
        MultiDiskBundleStore.
        """
        store_type = self.config.get('bundle_store', 'MultiDiskBundleStore')
        if store_type == MultiDiskBundleStore.__name__:
            return MultiDiskBundleStore(self.codalab_home)
        else:
            print >>sys.stderr, "Invalid bundle store type \"%s\"", store_type
            sys.exit(1)

    def apply_alias(self, key):
        return self.config['aliases'].get(key, key)

    @cached
    def session_name(self):
        '''
        Return the current session name.
        '''
        if self.temporary:
            return 'temporary'

        # If specified in the environment, then return that.
        session = os.getenv('CODALAB_SESSION')
        if session:
            return session

        # Otherwise, go up process hierarchy to the *highest up shell* out of
        # the consecutive shells.  Include Python and Ruby so we can script from inside them.
        #   cl bash python bash screen bash gnome-terminal init
        #                  ^
        #                  | return this
        # This way, it's easy to write scripts that have embedded 'cl' commands
        # which modify the current session.
        process = psutil.Process(os.getppid())
        session = 'top'
        max_depth = 10
        while process and max_depth:
            name = os.path.basename(process.cmdline()[0])
            if name not in ('sh', 'bash', 'csh', 'tcsh', 'zsh', 'python', 'ruby'):
                break
            session = str(process.pid)
            process = process.parent()
            max_depth = max_depth - 1
        return session

    @cached
    def session(self):
        '''
        Return the current session.
        '''
        sessions = self.state['sessions']
        name = self.session_name()
        if name not in sessions:
            # New session: set the address and worksheet uuid to the default (local if not specified)
            cli_config = self.config.get('cli', {})
            address = cli_config.get('default_address', 'local')
            worksheet_uuid = cli_config.get('default_worksheet_uuid', '')
            sessions[name] = {'address': address, 'worksheet_uuid': worksheet_uuid}
        return sessions[name]

    @cached
    def default_user_info(self):
        info = self.config['server'].get('default_user_info', {'time_quota': '1y', 'disk_quota': '1t'})
        return {
            'time_quota': formatting.parse_duration(info['time_quota']),
            'disk_quota': formatting.parse_size(info['disk_quota'])
        }

    @cached
    def model(self):
        """
        Return a model.  Called by the server.
        """
        model_class = self.config['server']['class']
        model = None
        if model_class == 'MySQLModel':
            from codalab.model.mysql_model import MySQLModel
            model = MySQLModel(engine_url=self.config['server']['engine_url'], default_user_info=self.default_user_info())
        elif model_class == 'SQLiteModel':
            from codalab.model.sqlite_model import SQLiteModel
            # Patch for backwards-compatibility until we have a cleaner abstraction around config
            # that can update configs to newer "versions"
            engine_url = self.config['server'].get('engine_url', "sqlite:///{}".format(os.path.join(self.codalab_home, 'bundle.db')))
            model = SQLiteModel(engine_url=engine_url, default_user_info=self.default_user_info())
        else:
            raise UsageError('Unexpected model class: %s, expected MySQLModel or SQLiteModel' % (model_class,))
        model.root_user_id = self.root_user_id()
        model.system_user_id = self.system_user_id()
        return model

    @cached
    def worker_model(self):
        # Whether the file system is shared between the worker and the bundle
        # service. Note, that the file system is considered to be shared only if
        # the worker is running as the root user.
        shared_file_system = self.config['workers'].get('shared_file_system', False)
        return WorkerModel(self.model().engine, self.worker_socket_dir, shared_file_system)

    @cached
    def upload_manager(self):
        return UploadManager(self.model(), self.bundle_store())

    @cached
    def download_manager(self):
        return DownloadManager(self.model(), self.worker_model(), self.bundle_store())

    def auth_handler(self, mock=False):
        '''
        Returns a class to authenticate users on the server-side.  Called by the server.
        '''
        auth_config = self.config['server']['auth']
        handler_class = auth_config['class']

        if mock or handler_class == 'MockAuthHandler':
            return self.mock_auth_handler()
        if handler_class == 'RestOAuthHandler':
            return self.rest_oauth_handler()
        raise UsageError('Unexpected auth handler class: %s, expected RestOAuthHandler or MockAuthHandler' % (handler_class,))

    @cached
    def mock_auth_handler(self):
        from codalab.server.auth import MockAuthHandler
        # Just create one user corresponding to the root
        users = [User(self.root_user_name(), self.root_user_id())]
        return MockAuthHandler(users)

    @cached
    def rest_oauth_handler(self):
        from codalab.server.auth import RestOAuthHandler
        address = 'http://%s:%d' % (self.config['server']['rest_host'],
                                    self.config['server']['rest_port'])
        return RestOAuthHandler(address, self.model())

    @property
    @cached
    def emailer(self):
        if 'email' in self.config:
            return SMTPEmailer(
                host=self.config['email']['host'],
                user=self.config['email']['user'],
                password=self.config['email']['password'],
                use_tls=True,
                default_sender='CodaLab <noreply@codalab.org>',
                server_email='noreply@codalab.org',
            )
        else:
            return ConsoleEmailer()

    def root_user_name(self):
        return self.config['server'].get('root_user_name', 'codalab')

    def root_user_id(self):
        return self.config['server'].get('root_user_id', '0')

    def system_user_id(self):
        return self.config['server'].get('system_user_id', '-1')

    # TODO(sckoo): remove when REST migration complete
    def derive_rest_address(self, address):
        """
        Given bundle service address, return corresponding REST address
        using manual translations (described in the comments below).
        Temporary hack to ease transition to REST API.
        """
        o = urlparse(address)
        if is_local_address(address):
            # local => http://localhost:<rest_port>
            precondition('server' in self.config and
                         'rest_host' in self.config['server'] and
                         'rest_port' in self.config['server'],
                         'Working on local now requires running a local '
                         'server, please configure "rest_host" and '
                         '"rest_port" under "server" in your config.json.')
            address = 'http://{rest_host}:{rest_port}'.format(**self.config['server'])
        elif (o.hostname == 'localhost' and
                      'server' in self.config and
                      'port' in self.config['server'] and
                      'rest_port' in self.config['server'] and
                      o.port == self.config['server']['port']):
            # http://localhost:<port> => http://localhost:<rest_port>
            # Note that this does not affect the address of the NLP
            # CodaLab instance behind an SSH tunnel
            address = address.replace(str(self.config['server']['port']),
                                      str(self.config['server']['rest_port']))
        elif o.hostname == 'localhost' and o.port == 3800:
            # Hard-coded for test-cli, which uses these ports for aux servers
            address = address.replace('3800', '3900')
        else:
            # http://worksheets.codalab.org/bundleservice => http://worksheets.codalab.org
            address = address.replace('/bundleservice', '')
        return address

    # TODO(sckoo): clean up backward compatibility hacks when REST API complete
    def current_client(self, use_rest=False):
        return self.client(self.session()['address'], use_rest=use_rest)

    # TODO(sckoo): clean up backward compatibility hacks when REST API complete
    def client(self, address, is_cli=True, use_rest=False):
        '''
        Return a client given the address.  Note that this can either be called
        by the CLI (is_cli=True) or the server (is_cli=False).
        If called by the CLI, we don't need to authenticate.
        Cache the Client if necessary.
        '''
        # FIXME(sckoo): temporary hack
        # When BundleCLI is no longer dependent on RemoteBundleClient
        # or LocalBundleClient, simplify everything to only using
        # JsonApiClient, and remove all use_rest kwargs.
        # Users should also then update their aliases to point at the
        # correct addresses of the REST servers.
        # Use non-rest address as key in credential cache to prevent duplicates
        auth_cache_key = address
        if use_rest:
            address = self.derive_rest_address(address)

        # Return cached client
        # Additionally requires that the client in the cache is the correct class.
        # This is necessary to prevent key collisions for the case where the
        # REST client and the old BundleClient both have the same address.
        # TODO(sckoo): Remove second condition when REST API complete
        if address in self.clients and \
                (use_rest == isinstance(self.clients[address], JsonApiClient)):
            return self.clients[address]

        # Create new client
        if use_rest:
            # Create RestOAuthHandler that authenticates directly with
            # OAuth endpoints on the REST server
            from codalab.server.auth import RestOAuthHandler
            auth_handler = RestOAuthHandler(address, None)

            # Create JsonApiClient with a callback to get access tokens
            client = JsonApiClient(
                address, lambda: self._authenticate(auth_cache_key, auth_handler))
        elif is_local_address(address):
            # if local force mockauth or if local server use correct auth
            bundle_store = self.bundle_store()
            model = self.model()
            worker_model = self.worker_model()
            upload_manager = self.upload_manager()
            download_manager = self.download_manager()
            auth_handler = self.auth_handler(mock=is_cli)

            from codalab.client.local_bundle_client import LocalBundleClient
            client = LocalBundleClient(address, bundle_store, model, worker_model, upload_manager, download_manager, auth_handler, self.cli_verbose)
            if is_cli:
                # Set current user
                access_token = self._authenticate(auth_cache_key, client.auth_handler)
                auth_handler.validate_token(access_token)
        else:
            from codalab.client.remote_bundle_client import RemoteBundleClient

            client = RemoteBundleClient(address, lambda a_client: self._authenticate(auth_cache_key, a_client), self.cli_verbose)
            self._authenticate(auth_cache_key, client)

        # Cache and return client
        self.clients[address] = client
        return client

    @property
    def cli_verbose(self):
        return self.config.get('cli', {}).get('verbose')

    def _authenticate(self, cache_key, auth_handler):
        """
        Authenticate with the given client. This will prompt user for password
        unless valid credentials are already available. Client state will be
        updated if new tokens are generated.

        :param cache_key: key by which to cache the access token, typically
                          the address of the CodaLab instance
        :param auth_handler: AuthHandler through which to authenticate
        :return: access token
        """
        auth = self.state['auth'].get(cache_key, {})
        def _cache_token(token_info, username=None):
            '''
            Helper to update state with new token info and optional username.
            Returns the latest access token.
            '''
            # Make sure this is in sync with auth.py.
            token_info['expires_at'] = time.time() + float(token_info['expires_in'])
            del token_info['expires_in']
            auth['token_info'] = token_info
            if username is not None:
                auth['username'] = username
            self.save_state()
            return token_info['access_token']

        # Check the cache for a valid token
        if 'token_info' in auth:
            token_info = auth['token_info']
            expires_at = token_info.get('expires_at', 0.0)

            # If token is not nearing expiration, just return it.
            if expires_at >= (time.time() + 10 * 60):
                return token_info['access_token']

            # Otherwise, let's refresh the token.
            token_info = auth_handler.generate_token('refresh_token',
                                                     auth['username'],
                                                     token_info['refresh_token'])
            if token_info is not None:
                return _cache_token(token_info)

        # If we get here, a valid token is not already available.
        auth = self.state['auth'][cache_key] = {}

        # For a local client with mock credentials, use the default username.
        if is_local_address(cache_key):
            username = self.root_user_name()
            password = ''
        else:
            username = os.environ.get('CODALAB_USERNAME')
            password = os.environ.get('CODALAB_PASSWORD')
            if username is None or password is None:
                print 'Requesting access at %s' % cache_key
            if username is None:
                sys.stdout.write('Username: ')  # Use write to avoid extra space
                username = sys.stdin.readline().rstrip()
            if password is None:
                password = getpass.getpass()

        token_info = auth_handler.generate_token('credentials', username, password)
        if token_info is None:
            raise PermissionError("Invalid username or password.")
        return _cache_token(token_info, username)

    # TODO(sckoo): clean up backward compatibility hacks when REST API complete
    def get_current_worksheet_uuid(self, use_rest=False):
        '''
        Return a worksheet_uuid for the current worksheet, or None if there is none.

        This method uses the current parent-process id to return the same result
        across multiple invocations in the same shell.
        '''
        session = self.session()
        client = self.client(session['address'], use_rest=use_rest)
        bundle_client = self.client(session['address'])
        worksheet_uuid = session.get('worksheet_uuid', None)
        if not worksheet_uuid:
            worksheet_uuid = bundle_client.get_worksheet_uuid(None, '')
        return (client, worksheet_uuid)

    def set_current_worksheet_uuid(self, address, worksheet_uuid):
        '''
        Set the current worksheet to the given worksheet_uuid.
        '''
        session = self.session()
        session['address'] = address
        if worksheet_uuid:
            session['worksheet_uuid'] = worksheet_uuid
        else:
            if 'worksheet_uuid' in session: del session['worksheet_uuid']
        self.save_state()

    def logout(self, address):
        """Clear credentials associated with given address."""
        if address in self.state['auth']:
            del self.state['auth'][address]
            self.save_state()

    def save_config(self):
        if self.temporary: return
        write_pretty_json(self.config, self.config_path)

    def save_state(self):
        if self.temporary: return
        write_pretty_json(self.state, self.state_path)
