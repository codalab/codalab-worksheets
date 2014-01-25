'''
ConfigParser is initialized with a path to a JSON config file. It provides
helper methods that initialize each of the main CodaLab classes based on the
configuration in this file:
  home: returns the CodaLab home directory
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

See client_config.json and server_config.json for examples.
'''
import json

from codalab.common import UsageError


def cached(fn):
  def inner(self):
    if fn.__name__ not in self.cache:
      self.cache[fn.__name__] = fn(self)
    return self.cache[fn.__name__]
  return inner


class ConfigParser(object):
  def __init__(self, path):
    self.cache = {}
    with open(path, 'rb') as config_file:
      config_json = config_file.read()
    self.config = json.loads(config_json)

  @cached
  def home(self):
    from codalab.lib import path_util
    result = path_util.normalize(self.config['home'])
    path_util.make_directory(result)
    return result

  @cached
  def bundle_store(self):
    home = self.home()
    from codalab.lib.bundle_store import BundleStore
    return BundleStore(home)

  @cached
  def cli(self):
    verbose = self.config['cli']['verbose']
    client = self.client()
    env_model = self.env_model()
    from codalab.lib.bundle_cli import BundleCLI
    return BundleCLI(client, env_model, verbose)

  @cached
  def client(self):
    client_class = self.config['client']['class']
    if client_class == 'LocalBundleClient':
      bundle_store = self.bundle_store()
      model = self.model()
      from codalab.client.local_bundle_client import LocalBundleClient
      return LocalBundleClient(bundle_store, model)
    elif client_class == 'RemoteBundleClient':
      address = self.config['client']['address']
      from codalab.client.remote_bundle_client import RemoteBundleClient
      return RemoteBundleClient(address)
    else:
      raise UsageError('Unexpected client class: %s' % (client_class,))

  @cached
  def env_model(self):
    home = self.home()
    from codalab.model.env_model import EnvModel
    return EnvModel(home)

  @cached
  def model(self):
    model_class = self.config['model']['class']
    if model_class == 'MySQLModel':
      arguments = ('username', 'password', 'address', 'database')
      kwargs = {arg: self.config['model'][arg] for arg in arguments}
      from codalab.model.mysql_model import MySQLModel
      return MySQLModel(**kwargs)
    if model_class == 'SQLiteModel':
      home = self.home()
      from codalab.model.sqlite_model import SQLiteModel
      return SQLiteModel(home)
    else:
      raise UsageError('Unexpected model class: %s' % (model_class,))

  @cached
  def rpc_server(self):
    address = tuple(self.config['server']['address'])
    client = self.client()
    from codalab.server.bundle_rpc_server import BundleRPCServer
    return BundleRPCServer(address, client)
