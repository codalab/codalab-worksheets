import xmlrpclib

from codalab.client.bundle_client import BundleClient
from codalab.common import BUNDLE_RPC_PORT


class RemoteBundleClient(BundleClient):
  PROXY_COMMANDS = (
    #'upload',
    'make',
    'run',
    'update',
    'info',
    'ls',
    #'cat',
    #'grep',
    'search',
    #'download',
    'wait',
  )

  def __init__(self):
    address = 'http://localhost:%s/' % (BUNDLE_RPC_PORT,)
    self.proxy = xmlrpclib.ServerProxy(address)
    def do_command(command):
      def inner(*args, **kwargs):
        return getattr(self.proxy, command)(*args, **kwargs)
      return inner
    for command in self.PROXY_COMMANDS:
      setattr(self, command, do_command(command))
