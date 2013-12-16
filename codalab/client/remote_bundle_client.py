import xmlrpclib

from codalab.client.bundle_client import BundleClient
from codalab.common import (
  BUNDLE_RPC_PORT,
  UsageError,
)


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
        try:
          return getattr(self.proxy, command)(*args, **kwargs)
        except xmlrpclib.Fault, e:
          # Transform server-side UsageErrors into client-side UsageErrors.
          if 'codalab.common.UsageError' in e.faultString:
            index = e.faultString.find(':')
            raise UsageError(e.faultString[index + 1:])
          else:
            raise
      return inner
    for command in self.PROXY_COMMANDS:
      setattr(self, command, do_command(command))
