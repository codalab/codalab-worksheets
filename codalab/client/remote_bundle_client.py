import contextlib
import sys
import xmlrpclib

from codalab.client.bundle_client import BundleClient
from codalab.common import (
  BUNDLE_RPC_PORT,
  UsageError,
)
from codalab.lib import file_util
from codalab.server.rpc_file_handle import RPCFileHandle


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
  EXTRA_COMMANDS = (
    'open_target',
    'read_file',
    'close_file',
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
    for command in self.PROXY_COMMANDS + self.EXTRA_COMMANDS:
      setattr(self, command, do_command(command))

  def cat(self, target):
    file_uuid = self.open_target(target)
    with contextlib.closing(RPCFileHandle(file_uuid, self.proxy)) as source:
      file_util.copy(source, sys.stdout)
