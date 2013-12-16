from SimpleXMLRPCServer import SimpleXMLRPCServer

from codalab.client.local_bundle_client import LocalBundleClient
from codalab.client.remote_bundle_client import RemoteBundleClient
from codalab.common import BUNDLE_RPC_PORT


class BundleRPCServer(SimpleXMLRPCServer):
  def __init__(self):
    address = ('localhost', BUNDLE_RPC_PORT)
    SimpleXMLRPCServer.__init__(self, address, allow_none=True)
    self.client = LocalBundleClient()
    for command in RemoteBundleClient.PROXY_COMMANDS:
      self.register_function(getattr(self.client, command), command)

  def serve_forever(self):
    print 'RPC server serving on port %s...' % (BUNDLE_RPC_PORT,)
    SimpleXMLRPCServer.serve_forever(self)
