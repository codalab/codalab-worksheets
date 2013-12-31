from codalab.client.local_bundle_client import LocalBundleClient
from codalab.client.remote_bundle_client import RemoteBundleClient
from codalab.common import BUNDLE_RPC_PORT
from codalab.server.file_server import FileServer


class BundleRPCServer(FileServer):
  def __init__(self):
    address = ('localhost', BUNDLE_RPC_PORT)
    self.client = LocalBundleClient()
    FileServer.__init__(self, address, self.client.bundle_store.temp)
    for command in RemoteBundleClient.PROXY_COMMANDS:
      self.register_function(getattr(self.client, command), command)
    self.register_function(self.open_target, 'open_target')

  def open_target(self, target):
    path = self.client.get_target_path(target)
    return self.open_file(path)

  def serve_forever(self):
    print 'RPC server serving on port %s...' % (BUNDLE_RPC_PORT,)
    FileServer.serve_forever(self)
