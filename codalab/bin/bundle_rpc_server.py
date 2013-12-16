from SimpleXMLRPCServer import SimpleXMLRPCServer

from codalab.client.local_bundle_client import LocalBundleClient
from codalab.client.remote_bundle_client import RemoteBundleClient
from codalab.common import BUNDLE_RPC_PORT


if __name__ == '__main__':
  client = LocalBundleClient()
  server = SimpleXMLRPCServer(('localhost', BUNDLE_RPC_PORT), allow_none=True)
  for command in RemoteBundleClient.PROXY_COMMANDS:
    server.register_function(getattr(client, command), command)
  print 'Listening on port %s...' % (BUNDLE_RPC_PORT,)
  server.serve_forever()
