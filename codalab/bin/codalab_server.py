#!/usr/bin/env python
from codalab.server.bundle_rpc_server import BundleRPCServer


if __name__ == '__main__':
  rpc_server = BundleRPCServer()
  rpc_server.serve_forever()
