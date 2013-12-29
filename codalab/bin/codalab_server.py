#!/usr/bin/env python


if __name__ == '__main__':
  from codalab.server.bundle_rpc_server import BundleRPCServer
  server = BundleRPCServer()
  server.serve_forever()
