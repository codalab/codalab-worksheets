#!/usr/bin/env python
import argparse


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('server_type', help='[file|rpc]', default='rpc', nargs='?')
  args = parser.parse_args()
  if args.server_type == 'file':
    from codalab.server.bundle_file_server import BundleFileServer
    server = BundleFileServer()
  elif args.server_type == 'rpc':
    from codalab.server.bundle_rpc_server import BundleRPCServer
    server = BundleRPCServer()
  else:
    raise ValueError('Unexpected server type: %s' % (args.server_type,))
  server.serve_forever()
