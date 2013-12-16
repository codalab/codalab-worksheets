#!/usr/bin/env python
import argparse


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('server_type', help='[ftp|rpc]', default='rpc', nargs='?')
  args = parser.parse_args()
  if args.server_type == 'ftp':
    from codalab.server.bundle_ftp_server import BundleFTPServer
    server = BundleFTPServer()
  elif args.server_type == 'rpc':
    from codalab.server.bundle_rpc_server import BundleRPCServer
    server = BundleRPCServer()
  else:
    raise ValueError('Unexpected server type: %s' % (args.server_type,))
  server.serve_forever()
