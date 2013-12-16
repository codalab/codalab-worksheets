#!/usr/bin/env python
import sys

from codalab.lib.bundle_cli import BundleCLI


if __name__ == '__main__':
  # Hijack certain arguments to control global CLI behavior.
  FLAGS = ('--local', '--verbose')
  flags = {flag: flag in sys.argv for flag in FLAGS}
  argv = [argument for argument in sys.argv[1:] if argument not in flags]
  # Defer client imports because sqlalchemy and xmlrpclib are heavy-weight.
  if flags['--local']:
    from codalab.client.local_bundle_client import LocalBundleClient
    client = LocalBundleClient()
  else:
    from codalab.client.remote_bundle_client import RemoteBundleClient
    client = RemoteBundleClient()
  cli = BundleCLI(client, verbose=flags['--verbose'])
  cli.do_command(argv)
