#!/usr/bin/env python
import sys

from codalab.lib.codalab_manager import CodaLabManager

if __name__ == '__main__':
    manager = CodaLabManager()
    # Either start the server or the client.
    if sys.argv[1:] == ['server']:
        from codalab.server.bundle_rpc_server import BundleRPCServer
        rpc_server = BundleRPCServer(manager)
        rpc_server.serve_forever()
    else:
        from codalab.lib.bundle_cli import BundleCLI
        cli = BundleCLI(manager)
        cli.do_command(sys.argv[1:])
