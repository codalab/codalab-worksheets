#!/usr/bin/env python
import sys

# DEPRECATED
from codalab.config.config_parser import ConfigParser


if __name__ == '__main__':
    config_parser = ConfigParser(sys.argv[1])
    rpc_server = config_parser.rpc_server()
    rpc_server.serve_forever()
