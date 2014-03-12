#!/usr/local/Cellar/python/2.7.3/Frameworks/Python.framework/Versions/2.7/Resources/Python.app/Contents/MacOS/Python
import sys

from codalab.config.config_parser import ConfigParser


if __name__ == '__main__':
    config_parser = ConfigParser(sys.argv[1])
    rpc_server = config_parser.rpc_server()
    rpc_server.serve_forever()
