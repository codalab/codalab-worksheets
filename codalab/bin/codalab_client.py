#!/usr/bin/env python
import sys

from codalab.config.config_parser import ConfigParser


if __name__ == '__main__':
  config_parser = ConfigParser(sys.argv[1])
  cli = config_parser.cli()
  cli.do_command(sys.argv[2:])
