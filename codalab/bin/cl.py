#!/usr/bin/env python
import argparse
import sys

from codalab.bundles import (
  BUNDLE_SUBCLASSES,
  get_bundle_subclass,
)
from codalab.bundles.uploaded_bundle import UploadedBundle
from codalab.client.local_bundle_client import LocalBundleClient
from codalab.lib.metadata_util import (
  add_metadata_arguments,
  request_missing_metadata,
)


class BundleCLI(object):
  '''
  Each CodaLab bundle command corresponds to a function on this class.
  This function should take a list of arguments and perform the action.
  '''
  DESCRIPTIONS = {
    'help': 'Show a usage message for cl.py or for a particular command.',
    'upload': 'Create a bundle by uploading an existing directory.',
    'info': 'Show detailed information about an existing bundle.',
    'ls': 'List the contents of a bundle.',
    'reset': 'Delete the codalab bundle store and reset the database.',
  }
  COMMON_COMMANDS = ('upload', 'info', 'ls')

  def __init__(self, client, verbose):
    self.client = client
    self.verbose = verbose

  def exit(self, message, error_code=1):
    if not error_code:
      raise ValueError('exit called with error_code=0')
    print >> sys.stderr, message
    sys.exit(error_code)

  def hack_formatter(self, parser):
    '''
    Screw with the argparse default formatter to improve help formatting.
    '''
    formatter_class = parser.formatter_class
    if type(formatter_class) == type:
      def mock_formatter_class(*args, **kwargs):
        return formatter_class(max_help_position=30, *args, **kwargs)
      parser.formatter_class = mock_formatter_class

  def do_command(self, argv):
    if argv:
      (command, remaining_args) = (argv[0], argv[1:])
    else:
      (command, remaining_args) = ('help', [])
    command_fn = getattr(self, 'do_%s_command' % (command,), None)
    if not command_fn:
      self.exit("'%s' is not a codalab command. %s" % (command, self.USAGE))
    parser = argparse.ArgumentParser(
      prog='./cl.py %s' % (command,),
      description=self.DESCRIPTIONS[command],
    )
    self.hack_formatter(parser)
    if self.verbose:
      command_fn(remaining_args, parser)
    else:
      try:
        return command_fn(remaining_args, parser)
      except Exception, e:
        self.exit('%s: %s' % (e.__class__.__name__, e))

  def do_help_command(self, argv, parser):
    if argv:
      self.do_command([argv[0], '-h'])
    print 'usage: ./cl.py <command> <arguments>'
    print '\nThe most commonly used codalab commands are:'
    max_length = max(len(command) for command in self.DESCRIPTIONS)
    indent = 2
    for command in self.COMMON_COMMANDS:
      print '%s%s%s%s' % (
        indent*' ',
        command,
        (indent + max_length - len(command))*' ',
        self.DESCRIPTIONS[command],
      )

  def do_upload_command(self, argv, parser):
    parser.add_argument('bundle_type', help='bundle type: [program|dataset]')
    parser.add_argument('path', help='path of the directory to upload')
    # Add metadata arguments for UploadedBundle and all of its subclasses.
    metadata_keys = set()
    add_metadata_arguments(UploadedBundle, metadata_keys, parser)
    for bundle_subclass in BUNDLE_SUBCLASSES:
      if issubclass(bundle_subclass, UploadedBundle):
        add_metadata_arguments(bundle_subclass, metadata_keys, parser)
    args = parser.parse_args(argv)
    bundle_subclass = get_bundle_subclass(args.bundle_type)
    metadata = request_missing_metadata(bundle_subclass, args)
    print self.client.upload(args.bundle_type, args.path, metadata)

  def do_reset_command(self, argv, parser):
    parser.add_argument('--commit', type=bool, help='no-op unless committed')
    args = parser.parser_args(argv)
    if not args.commit:
      raise ValueError('Reset does nothing unless committed!')
    self.client.bundle_store.clear()
    self.client.model.clear()


if __name__ == '__main__':
  cli = BundleCLI(LocalBundleClient(), verbose=False)
  cli.do_command(sys.argv[1:])
