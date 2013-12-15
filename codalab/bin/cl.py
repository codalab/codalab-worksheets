#!/usr/bin/env python
import argparse
import itertools
import os
import sys

from codalab.bundles import (
  BUNDLE_SUBCLASSES,
  get_bundle_subclass,
  UPLOADABLE_TYPES,
)
from codalab.bundles.uploaded_bundle import UploadedBundle
from codalab.client.local_bundle_client import LocalBundleClient
from codalab.common import (
  precondition,
  UsageError,
)
from codalab.lib import metadata_util


class BundleCLI(object):
  '''
  Each CodaLab bundle command corresponds to a function on this class.
  This function should take a list of arguments and perform the action.

    ex: BundleCLI.do_command(['upload', 'program', '.'])
        -> BundleCLI.do_upload_command(['program', '.'], parser)
  '''
  DESCRIPTIONS = {
    'help': 'Show a usage message for cl or for a particular command.',
    'upload': 'Create a bundle by uploading an existing directory.',
    'make': 'Create a bundle by packaging data from existing bundles.',
    'list': 'Show basic information for all bundles.',
    'info': 'Show detailed information for a single bundle.',
    'ls': 'List the contents of a bundle.',
    'reset': 'Delete the codalab bundle store and reset the database.',
  }
  COMMON_COMMANDS = ('upload', 'make', 'list', 'info', 'ls')

  def __init__(self, client, verbose):
    self.client = client
    self.verbose = verbose

  def exit(self, message, error_code=1):
    '''
    Print the message to stderr and exit with the given error code.
    '''
    precondition(error_code, 'exit called with error_code == 0')
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

  def parse_target(self, target):
    return tuple(target.split(os.sep, 1)) if os.sep in target else (target, '')

  def do_command(self, argv):
    if argv:
      (command, remaining_args) = (argv[0], argv[1:])
    else:
      (command, remaining_args) = ('help', [])
    command_fn = getattr(self, 'do_%s_command' % (command,), None)
    if not command_fn:
      self.exit("'%s' is not a codalab command. Try 'cl help'.")
    parser = argparse.ArgumentParser(
      prog='cl %s' % (command,),
      description=self.DESCRIPTIONS[command],
    )
    self.hack_formatter(parser)
    if self.verbose:
      command_fn(remaining_args, parser)
    else:
      try:
        return command_fn(remaining_args, parser)
      except UsageError, e:
        self.exit('%s: %s' % (e.__class__.__name__, e))

  def do_help_command(self, argv, parser):
    if argv:
      self.do_command([argv[0], '-h'])
    print 'usage: cl <command> <arguments>'
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
    help_text = 'bundle_type: [%s]' % ('|'.join(sorted(UPLOADABLE_TYPES)))
    parser.add_argument('bundle_type', help=help_text)
    parser.add_argument('path', help='path of the directory to upload')
    # Add metadata arguments for UploadedBundle and all of its subclasses.
    metadata_keys = set()
    metadata_util.add_arguments(UploadedBundle, metadata_keys, parser)
    for bundle_subclass in BUNDLE_SUBCLASSES:
      if issubclass(bundle_subclass, UploadedBundle):
        metadata_util.add_arguments(bundle_subclass, metadata_keys, parser)
    args = parser.parse_args(argv)
    if args.bundle_type not in UPLOADABLE_TYPES:
      raise UsageError('Invalid bundle type %s (options: [%s])' % (
        args.bundle_type, '|'.join(sorted(UPLOADABLE_TYPES)),
      ))
    bundle_subclass = get_bundle_subclass(args.bundle_type)
    metadata = metadata_util.request_missing_data(bundle_subclass, args)
    print self.client.upload(args.bundle_type, args.path, metadata)

  def do_make_command(self, argv, parser):
    help = '<key>:[<uuid>|<name>][%s<subpath within bundle>]' % (os.sep,)
    parser.add_argument('target', help=help, nargs='+')
    args = parser.parse_args(argv)
    targets = {}
    for argument in args.target:
      if ':' not in argument:
        raise UsageError('Malformed target %s (expected %s)' % (argument, help))
      (key, target) = argument.split(':', 1)
      if key in targets:
        raise UsageError('Duplicate key: %s' % (key,))
      targets[key] = self.parse_target(target)
    print self.client.make(targets)

  def do_list_command(self, argv, parser):
    parser.parse_args(argv)
    bundle_info_list = self.client.search()
    if bundle_info_list:
      columns = ('uuid', 'name', 'bundle_type', 'state')
      rows = list(itertools.chain([columns], (
        [info.get(col, info['metadata'].get(col, '')) for col in columns]
        for info in bundle_info_list
      )))
      lengths = [max(len(value) for value in col) for col in zip(*rows)]
      for (i, row) in enumerate(rows):
        row_strs = []
        for (value, length) in zip(row, lengths):
          row_strs.append(value + (length - len(value))*' ')
        print '  '.join(row_strs)
        if i == 0:
          print (sum(lengths) + 2*(len(columns) - 1))*'-'


  def do_info_command(self, argv, parser):
    parser.add_argument(
      'bundle_spec',
      help='identifier: [<uuid>|<name>]'
    )
    args = parser.parse_args(argv)
    uuid = self.client.get_spec_uuid(args.bundle_spec)
    info = self.client.info(uuid)
    print '''
{bundle_type}: {name}
{description}
  uuid:     {uuid}
  location: {location}
  state:    {state}
    '''.strip().format(
      bundle_type=info['bundle_type'],
      name=(info['metadata'].get('name') or '<no name>'),
      description=(info['metadata'].get('description') or '<no description>'),
      uuid=info['uuid'],
      location=(info['location'] or '<this bundle is not ready>'),
      state=info['state'].upper(),
    )

  def do_ls_command(self, argv, parser):
    parser.add_argument(
      'target',
      help='[<uuid>|<name>][%s<subpath within bundle>]' % (os.sep,),
    )
    args = parser.parse_args(argv)
    target = self.parse_target(args.target)
    (directories, files) = self.client.ls(target)
    print '\n  '.join(['Directories:'] + list(directories))
    print '\n  '.join(['Files:'] + list(files))

  def do_reset_command(self, argv, parser):
    parser.add_argument(
      '--commit',
      action='store_true',
      help='reset is a no-op unless committed',
    )
    args = parser.parse_args(argv)
    if not args.commit:
      raise UsageError('If you really want to delete all bundles, use --commit')
    self.client.bundle_store._reset()
    self.client.model._reset()


if __name__ == '__main__':
  VERBOSE = '--verbose'
  verbose = VERBOSE in sys.argv
  argv = [argument for argument in sys.argv[1:] if argument != VERBOSE]
  cli = BundleCLI(LocalBundleClient(), verbose=verbose)
  cli.do_command(argv)
