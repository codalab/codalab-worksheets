#!/usr/bin/env python
import argparse
import re
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
from codalab.objects.bundle import Bundle


class BundleCLI(object):
  '''
  Each CodaLab bundle command corresponds to a function on this class.
  This function should take a list of arguments and perform the action.

    ex: BundleCLI.do_command(['upload', 'program', '.'])
        -> BundleCLI.do_upload_command(['program', '.'], parser)
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
    '''
    Print the message to stderr and exit with the given error code.
    '''
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

  def parse_bundle_spec(self, bundle_spec):
    '''
    Take a string bundle_spec, which is EITHER a uuid or a bundle name,
    and resolve it to a unique bundle uuid.
    '''
    if not bundle_spec:
      raise ValueError('Tried to expand empty bundle_spec!')
    if re.match(Bundle.UUID_REGEX, bundle_spec):
      return bundle_spec
    elif not re.match(UploadedBundle.NAME_REGEX, bundle_spec):
      raise ValueError(
        "Bundle names should match '%s', was '%s'" %
        (UploadedBundle.NAME_REGEX, bundle_spec)
      )
    bundles = self.client.model.search_bundles({'name': bundle_spec})
    if not bundles:
      raise ValueError('No bundle found with name: %s' % (bundle_spec,))
    elif len(bundles) > 1:
      raise ValueError(
        'Found multiple bundles with name %s: %s' %
        (bundle_spec, ''.join('\n    %s' % (bundle,) for bundle in bundles))
      )
    return bundles[0].uuid

  def parse_target(self, target):
    if ':' in target:
      (bundle_spec, subpath) = target.split(':', 1)
    else:
      (bundle_spec, subpath) = (target, '')
    return (self.parse_bundle_spec(bundle_spec), subpath)

  def do_command(self, argv):
    if argv:
      (command, remaining_args) = (argv[0], argv[1:])
    else:
      (command, remaining_args) = ('help', [])
    command_fn = getattr(self, 'do_%s_command' % (command,), None)
    if not command_fn:
      self.exit("'%s' is not a codalab command. Try './cl.py help'.")
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

  def do_info_command(self, argv, parser):
    parser.add_argument(
      'bundle_spec',
      help='identifier: [<uuid>|<name>]'
    )
    args = parser.parse_args(argv)
    uuid = self.parse_bundle_spec(args.bundle_spec)
    info = self.client.info(uuid)
    print '''
%s: %s
%s
  uuid:     %s
  location: %s
  state:    %s
    '''.strip() % (
      info['bundle_type'],
      (info['metadata'].get('name') or '<no name>'),
      (info['metadata'].get('description') or '<no description>'),
      info['uuid'],
      info['location'],
      info['state'].upper(),
    )

  def do_ls_command(self, argv, parser):
    parser.add_argument(
      'target',
      help='[<uuid>|<name>][:<subpath within bundle>]',
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
      raise ValueError('If you really want to delete all bundles, use --commit')
    self.client.bundle_store._reset()
    self.client.model._reset()


if __name__ == '__main__':
  cli = BundleCLI(LocalBundleClient(), verbose=False)
  cli.do_command(sys.argv[1:])
