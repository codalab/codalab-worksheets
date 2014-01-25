'''
BundleCLI is a class that provides one major API method, do_command, which takes
a list of CodaLab bundle system command-line arguments and executes them.

Each of the supported commands corresponds to a method on this class.
This function takes an argument list and an ArgumentParser and does the action.

  ex: BundleCLI.do_command(['upload', 'program', '.'])
   -> BundleCLI.do_upload_command(['program', '.'], parser)
'''
import argparse
import collections
import datetime
import itertools
import os
import sys
import time

from codalab.bundles import (
  get_bundle_subclass,
  UPLOADED_TYPES,
)
from codalab.bundles.make_bundle import MakeBundle
from codalab.bundles.uploaded_bundle import UploadedBundle
from codalab.bundles.run_bundle import RunBundle
from codalab.common import (
  precondition,
  State,
  UsageError,
)
from codalab.lib import (
  metadata_util,
  path_util,
  spec_util,
)
from codalab.objects.worker import Worker


class BundleCLI(object):
  DESCRIPTIONS = {
    'help': 'Show a usage message for cl or for a particular command.',
    'upload': 'Create a bundle by uploading an existing directory.',
    'make': 'Create a bundle by packaging data from existing bundles.',
    'run': 'Create a bundle by running a program bundle on an input.',
    'edit': "Edit an existing bundle's metadata.",
    'rm': 'Delete a bundle and all bundles that depend on it.',
    'list': 'Show basic information for all bundles [in a worksheet].',
    'info': 'Show detailed information for a single bundle.',
    'ls': 'List the contents of a bundle.',
    'cat': 'Print the contents of a file in a bundle.',
    'wait': 'Wait until a bundle is ready or failed, then print its state.',
    # Worksheet-related commands.
    'new': 'Create a new worksheet and make it the current one.',
    'add': 'Append a bundle to a worksheet.',
    'work': 'Set the current worksheet.',
    'rework': 'Pop open a full-text editor to curate a worksheet.',
    # Commands that can only be executed on a LocalBundleClient.
    'cleanup': 'Clean up the CodaLab bundle store.',
    'worker': 'Run the CodaLab bundle worker.',
    'reset': 'Delete the CodaLab bundle store and reset the database.',
  }
  COMMON_COMMANDS = (
    'upload',
    'make',
    'run',
    'edit',
    'rm',
    'list',
    'info',
    'ls',
    'cat',
    'wait',
  )
  WORKSHEET_COMMANDS = (
    'new',
    'add',
    'work',
    'rework',
  )

  def __init__(self, client, env_model, verbose):
    self.client = client
    self.env_model = env_model
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

  def print_table(self, columns, row_dicts):
    '''
    Pretty-print a list of columns from each row in the given list of dicts.
    '''
    rows = list(itertools.chain([columns], (
      [row_dict.get(col, '') for col in columns] for row_dict in row_dicts
    )))
    lengths = [max(len(value) for value in col) for col in zip(*rows)]
    for (i, row) in enumerate(rows):
      row_strs = []
      for (value, length) in zip(row, lengths):
        row_strs.append(value + (length - len(value))*' ')
      print '  '.join(row_strs)
      if i == 0:
        print (sum(lengths) + 2*(len(columns) - 1))*'-'

  def size_str(self, size):
    for unit in ('bytes', 'KB', 'MB', 'GB'):
      if size < 1024:
        return '%d %s' % (size, unit)
      size /= 1024

  def time_str(self, ts):
    return datetime.datetime.utcfromtimestamp(ts).isoformat().replace('T', ' ')

  def do_command(self, argv):
    if argv:
      (command, remaining_args) = (argv[0], argv[1:])
    else:
      (command, remaining_args) = ('help', [])
    command_fn = getattr(self, 'do_%s_command' % (command,), None)
    if not command_fn:
      self.exit("'%s' is not a codalab command. Try 'cl help'." % (command,))
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
    max_length = max(
      len(command) for command in
      itertools.chain(self.COMMON_COMMANDS, self.WORKSHEET_COMMANDS)
    )
    indent = 2
    def print_command(command):
      print '%s%s%s%s' % (
        indent*' ',
        command,
        (indent + max_length - len(command))*' ',
        self.DESCRIPTIONS[command],
      )
    print '\nThe most commonly used codalab commands are:'
    for command in self.COMMON_COMMANDS:
      print_command(command)
    print '\nCommands for using worksheets include:'
    for command in self.WORKSHEET_COMMANDS:
      print_command(command)

  def do_upload_command(self, argv, parser):
    help_text = 'bundle_type: [%s]' % ('|'.join(sorted(UPLOADED_TYPES)))
    parser.add_argument('bundle_type', help=help_text)
    parser.add_argument('path', help='path of the directory to upload')
    # Add metadata arguments for UploadedBundle and all of its subclasses.
    metadata_keys = set()
    metadata_util.add_arguments(UploadedBundle, metadata_keys, parser)
    for bundle_type in UPLOADED_TYPES:
      bundle_subclass = get_bundle_subclass(bundle_type)
      metadata_util.add_arguments(bundle_subclass, metadata_keys, parser)
    metadata_util.add_auto_argument(parser)
    args = parser.parse_args(argv)
    # Check that the upload path exists.
    path_util.check_isvalid(path_util.normalize(args.path), 'upload')
    # Pull out the upload bundle type from the arguments and validate it.
    if args.bundle_type not in UPLOADED_TYPES:
      raise UsageError('Invalid bundle type %s (options: [%s])' % (
        args.bundle_type, '|'.join(sorted(UPLOADED_TYPES)),
      ))
    bundle_subclass = get_bundle_subclass(args.bundle_type)
    metadata = metadata_util.request_missing_data(bundle_subclass, args)
    # Type-check the bundle metadata BEFORE uploading the bundle data.
    # This optimization will avoid file copies on failed bundle creations.
    bundle_subclass.construct(data_hash='', metadata=metadata).validate()
    print self.client.upload(args.bundle_type, args.path, metadata)

  def do_make_command(self, argv, parser):
    help = '[<key>:][<uuid>|<name>][%s<subpath within bundle>]' % (os.sep,)
    parser.add_argument('target', help=help, nargs='+')
    metadata_util.add_arguments(MakeBundle, set(), parser)
    metadata_util.add_auto_argument(parser)
    args = parser.parse_args(argv)
    targets = {}
    # Turn targets into a dict mapping key -> (uuid, subpath)) tuples.
    for argument in args.target:
      if ':' in argument:
        (key, target) = argument.split(':', 1)
      else:
        # Provide syntactic sugar for a make bundle with a single anonymous target.
        (key, target) = ('', argument)
      if key in targets:
        if key:
          raise UsageError('Duplicate key: %s' % (key,))
        else:
          raise UsageError('Must specify keys when packaging multiple targets!')
      targets[key] = self.parse_target(target)
    metadata = metadata_util.request_missing_data(MakeBundle, args)
    print self.client.make(targets, metadata)

  def do_run_command(self, argv, parser):
    help = '[<uuid>|<name>][%s<subpath within bundle>]' % (os.sep,)
    parser.add_argument('program_target', help=help)
    parser.add_argument('input_target', help=help)
    parser.add_argument(
      'command',
      help='shell command with access to program, input, and output',
    )
    metadata_util.add_arguments(RunBundle, set(), parser)
    metadata_util.add_auto_argument(parser)
    args = parser.parse_args(argv)
    program_target = self.parse_target(args.program_target)
    input_target = self.parse_target(args.input_target)
    metadata = metadata_util.request_missing_data(RunBundle, args)
    print self.client.run(program_target, input_target, args.command, metadata)

  def do_edit_command(self, argv, parser):
    parser.add_argument('bundle_spec', help='identifier: [<uuid>|<name>]')
    args = parser.parse_args(argv)
    info = self.client.info(args.bundle_spec)
    bundle_subclass = get_bundle_subclass(info['bundle_type'])
    new_metadata = metadata_util.request_missing_data(
      bundle_subclass,
      args,
      info['metadata'],
    )
    if new_metadata != info['metadata']:
      self.client.edit(info['uuid'], new_metadata)

  def do_rm_command(self, argv, parser):
    parser.add_argument('bundle_spec', help='identifier: [<uuid>|<name>]')
    parser.add_argument(
      '-f', '--force',
      action='store_true',
      help='delete all downstream dependencies',
    )
    args = parser.parse_args(argv)
    self.client.delete(args.bundle_spec, args.force)

  def do_list_command(self, argv, parser):
    parser.parse_args(argv)
    bundle_info_list = self.client.search()
    if bundle_info_list:
      columns = ('uuid', 'name', 'bundle_type', 'state')
      bundle_dicts = [
        {col: info.get(col, info['metadata'].get(col, '')) for col in columns}
        for info in bundle_info_list
      ]
      self.print_table(columns, bundle_dicts)

  def do_info_command(self, argv, parser):
    parser.add_argument('bundle_spec', help='identifier: [<uuid>|<name>]')
    parser.add_argument(
      '-p', '--parents',
      action='store_true',
      help="print a list of this bundle's parents",
    )
    parser.add_argument(
      '-c', '--children',
      action='store_true',
      help="print a list of this bundle's children",
    )
    args = parser.parse_args(argv)
    if args.parents and args.children:
      raise UsageError('Only one of -p and -c should be used at a time!')
    info = self.client.info(args.bundle_spec, args.parents, args.children)
    if args.parents:
      if info['parents']:
        print '\n'.join(info['parents'])
    elif args.children:
      if info['children']:
        print '\n'.join(info['children'])
    else:
      print self.format_basic_info(info)

  def format_basic_info(self, info):
    metadata = collections.defaultdict(lambda: None, info['metadata'])
    # Format some simple fields of the basic info string.
    fields = {
      'bundle_type': info['bundle_type'],
      'uuid': info['uuid'],
      'data_hash': info['data_hash'] or '<no hash>',
      'state': info['state'],
      'name': metadata['name'] or '<no name>',
      'description': metadata['description'] or '<no description>',
    }
    # Format statistics about this bundle - creation time, runtime, size, etc.
    stats = []
    if 'created' in metadata:
      stats.append('Created: %s' % (self.time_str(metadata['created']),))
    if 'data_size' in metadata:
      stats.append('Size:    %s' % (self.size_str(metadata['data_size']),))
    fields['stats'] = 'Stats:\n  %s\n' % ('\n  '.join(stats),) if stats else ''
    # Compute a nicely-formatted list of hard dependencies. Since this type of
    # dependency is realized within this bundle as a symlink to another bundle,
    # label these dependencies as 'references' in the UI.
    fields['hard_dependencies'] = ''
    if info['hard_dependencies']:
      deps = info['hard_dependencies']
      if len(deps) == 1 and not deps[0]['child_path']:
        fields['hard_dependencies'] = 'Reference:\n  %s\n' % (
          path_util.safe_join(deps[0]['parent_uuid'], deps[0]['parent_path']),)
      else:
        fields['hard_dependencies'] = 'References:\n%s\n' % ('\n'.join(
          '  %s:%s' % (
            dep['child_path'],
            path_util.safe_join(dep['parent_uuid'], dep['parent_path']),
          ) for dep in sorted(deps, key=lambda dep: dep['child_path'])
        ))
    # Compute a nicely-formatted failure message, if this bundle failed.
    # It is possible for bundles that are not failed to have failure messages:
    # for example, if a bundle is killed in the database after running for too
    # long then succeeds afterwards, it will be in this state.
    fields['failure_message'] = ''
    if info['state'] == State.FAILED and metadata['failure_message']:
      fields['failure_message'] = 'Failure message:\n  %s\n' % ('\n  '.join(
        metadata['failure_message'].split('\n')
      ))
    # Return the formatted summary of the bundle info.
    return '''
{bundle_type}: {name}
{description}
  UUID:  {uuid}
  Hash:  {data_hash}
  State: {state}
{stats}{hard_dependencies}{failure_message}
    '''.format(**fields).strip()

  def do_ls_command(self, argv, parser):
    parser.add_argument(
      'target',
      help='[<uuid>|<name>][%s<subpath within bundle>]' % (os.sep,),
    )
    args = parser.parse_args(argv)
    target = self.parse_target(args.target)
    (directories, files) = self.client.ls(target)
    if directories:
      print '\n  '.join(['Directories:'] + list(directories))
    if files:
      print '\n  '.join(['Files:'] + list(files))

  def do_cat_command(self, argv, parser):
    parser.add_argument(
      'target',
      help='[<uuid>|<name>][%s<subpath within bundle>]' % (os.sep,),
    )
    args = parser.parse_args(argv)
    target = self.parse_target(args.target)
    self.client.cat(target)

  def do_wait_command(self, argv, parser):
    parser.add_argument('bundle_spec', help='identifier: [<uuid>|<name>]')
    args = parser.parse_args(argv)
    state = self.client.wait(args.bundle_spec)
    if state == State.READY:
      print state
    else:
      self.exit(state)

  #############################################################################
  # CLI methods for worksheet-related commands follow!
  #############################################################################

  def do_new_command(self, argv, parser):
    parser.add_argument('name', help='name: ' + spec_util.NAME_REGEX.pattern)
    args = parser.parse_args(argv)
    uuid = self.client.new_worksheet(args.name)
    self.env_model.set_current_worksheet(uuid, args.name)
    print 'Switched to worksheet %s.' % (args.name,)

  def do_add_command(self, argv, parser):
    parser.add_argument('bundle_spec', help='identifier: [<uuid>|<name>]')
    parser.add_argument(
      'worksheet_spec',
      help='identifier: [<uuid>|<name>]',
      #optional=True,
    )
    args = parser.parse_args(argv)
    self.client.add_worksheet_item(args.worksheet_spec, args.bundle_spec)

  def do_work_command(self, argv, parser):
    parser.add_argument(
      'worksheet_spec',
      help='identifier: [<uuid>|<name>]',
      nargs='?',
    )
    parser.add_argument(
      '-x', '--exit',
      action='store_true',
      help='Leave the current worksheet.',
    )
    args = parser.parse_args(argv)
    if args.worksheet_spec:
      info = self.client.worksheet_info(args.worksheet_spec)
      self.env_model.set_current_worksheet(info['uuid'], args.worksheet_spec)
      print 'Switched to worksheet %s.' % (args.worksheet_spec,)
    elif args.exit:
      self.env_model.clear_current_worksheet()
    else:
      (worksheet_uuid, worksheet_spec) = self.env_model.get_current_worksheet()
      worksheet_dicts = self.client.list_worksheets()
      if worksheet_uuid:
        print 'Currently on worksheet %s. Use `cl work -x` to leave.' % (worksheet_spec,)
      else:
        print 'Not on any worksheet. Use `cl new` or `cl work` to join one.'
      if worksheet_dicts:
        print ''
        self.print_table(('uuid', 'name'), worksheet_dicts)

  def do_rework_command(self, argv, parser):
    parser.add_argument('worksheet_spec', help='identifier: [<uuid>|<name>]')
    args = parser.parse_args(argv)
    info = self.client.worksheet_info(args.worksheet_spec)
    print 'Editing Worksheet(uuid=%r, name=%r)...' % (info['uuid'], info['name'])

  #############################################################################
  # LocalBundleClient-only commands follow!
  #############################################################################

  def do_cleanup_command(self, argv, parser):
    # This command only works if self.client is a LocalBundleClient.
    parser.parse_args(argv)
    self.client.bundle_store.full_cleanup(self.client.model)

  def do_worker_command(self, argv, parser):
    # This command only works if self.client is a LocalBundleClient.
    parser.add_argument('iterations', type=int, default=None, nargs='?')
    args = parser.parse_args(argv)
    worker = Worker(self.client.bundle_store, self.client.model)
    i = 0
    while not args.iterations or i < args.iterations:
      if i and not args.iterations:
        time.sleep(60)
      print 'Running CodaLab bundle worker iteration %s...\n' % (i,)
      worker.update_created_bundles()
      worker.update_staged_bundles()
      i += 1

  def do_reset_command(self, argv, parser):
    # This command only works if self.client is a LocalBundleClient.
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
