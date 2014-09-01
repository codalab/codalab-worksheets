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
import itertools
import os
import re
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
  PermissionError,
  UsageError,
)
from codalab.lib import (
  metadata_util,
  path_util,
  spec_util,
  worksheet_util,
  canonicalize,
  formatting
)
from codalab.objects.worksheet import Worksheet
from codalab.objects.work_manager import Worker
from codalab.machines import (
  local_machine,
  pool_machine,
  remote_machine,
)

class BundleCLI(object):
    DESCRIPTIONS = {
      # Commands for bundles.
      'upload': 'Create a bundle by uploading an existing file/directory.',
      'make': 'Create a bundle out of existing bundles.',
      'run': 'Create a bundle by running a program bundle on an input bundle.',
      'edit': "Edit an existing bundle's metadata.",
      'rm': 'Delete a bundle (and all bundles that depend on it).',
      'search': 'Search for bundles in the system',
      'ls': 'List bundles in a worksheet.',
      'info': 'Show detailed information for a bundle.',
      'cat': 'Print the contents of a file/directory in a bundle.',
      'wait': 'Wait until a bundle finishes.',
      'download': 'Download bundle from an instance.',
      'cp': 'Copy bundles across instances.',
      'mimic': 'Creates a set of bundles based on analogy with another set.',
      'macro': 'Use mimicry to simulate macros.',
      # Commands for worksheets.
      'kill': 'Instruct the worker to terminate a running bundle.',
      'new': 'Create a new worksheet and make it the current one.',
      'add': 'Append a bundle to a worksheet.',
      'work': 'Set the current instance/worksheet.',
      'print': 'Print the contents of a worksheet.',
      'wedit': 'Edit the contents of a worksheet.',
      'wrm': 'Delete a worksheet.',
      'wls': 'List all worksheets.',
      'wcp': 'Copy the contents from one worksheet to another.',
      # Commands for groups and permissions.
      'list-groups': 'Show groups to which you belong.',
      'new-group': 'Create a new group.',
      'rm-group': 'Delete a group.',
      'group-info': 'Show detailed information for a group.',
      'add-user': 'Add a user to a group.',
      'rm-user': 'Remove a user from a group.',
      'set-perm': 'Set a group\'s permissions for a worksheet.',
      # Commands that can only be executed on a LocalBundleClient.
      'help': 'Show a usage message for cl or for a particular command.',
      'status': 'Show current client status.',
      'alias': 'Manage CodaLab instance aliases.',
      'worker': 'Run the CodaLab bundle worker.',
      # Internal commands wihch are used for debugging.
      'cleanup': 'Clean up the CodaLab bundle store.',
      'reset': 'Delete the CodaLab bundle store and reset the database.',
      # Note: this is not actually handled in BundleCLI, but here just to show the help
      'server': 'Start an instance of the CodaLab server.',
    }

    BUNDLE_COMMANDS = (
        'upload',
        'make',
        'run',
        'edit',
        'rm',
        'ls',
        'info',
        'cat',
        'wait',
        'download',
        'cp',
    )

    WORKSHEET_COMMANDS = (
        'new',
        'add',
        'work',
        'print',
        'wedit',
        'wrm',
        'wls',
        'wcp',
    )

    GROUP_AND_PERMISSION_COMMANDS = (
        'list-groups',
        'new-group',
        'rm-group',
        'group-info',
        'add-user',
        'rm-user',
        'set-perm',
    )

    OTHER_COMMANDS = (
        'help',
        'status',
        'alias',
        'worker',
        'server',
    )

    SHORTCUTS = {
        'up': 'upload',
        'down': 'download',
    }

    def __init__(self, manager):
        self.manager = manager
        self.verbose = manager.cli_verbose()

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

    def get_worksheet_bundles(self, worksheet_info):
        '''
        Return list of info dicts of distinct, non-orphaned bundles in the worksheet.
        '''
        result = []
        for (bundle_info, _, _) in worksheet_info['items']:
            if bundle_info:
                result.append(bundle_info)
        return result

    def parse_target(self, client, worksheet_uuid, target_spec):
        '''
        Helper: A target_spec is a bundle_spec[/subpath].
        '''
        if os.sep in target_spec:
            bundle_spec, subpath = tuple(target_spec.split(os.sep, 1))
        else:
            bundle_spec, subpath = target_spec, ''
        # Resolve the bundle_spec to a particular bundle_uuid.
        bundle_uuid = worksheet_util.get_bundle_uuid(client, worksheet_uuid, bundle_spec)
        return (bundle_uuid, subpath)

    def parse_key_targets(self, client, worksheet_uuid, items):
        '''
        Helper: items is a list of strings which are [<key>]:<target>
        '''
        targets = {}
        # Turn targets into a dict mapping key -> (uuid, subpath)) tuples.
        for item in items:
            if ':' in item:
                (key, target) = item.split(':', 1)
                if key == '': key = target  # Set default key to be same as target
            else:
                # Provide syntactic sugar for a make bundle with a single anonymous target.
                (key, target) = ('', item)
            if key in targets:
                if key:
                    raise UsageError('Duplicate key: %s' % (key,))
                else:
                    raise UsageError('Must specify keys when packaging multiple targets!')
            targets[key] = self.parse_target(client, worksheet_uuid, target)
        return targets

    def print_table(self, columns, row_dicts, post_funcs={}, justify={}, show_header=True, indent=''):
        '''
        Pretty-print a list of columns from each row in the given list of dicts.
        '''
        # Get the contents of the table
        rows = [columns]
        for row_dict in row_dicts:
            row = []
            for col in columns:
                cell = row_dict.get(col)
                func = post_funcs.get(col)
                if func: cell = worksheet_util.apply_func(func, cell)
                if cell == None: cell = ''
                row.append(cell)
            rows.append(row)

        # Display the table
        lengths = [max(len(str(value)) for value in col) for col in zip(*rows)]
        for (i, row) in enumerate(rows):
            row_strs = []
            for (j, value) in enumerate(row):
                length = lengths[j]
                padding = (length - len(str(value))) * ' '
                if justify.get(columns[j], -1) < 0:
                    row_strs.append(str(value) + padding)
                else:
                    row_strs.append(padding + str(value))
                # TODO: center
            if show_header or i > 0:
                print indent + '  '.join(row_strs)
            if i == 0:
                print indent + (sum(lengths) + 2*(len(columns) - 1)) * '-'

    GLOBAL_SPEC_FORMAT = "[<alias>::|<address>::]|(<uuid>|<name>)"
    TARGET_SPEC_FORMAT = '[<key>:](<uuid>|<name>)[%s<subpath within bundle>]' % (os.sep,)
    BUNDLE_SPEC_FORMAT = '(<uuid>|<name>)'
    WORKSHEET_SPEC_FORMAT = GLOBAL_SPEC_FORMAT

    def parse_spec(self, spec):
        '''
        Parse a global spec, which includes the instance and either a bundle or worksheet spec.
        Example: http://codalab.org/bundleservice::wine
        Return (client, spec)
        '''
        tokens = spec.split('::')
        if len(tokens) == 1:
            address = self.manager.session()['address']
            spec = tokens[0]
        else:
            address = self.manager.apply_alias(tokens[0])
            spec = tokens[1]
        if spec == '': spec = Worksheet.DEFAULT_WORKSHEET_NAME
        return (self.manager.client(address), spec)

    def parse_client_worksheet_uuid(self, spec):
        if not spec:
            client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        else:
            client, spec = self.parse_spec(spec)
            worksheet_uuid = worksheet_util.get_worksheet_uuid(client, spec)
        return (client, worksheet_uuid)

    def create_parser(self, command):
        parser = argparse.ArgumentParser(
          prog='cl %s' % (command,),
          description=self.DESCRIPTIONS[command],
        )
        self.hack_formatter(parser)
        return parser

    #############################################################################
    # CLI methods
    #############################################################################

    def do_command(self, argv):
        if argv:
            (command, remaining_args) = (argv[0], argv[1:])
        else:
            (command, remaining_args) = ('help', [])
        command = self.SHORTCUTS.get(command, command)

        command_fn = getattr(self, 'do_%s_command' % (command.replace('-', '_'),), None)
        if not command_fn:
            self.exit("'%s' is not a CodaLab command. Try 'cl help'." % (command,))
        parser = self.create_parser(command)
        if self.verbose >= 2:
            command_fn(remaining_args, parser)
        else:
            try:
                # Profiling (off by default)
                if False:
                    import hotshot, hotshot.stats
                    prof_path = 'codalab.prof'
                    prof = hotshot.Profile(prof_path)
                    prof.runcall(command_fn, remaining_args, parser)
                    prof.close()
                    stats = hotshot.stats.load(prof_path)
                    #stats.strip_dirs()
                    stats.sort_stats('time', 'calls')
                    stats.print_stats(20)
                else:
                    command_fn(remaining_args, parser)
            except PermissionError:
                self.exit("You do not have sufficient permissions to execute this command.")
            except UsageError, e:
                self.exit('%s: %s' % (e.__class__.__name__, e))

    def do_help_command(self, argv, parser):
        if argv:
            self.do_command([argv[0], '-h'] + argv[1:])
        print 'Usage: cl <command> <arguments>'
        max_length = max(
          len(command) for command in
          itertools.chain(self.BUNDLE_COMMANDS,
                          self.WORKSHEET_COMMANDS,
                          self.GROUP_AND_PERMISSION_COMMANDS,
                          self.OTHER_COMMANDS)
        )
        indent = 2
        def print_command(command):
            print '%s%s%s%s' % (
              indent*' ',
              command,
              (indent + max_length - len(command))*' ',
              self.DESCRIPTIONS[command],
            )
        print '\nCommands for bundles:'
        for command in self.BUNDLE_COMMANDS:
            print_command(command)
        print '\nCommands for worksheets:'
        for command in self.WORKSHEET_COMMANDS:
            print_command(command)
        print '\nCommands for groups and permissions:'
        for command in self.GROUP_AND_PERMISSION_COMMANDS:
            print_command(command)
        print '\nOther commands:'
        for command in self.OTHER_COMMANDS:
            print_command(command)

    def do_status_command(self, argv, parser):
        print "codalab_home: %s" % self.manager.codalab_home()
        print "session: %s" % self.manager.session_name()
        address = self.manager.session()['address']
        print "address: %s" % address
        state = self.manager.state['auth'].get(address, {})
        if 'username' in state:
            print "username: %s" % state['username']
        worksheet_info = self.get_current_worksheet_info()
        if worksheet_info:
            print "worksheet: %s(%s)" % (worksheet_info['name'], worksheet_info['uuid'])

    def do_alias_command(self, argv, parser):
        '''
        Show, add, modify, delete aliases (mappings from names to instances).
        Only modifies the CLI configuration, doesn't need a BundleClient.
        '''
        parser.add_argument('key', help='name of the alias (e.g., cloud)', nargs='?')
        parser.add_argument('value', help='Instance to map the alias to (e.g., http://codalab.org:2800)', nargs='?')
        parser.add_argument('-r', '--remove', help='Remove this alias', action='store_true')
        args = parser.parse_args(argv)
        aliases = self.manager.config['aliases']
        if args.key:
            value = aliases.get(args.key)
            if args.remove:
                del aliases[args.key]
                self.manager.save_config()
            elif args.value:
                aliases[args.key] = args.value
                self.manager.save_config()
            else:
                print args.key + ': ' + (value if value else '(none)')
        else:
            for key, value in aliases.items():
                print key + ': ' + value

    def do_upload_command(self, argv, parser):
        help_text = 'bundle_type: [%s]' % ('|'.join(sorted(UPLOADED_TYPES)))
        parser.add_argument('bundle_type', help=help_text)
        parser.add_argument('path', help='path(s) of the file/directory to upload', nargs='+')
        parser.add_argument('-b', '--base', help='Inherit the metadata from this bundle specification.')
        parser.add_argument('-B', '--base-use-default-name', help='Inherit the metadata from the bundle with the same name as the path.', action='store_true')
        parser.add_argument('-w', '--worksheet_spec', help='upload to this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')

        # Add metadata arguments for UploadedBundle and all of its subclasses.
        metadata_keys = set()
        metadata_util.add_arguments(UploadedBundle, metadata_keys, parser)
        for bundle_type in UPLOADED_TYPES:
            bundle_subclass = get_bundle_subclass(bundle_type)
            metadata_util.add_arguments(bundle_subclass, metadata_keys, parser)
        metadata_util.add_edit_argument(parser)
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        # Expand shortcuts
        if args.bundle_type == 'd': args.bundle_type = 'dataset'
        if args.bundle_type == 'p': args.bundle_type = 'program'

        # Check that the upload path exists.
        for path in args.path:
            path_util.check_isvalid(path_util.normalize(path), 'upload')

        # Pull out the upload bundle type from the arguments and validate it.
        if args.bundle_type not in UPLOADED_TYPES:
            raise UsageError('Invalid bundle type %s (options: [%s])' % (
              args.bundle_type, '|'.join(sorted(UPLOADED_TYPES)),
            ))
        bundle_subclass = get_bundle_subclass(args.bundle_type)
        # Get metadata
        metadata = None
        if not args.base and args.base_use_default_name:
            args.base = os.path.basename(args.path[0]) # Use default name
        if args.base:
            bundle_uuid = worksheet_util.get_bundle_uuid(client, worksheet_uuid, args.base)
            info = client.get_bundle_info(bundle_uuid)
            metadata = info['metadata']
        metadata = metadata_util.request_missing_metadata(bundle_subclass, args, initial_metadata=metadata)
        # Type-check the bundle metadata BEFORE uploading the bundle data.
        # This optimization will avoid file copies on failed bundle creations.
        bundle_subclass.construct(data_hash='', metadata=metadata).validate()

        # If only one path, strip away the list so that we make a bundle that
        # is this path rather than contains it.
        if len(args.path) == 1: args.path = args.path[0]

        # Finally, once everything has been checked, then call the client to upload.
        # Follow symlinks so we don't end up with broken symlinks.
        print client.upload_bundle(args.path, {'bundle_type': args.bundle_type, 'metadata': metadata}, worksheet_uuid, True)

    def do_download_command(self, argv, parser):
        parser.add_argument('target_spec', help=self.TARGET_SPEC_FORMAT)
        parser.add_argument('-o', '--output-dir', help='Directory to download file.  By default, the bundle or subpath name is used.')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        target = self.parse_target(client, worksheet_uuid, args.target_spec)
        bundle_uuid, subpath = target

        # Copy into desired directory.
        info = client.get_bundle_info(bundle_uuid)
        if args.output_dir:
            local_dir = args.output_dir
        else:
            local_dir = info['metadata']['name'] if subpath == '' else os.path.basename(subpath)
        final_path = os.path.join(os.getcwd(), local_dir)
        if os.path.exists(final_path):
            print 'Local directory', local_dir, 'already exists.'
            return

        # Download first to a local location path.
        local_path, temp_path = client.download_target(target, True)
        path_util.copy(local_path, final_path, follow_symlinks=True)
        if temp_path:
          path_util.remove(temp_path)
        print 'Downloaded %s(%s) to %s.' % (bundle_uuid, info['metadata']['name'], final_path)

    def do_cp_command(self, argv, parser):
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT)
        parser.add_argument('worksheet_spec', help='%s (copy to this worksheet)' % self.WORKSHEET_SPEC_FORMAT)
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()

        # Source bundle
        (source_client, source_spec) = self.parse_spec(args.bundle_spec)
        # worksheet_uuid is only applicable if we're on the source client
        if source_client != client: worksheet_uuid = None
        source_bundle_uuid = worksheet_util.get_bundle_uuid(source_client, worksheet_uuid, source_spec)

        # Destination worksheet
        (dest_client, dest_worksheet_uuid) = self.parse_client_worksheet_uuid(args.worksheet_spec)

        # Copy!
        self.copy_bundle(source_client, source_bundle_uuid, dest_client, dest_worksheet_uuid)

    def copy_bundle(self, source_client, source_bundle_uuid, dest_client, dest_worksheet_uuid):
        '''
        Helper function that supports cp and wcp.
        Copies the source bundle to the target worksheet.
        Goes between two clients by downloading and then uploading, which is
        not the most efficient.  Usually one of the source or destination
        clients will be local, so it's not too expensive.
        '''
        # TODO: copy all the hard dependencies (for make bundles)

        # Check if the bundle already exists on the destination, then don't copy it
        # (although metadata could be different)
        bundle = None
        try:
            bundle = dest_client.get_bundle_info(source_bundle_uuid)
        except:
            pass

        source_desc = "%s(%s)" % (source_bundle_uuid, source_client.get_bundle_info(source_bundle_uuid)['metadata']['name'])
        if not bundle:
            print "Copying %s..." % source_desc

            # Download from source
            source_path, temp_path = source_client.download_target((source_bundle_uuid, ''), False)
            info = source_client.get_bundle_info(source_bundle_uuid)

            # Upload to dest
            print dest_client.upload_bundle(source_path, info, dest_worksheet_uuid, False)
            if temp_path: path_util.remove(temp_path)
        else:
            print "%s already exists on destination client" % source_desc

            # Just need to add it to the worksheet
            dest_client.add_worksheet_item(dest_worksheet_uuid, (source_bundle_uuid, None, worksheet_util.TYPE_BUNDLE))

    def do_make_command(self, argv, parser):
        parser.add_argument('target_spec', help=self.TARGET_SPEC_FORMAT, nargs='+')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        metadata_util.add_arguments(MakeBundle, set(), parser)
        metadata_util.add_edit_argument(parser)
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        targets = self.parse_key_targets(client, worksheet_uuid, args.target_spec)
        metadata = metadata_util.request_missing_metadata(MakeBundle, args)
        print client.derive_bundle('make', targets, None, metadata, worksheet_uuid)

    def desugar_command(self, target_spec, command):
        '''
        Desugar command, returning updated target_spec and command.
        Example: %corenlp%/run %a.txt% => [1:corenlp, 2:a.txt], 1/run 2
        '''
        pattern = re.compile('^([^%]*)%([^%]+)%(.*)$')
        buf = ''
        while True:
            m = pattern.match(command)
            if not m: break
            i = str(len(target_spec)+1)
            if ':' in m.group(2):
                i, val = m.group(2).split(':', 1)
                if i == '': i = val
                target_spec.append(m.group(2))
            else:
                target_spec.append(i + ':' + m.group(2))
            buf += m.group(1) + i
            command = m.group(3)
        return (target_spec, buf + command)

    # After running a bundle, we can wait for it, possibly observing it's output.
    # These functions are shared across run and mimic.
    def add_wait_args(self, parser):
        parser.add_argument('-W', '--wait', action='store_true', help='Wait until run finishes')
        parser.add_argument('-t', '--tail', action='store_true', help='Wait until run finishes, displaying output')
        parser.add_argument('-v', '--verbose', action='store_true', help='Display verbose output')
    def wait(self, client, args, uuid):
        if args.wait:
            state = self.follow_targets(client, uuid, [])
            self.do_info_command([uuid, '--verbose'], self.create_parser('info'))
        if args.tail:
            state = self.follow_targets(client, uuid, ['stdout', 'stderr'])
            if args.verbose:
                self.do_info_command([uuid, '--verbose'], self.create_parser('info'))

    def do_run_command(self, argv, parser):
        # Usually, the last argument is the command, but we use a special notation '---' to allow
        # us to specify the command across multiple tokens.
        #   key:target ... key:target "command_1 ... command_n"
        #   <==>
        #   key:target ... key:target --- command_1 ... command_n
        try:
            i = argv.index('---')
            argv = argv[0:i] + [' '.join(argv[i+1:])]  # TODO: quote command properly
        except:
            pass
        parser.add_argument('target_spec', help=self.TARGET_SPEC_FORMAT, nargs='*')
        parser.add_argument('command', help='Command-line')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        self.add_wait_args(parser)
        metadata_util.add_arguments(RunBundle, set(), parser)
        metadata_util.add_edit_argument(parser)
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        args.target_spec, args.command = self.desugar_command(args.target_spec, args.command)
        targets = self.parse_key_targets(client, worksheet_uuid, args.target_spec)
        metadata = metadata_util.request_missing_metadata(RunBundle, args)
        uuid = client.derive_bundle('run', targets, args.command, metadata, worksheet_uuid)
        print uuid
        self.wait(client, args, uuid)

    def do_edit_command(self, argv, parser):
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT)
        parser.add_argument('-n', '--name', help='new name: ' + spec_util.NAME_REGEX.pattern, nargs='?')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        self.add_wait_args(parser)
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        bundle_uuid = worksheet_util.get_bundle_uuid(client, worksheet_uuid, args.bundle_spec)
        info = client.get_bundle_info(bundle_uuid)
        bundle_subclass = get_bundle_subclass(info['bundle_type'])
        if args.name:
            # Just change the name
            new_metadata = info['metadata']
            new_metadata['name'] = args.name
            client.update_bundle_metadata(bundle_uuid, new_metadata)
        else:
            # Prompt user for all information
            new_metadata = metadata_util.request_missing_metadata(
              bundle_subclass,
              args,
              info['metadata'],
            )
            if new_metadata != info['metadata']:
                client.update_bundle_metadata(bundle_uuid, new_metadata)
                print "Saved metadata for bundle %s." % (bundle_uuid)

    def do_rm_command(self, argv, parser):
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT, nargs='+')
        parser.add_argument(
          '-f', '--force',
          action='store_true',
          help='delete bundle (DANGEROUS - breaking dependencies!)',
        )
        parser.add_argument(
          '-r', '--recursive',
          action='store_true',
          help='delete all bundles downstream that depend on this bundle',
        )
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        # Resolve all the bundles first, then delete (this is important since
        # some of the bundle specs are relative).
        bundle_uuids = [worksheet_util.get_bundle_uuid(client, worksheet_uuid, bundle_spec) for bundle_spec in args.bundle_spec]
        deleted_uuids = client.delete_bundles(bundle_uuids, args.force, args.recursive)
        for uuid in deleted_uuids: print uuid

    def do_search_command(self, argv, parser):
        parser.add_argument(
          'keywords',
          help='keywords to search for',
          nargs='+',
        )
        parser.add_argument(
          '-c', '--count',
          help='just count number of bundles',
          action='store_true'
        )
        parser.add_argument('-u', '--uuid-only', help='only print uuids', action='store_true')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        bundle_uuids = client.search_bundle_uuids(worksheet_uuid, args.keywords, 100, args.count)
        if args.uuid_only:
            bundle_info_list = [{'uuid': uuid} for uuid in bundle_uuids]
        else:
            bundle_infos = client.get_bundle_infos(bundle_uuids)
            bundle_info_list = [bundle_infos[uuid] for uuid in bundle_uuids]

        if len(bundle_info_list) > 0:
            self.print_bundle_info_list(bundle_info_list, uuid_only=args.uuid_only)
        else:
            if not args.uuid_only:
                print 'No search results for keywords: %s' % ' '.join(args.keywords)

    def do_ls_command(self, argv, parser):
        parser.add_argument('worksheet_spec', help='identifier: %s (default: current worksheet)' % self.GLOBAL_SPEC_FORMAT, nargs='?')
        parser.add_argument('-u', '--uuid-only', help='only print uuids', action='store_true')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        worksheet_info = client.get_worksheet_info(worksheet_uuid)
        bundle_info_list = self.get_worksheet_bundles(worksheet_info)
        if len(bundle_info_list) > 0:
            if not args.uuid_only:
                print 'Worksheet: %s' % self.worksheet_str(worksheet_info)
            self.print_bundle_info_list(bundle_info_list, args.uuid_only)
        else:
            if not args.uuid_only:
                print 'Worksheet %s(%s) is empty.' % (worksheet_info['name'], worksheet_info['uuid'])

    # Helper
    def print_bundle_info_list(self, bundle_info_list, uuid_only):
        if uuid_only:
            for bundle_info in bundle_info_list:
                print bundle_info['uuid']
        else:
            columns = ('uuid', 'name', 'bundle_type', 'created', 'data_size', 'state')
            post_funcs = {'created': 'date', 'data_size': 'size'}
            justify = {'data_size': 1}
            bundle_dicts = [
              {col: info.get(col, info['metadata'].get(col)) for col in columns}
              for info in bundle_info_list
            ]
            self.print_table(columns, bundle_dicts, post_funcs=post_funcs, justify=justify)

    def do_info_command(self, argv, parser):
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT, nargs='+')
        parser.add_argument('-f', '--field', help='print out these fields', nargs='?')
        parser.add_argument('-r', '--raw', action='store_true', help='print out raw information (no rendering)')
        parser.add_argument('-c', '--children', action='store_true', help="print only a list of this bundle's children")
        parser.add_argument('-v', '--verbose', action='store_true', help="print top-level contents of bundle")
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        for i, bundle_spec in enumerate(args.bundle_spec):
            bundle_uuid = worksheet_util.get_bundle_uuid(client, worksheet_uuid, bundle_spec)
            info = client.get_bundle_info(bundle_uuid, args.children)

            if args.field:
                # Display a single field (arbitrary genpath)
                genpath = args.field
                if worksheet_util.is_file_genpath(genpath):
                    value = worksheet_util.interpret_file_genpath(client, {}, bundle_uuid, genpath)
                else:
                    value = worksheet_util.interpret_genpath(info, genpath)
                print value
            else:
                # Display all the fields
                if i > 0: print
                self.print_basic_info(client, info, args.raw)
                if args.children: self.print_children(info)
                if args.verbose: self.print_contents(client, info)

    def print_basic_info(self, client, info, raw):
        def key_value_str(key, value):
            return '%-16s: %s' % (key, value if value != None else '<none>')

        metadata = info['metadata']
        lines = []  # The output that we're accumulating

        # Bundle fields
        for key in ('bundle_type', 'uuid', 'data_hash', 'state', 'failure_message', 'command', 'owner_id'):
            if not raw:
                if key not in info: continue
            lines.append(key_value_str(key, info.get(key)))

        # Metadata fields (standard)
        cls = get_bundle_subclass(info['bundle_type'])
        for spec in cls.METADATA_SPECS:
            key = spec.key
            if not raw:
                if key not in metadata: continue
                if metadata[key] == '' or metadata[key] == []: continue
                value = worksheet_util.apply_func(spec.formatting, metadata.get(key))
                if isinstance(value, list): value = ' | '.join(value)
            else:
                value = metadata.get(key)
            lines.append(key_value_str(key, value))

        # Metadata fields (non-standard)
        standard_keys = set(spec.key for spec in cls.METADATA_SPECS)
        for key, value in metadata.items():
            if key in standard_keys: continue
            lines.append(key_value_str(key, value))

        # Dependencies (both hard dependencies and soft)
        def display_dependencies(label, deps):
            lines.append(label + ':')
            for dep in sorted(deps, key=lambda dep: dep['child_path']):
                child = dep['child_path']
                parent = path_util.safe_join((dep['parent_name'] or 'MISSING') + '(' + dep['parent_uuid'] + ')', dep['parent_path'])
                lines.append('  %s: %s' % (child, parent))
        if info['hard_dependencies']:
            deps = info['hard_dependencies']
            if len(deps) == 1 and not deps[0]['child_path']:
                display_dependencies('hard_dependency', deps)
            else:
                display_dependencies('hard_dependencies', deps)
        elif info['dependencies']:
            deps = info['dependencies']
            display_dependencies('dependencies', deps)

        print '\n'.join(lines)

    def print_children(self, info):
        if not info['children']: return
        print 'children:'
        for child in info['children']:
            print "  %s" % child

    def print_contents(self, client, info):
        def wrap(string): return '=== ' + string + ' ==='
        print wrap('contents')
        bundle_uuid = info['uuid']
        info = self.print_target_info(client, (bundle_uuid, ''), decorate=True)
        # Print first 10 lines of stdout and stderr
        contents = info.get('contents')
        if contents:
            for item in contents:
                if item['name'] not in ['stdout', 'stderr']: continue
                print wrap(item['name'])
                self.print_target_info(client, (bundle_uuid, item['name']), decorate=True)

    def do_cat_command(self, argv, parser):
        parser.add_argument('target_spec', help=self.TARGET_SPEC_FORMAT)
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        target = self.parse_target(client, worksheet_uuid, args.target_spec)
        self.print_target_info(client, target, decorate=False)

    # Helper: shared between info and cat
    def print_target_info(self, client, target, decorate):
        info = client.get_target_info(target, 1)
        if 'type' not in info:
            self.exit('Target doesn\'t exist: %s/%s' % target)
        if info['type'] == 'file':
            if decorate:
                for line in client.head_target(target, 10):
                    print line,
            else:
                client.cat_target(target, sys.stdout)
        def size(x):
            t = x.get('type', 'MISSING')
            if t == 'file': return formatting.size_str(x['size'])
            if t == 'directory': return 'dir'
            return t
        if info['type'] == 'directory':
            contents = [
                {'name': x['name'], 'size': size(x)}
                for x in info['contents']
            ]
            contents = sorted(contents, key=lambda r : r['name'])
            self.print_table(('name', 'size'), contents, justify={'size':1}, indent='')
        return info

    def do_wait_command(self, argv, parser):
        parser.add_argument(
          'target_spec',
          help=self.TARGET_SPEC_FORMAT
        )
        parser.add_argument(
          '-t', '--tail',
          action='store_true',
          help="print out the tail of the file or bundle and block until the bundle is done"
        )
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        target = self.parse_target(client, worksheet_uuid, args.target_spec)
        (bundle_uuid, subpath) = target

        # Figure files to display
        subpaths = []
        if args.tail:
            if subpath == '':
                subpaths = ['stdout', 'stderr']
            else:
                subpaths = [subpath]
        state = self.follow_targets(client, bundle_uuid, subpaths)
        if state != State.READY:
            self.exit(state)

    def follow_targets(self, client, bundle_uuid, subpaths):
        '''
        Block on the execution of the given bundle.
        subpaths: list of files to print out output as we go along.
        Return READY or FAILED based on whether it was computed successfully.
        '''
        handles = [None] * len(subpaths)

        # Constants for a simple exponential backoff routine that will decrease the
        # frequency at which we check this bundle's state from 1s to 1m.
        period = 1.0
        backoff = 1.1
        max_period = 60.0
        info = None
        while True:
            # Call update functions
            change = False
            for i, handle in enumerate(handles):
                if not handle:
                    handle = handles[i] = client.open_target_handle((bundle_uuid, subpaths[i]))
                    if not handle: continue
                    # Go to near the end of the file (TODO: make this match up with lines)
                    pos = max(handle.tell() - 64, 0)
                    handle.seek(pos, 0)
                # Read from that file
                while True:
                    result = handle.read(16384)
                    if result == '': break
                    change = True
                    sys.stdout.write(result)
            sys.stdout.flush()

            # Update bundle info
            info = client.get_bundle_info(bundle_uuid)
            if info['state'] in (State.READY, State.FAILED): break

            # Sleep if nothing happened
            if not change:
                time.sleep(period)
                period = min(backoff*period, max_period)

        for handle in handles:
            if not handle: continue
            # Read the remainder of the file
            while True:
                result = handle.read(16384)
                if result == '': break
                sys.stdout.write(result)
            client.close_target_handle(handle)

        return info['state']

    def do_mimic_command(self, argv, parser):
        parser.add_argument(
          'bundles',
          help="old_input_1 ... old_input_n old_output new_input_1 ... new_input_n (%s)" % self.BUNDLE_SPEC_FORMAT,
          nargs='+'
        )
        self.add_mimic_args(parser)
        args = parser.parse_args(argv)
        self.mimic(args)

    def do_macro_command(self, argv, parser):
        '''
        Just like do_mimic_command.
        '''
        parser.add_argument(
          'macro_name',
          help='name of the macro (look for <macro_name>-in1, ..., and <macro_name>-out bundles)',
        )
        parser.add_argument(
          'bundles',
          help="new_input_1 ... new_input_n (bundles %s)" % self.BUNDLE_SPEC_FORMAT,
          nargs='+'
        )
        self.add_mimic_args(parser)
        args = parser.parse_args(argv)
        # For a macro, it's important that the name be not-null, so that we
        # don't create bundles called '<macro_name>-out', which would clash
        # next time we try to use the macro.
        if not args.name: args.name = 'new'
        # Reduce to the mimic case
        args.bundles = [args.macro_name + '-in' + str(i+1) for i in range(len(args.bundles))] + \
                       [args.macro_name + '-out'] + args.bundles
        self.mimic(args)

    def add_mimic_args(self, parser):
        parser.add_argument('-n', '--name', help='name of the output bundle')
        parser.add_argument('-d', '--depth', type=int, default=10, help="number of parents to look back from the old output in search of the old input")
        parser.add_argument('-s', '--shadow', action='store_true', help="add the newly created bundles right after the old bundles that are being mimicked")
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        self.add_wait_args(parser)

    def mimic(self, args):
        '''
        Use args.bundles to generate a mimic call to the BundleClient.
        '''
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        bundle_uuids = [worksheet_util.get_bundle_uuid(client, worksheet_uuid, spec) for spec in args.bundles]

        # Two cases for args.bundles
        # (A) old_input_1 ... old_input_n            new_input_1 ... new_input_n [go to all outputs]
        # (B) old_input_1 ... old_input_n old_output new_input_1 ... new_input_n [go from inputs to given output]
        n = len(bundle_uuids) / 2
        if len(bundle_uuids) % 2 == 0:  # (A)
            old_inputs = bundle_uuids[0:n]
            old_output = None
            new_inputs = bundle_uuids[n:]
        else: # (B)
            old_inputs = bundle_uuids[0:n]
            old_output = bundle_uuids[n]
            new_inputs = bundle_uuids[n+1:]

        new_uuid = client.mimic(
            old_inputs, old_output, new_inputs, args.name,
            worksheet_uuid, args.depth, args.shadow)
        self.wait(client, args, new_uuid)
        print new_uuid

    def do_kill_command(self, argv, parser):
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT, nargs='+')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        bundle_uuids = []
        for bundle_spec in args.bundle_spec:
            bundle_uuid = worksheet_util.get_bundle_uuid(client, worksheet_uuid, bundle_spec)
            bundle_uuids.append(bundle_uuid)
            print bundle_uuid
        client.kill_bundles(bundle_uuids)

    #############################################################################
    # CLI methods for worksheet-related commands follow!
    #############################################################################

    def get_current_worksheet_info(self):
        '''
        Return the current worksheet's info, or None, if there is none.
        '''
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        return client.get_worksheet_info(worksheet_uuid)

    def worksheet_str(self, worksheet_info):
        return '%s::%s(%s)' % (self.manager.session()['address'], worksheet_info['name'], worksheet_info['uuid'])

    def do_new_command(self, argv, parser):
        # TODO: This command is a bit dangerous because we easily can create a
        # worksheet with the same name.  Need a way to organize worksheets by a
        # given user.
        parser.add_argument('name', help='name: ' + spec_util.NAME_REGEX.pattern)
        args = parser.parse_args(argv)

        client = self.manager.current_client()
        uuid = client.new_worksheet(args.name)
        self.manager.set_current_worksheet_uuid(client, uuid)
        worksheet_info = client.get_worksheet_info(uuid)
        print 'Created and switched to worksheet %s.' % (self.worksheet_str(worksheet_info))

    def do_add_command(self, argv, parser):
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT, nargs='+')
        parser.add_argument('-m', '--message', help='add a text element', nargs='?')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        for spec in args.bundle_spec:
            bundle_uuid = worksheet_util.get_bundle_uuid(client, worksheet_uuid, spec)
            client.add_worksheet_item(worksheet_uuid, (bundle_uuid, None, worksheet_util.TYPE_BUNDLE))
        if args.message != None:
            if args.message.startswith('%'):
                client.add_worksheet_item(worksheet_uuid, (None, args.message[1:].strip(), worksheet_util.TYPE_DIRECTIVE))
            else:
                client.add_worksheet_item(worksheet_uuid, (None, args.message, worksheet_util.TYPE_MARKUP))

    def do_work_command(self, argv, parser):
        parser.add_argument(
          'worksheet_spec',
          help=self.WORKSHEET_SPEC_FORMAT,
          nargs='?',
        )
        args = parser.parse_args(argv)
        if args.worksheet_spec:
            client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
            worksheet_info = client.get_worksheet_info(worksheet_uuid)  # Replace with something lightweighter
            self.manager.set_current_worksheet_uuid(client, worksheet_uuid)
            print 'Switched to worksheet %s.' % (self.worksheet_str(worksheet_info))
        else:
            worksheet_info = self.get_current_worksheet_info()
            if worksheet_info:
                print 'Currently on worksheet %s.' % (self.worksheet_str(worksheet_info))
            else:
                print 'Not on any worksheet. Use `cl new` or `cl work` to switch to one.'

    def do_wedit_command(self, argv, parser):
        parser.add_argument('worksheet_spec', help=self.GLOBAL_SPEC_FORMAT, nargs='?')
        parser.add_argument('-n', '--name', help='new name: ' + spec_util.NAME_REGEX.pattern, nargs='?')
        parser.add_argument('-f', '--file', help='overwrite the given worksheet with this file', nargs='?')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        worksheet_info = client.get_worksheet_info(worksheet_uuid)
        if args.name:
            client.rename_worksheet(worksheet_info['uuid'], args.name)
        else:
            # Either get a list of lines from the given file or request it from the user in an editor.
            if args.file:
                lines = [line.strip() for line in open(args.file).readlines()]
            else:
                lines = worksheet_util.request_lines(worksheet_info, client)

            # Parse the lines.
            new_items, commands = worksheet_util.parse_worksheet_form(lines, client, worksheet_info['uuid'])

            # Save the worksheet.
            client.update_worksheet(worksheet_info, new_items)
            print 'Saved worksheet %s(%s).' % (worksheet_info['name'], worksheet_info['uuid'])

            # Batch the rm commands so that we can handle the recursive
            # dependencies properly (and it's faster).
            rm_bundle_uuids = []
            rest_commands = []
            for command in commands:
                if command[0] == 'rm' and len(command) == 2:
                    rm_bundle_uuids.append(command[1])
                else:
                    rest_commands.append(command)
            commands = rest_commands
            if len(rm_bundle_uuids) > 0:
                commands.append(['rm'] + rm_bundle_uuids)

            # Execute the commands that the user put into the worksheet.
            for command in commands:
                # Make sure to do it with respect to this worksheet!
                spec = client.address + '::' + worksheet_uuid
                if command[0] in ('ls', 'print'):
                    command.append(spec)
                else:
                    command.extend(['--worksheet_spec', spec])
                print '=== Executing: %s' % ' '.join(command)
                self.do_command(command)


    def do_print_command(self, argv, parser):
        parser.add_argument('worksheet_spec', help=self.GLOBAL_SPEC_FORMAT, nargs='?')
        parser.add_argument('-r', '--raw', action='store_true', help='print out the raw contents')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        worksheet_info = client.get_worksheet_info(worksheet_uuid)
        if args.raw:
            lines = worksheet_util.get_worksheet_lines(worksheet_info)
            for line in lines:
                print line
        else:
            interpreted = worksheet_util.interpret_items(worksheet_util.get_default_schemas(), worksheet_info['items'])
            self.display_interpreted(client, worksheet_info, interpreted)

    def display_interpreted(self, client, worksheet_info, interpreted):
        title = interpreted.get('title')
        if title:
            print '[[', title, ']]'
        is_last_newline = False
        for item in interpreted['items']:
            mode = item['mode']
            data = item['interpreted']
            is_newline = (data == '')
            if mode == 'inline' or mode == 'markup' or mode == 'contents':
                if not (is_newline and is_last_newline):
                    if mode == 'inline':
                        if isinstance(data, tuple):
                            data = client.interpret_file_genpaths([data])[0]
                        print '[' + str(data) + ']'
                    elif mode == 'contents':
                        self.print_target_info(client, data, decorate=True)
                    else:
                        print data
            elif mode == 'record' or mode == 'table':
                # header_name_posts is a list of (name, post-processing) pairs.
                header, contents = data

                # Request information
                requests = []
                for r, row in enumerate(contents):
                    for key, value in row.items():
                        # value can be either a string (already rendered) or a (bundle_uuid, genpath, post) triple
                        if isinstance(value, tuple):
                            requests.append(value)
                responses = client.interpret_file_genpaths(requests)

                # Put it in a table
                new_contents = []
                ri = 0
                for r, row in enumerate(contents):
                    new_row = {}
                    for key, value in row.items():
                        if isinstance(value, tuple):
                            value = responses[ri]
                            ri += 1
                        new_row[key] = value
                    new_contents.append(new_row)
                contents = new_contents
                    
                # Print the table
                self.print_table(header, contents, show_header=(mode == 'table'), indent='  ')
            elif mode == 'html' or mode == 'image':
                # Placeholder
                print '[' + mode + ' ' + str(data) + ']'
            elif mode == 'search':
                search_interpreted = worksheet_util.interpret_search(client, worksheet_info['uuid'], data)
                self.display_interpreted(client, worksheet_info, search_interpreted)
            else:
                raise UsageError('Invalid display mode: %s' % mode)
            is_last_newline = is_newline

    def do_wls_command(self, argv, parser):
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        worksheet_dicts = client.list_worksheets()
        if worksheet_dicts:
            self.print_table(('uuid', 'name'), worksheet_dicts)
        else:
            print 'No worksheets found.'

    def do_wrm_command(self, argv, parser):
        parser.add_argument('worksheet_spec', help='identifier: [<uuid>|<name>]')
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        client.delete_worksheet(args.worksheet_spec)

    def do_wcp_command(self, argv, parser):
        parser.add_argument(
          'source_worksheet_spec',
          help=self.WORKSHEET_SPEC_FORMAT,
          nargs='?',
        )
        parser.add_argument(
          'dest_worksheet_spec',
          help='%s (default: current worksheet)' % self.WORKSHEET_SPEC_FORMAT,
          nargs='?',
        )
        args = parser.parse_args(argv)

        # Source worksheet
        (source_client, source_worksheet_uuid) = self.parse_client_worksheet_uuid(args.source_worksheet_spec)
        items = source_client.get_worksheet_info(source_worksheet_uuid)['items']

        # Destination worksheet
        (dest_client, dest_worksheet_uuid) = self.parse_client_worksheet_uuid(args.dest_worksheet_spec)

        for item in items:
            (source_bundle_info, value_obj, type) = item
            if source_bundle_info != None:
                # Copy bundle
                self.copy_bundle(source_client, source_bundle_info['uuid'], dest_client, dest_worksheet_uuid)
            else:
                # Copy non-bundle
                dest_client.add_worksheet_item(dest_worksheet_uuid, worksheet_util.convert_item_to_db(item))

        print 'Copied %s worksheet items to %s.' % (len(items), dest_worksheet_uuid)


    #############################################################################
    # CLI methods for commands related to groups and permissions follow!
    #############################################################################

    def do_list_groups_command(self, argv, parser):
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        group_dicts = client.list_groups()
        if group_dicts:
            self.print_table(('name', 'uuid', 'role'), group_dicts)
        else:
            print 'No groups found.'

    def do_new_group_command(self, argv, parser):
        parser.add_argument('name', help='name: ' + spec_util.NAME_REGEX.pattern)
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        group_dict = client.new_group(args.name)
        print 'Created new group %s(%s).' % (group_dict['name'], group_dict['uuid'])

    def do_rm_group_command(self, argv, parser):
        parser.add_argument('group_spec', help='group identifier: [<uuid>|<name>]')
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        group_dict = client.rm_group(args.group_spec)
        print 'Deleted group %s(%s).' % (group_dict['name'], group_dict['uuid'])

    def do_group_info_command(self, argv, parser):
        parser.add_argument('group_spec', help='group identifier: [<uuid>|<name>]')
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        group_dict = client.group_info(args.group_spec)
        #print 'Listing members of group %s (%s):\n' % (group_dict['name'], group_dict['uuid'])
        self.print_table(('name', 'role'), group_dict['members'])

    def do_add_user_command(self, argv, parser):
        parser.add_argument('user_spec', help='username')
        parser.add_argument('group_spec', help='group identifier: [<uuid>|<name>]')
        parser.add_argument('-a', '--admin', action='store_true',
                            help='grant admin privileges for the group')
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        user_info = client.add_user(args.user_spec, args.group_spec, args.admin)
        if 'operation' in user_info:
            print '%s %s %s group %s' % (user_info['operation'],
                                         user_info['name'],
                                         'to' if user_info['operation'] == 'Added' else 'in',
                                         user_info['group_uuid'])
        else:
            print '%s is already in group %s' % (user_info['name'], user_info['group_uuid'])

    def do_rm_user_command(self, argv, parser):
        parser.add_argument('user_spec', help='username')
        parser.add_argument('group_spec', help='group identifier: [<uuid>|<name>]')
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        user_info = client.rm_user(args.user_spec, args.group_spec)
        if user_info is None:
            print '%s is not a member of group %s.' % (user_info['name'], user_info['group_uuid'])
        else:
            print 'Removed %s from group %s.' % (user_info['name'], user_info['group_uuid'])

    def do_set_perm_command(self, argv, parser):
        parser.add_argument('worksheet_spec', help='worksheet identifier: [<uuid>|<name>]')
        parser.add_argument('permission', help='permission: [none|(r)ead|(a)ll]')
        parser.add_argument('group_spec', help='group identifier: [<uuid>|<name>|public]')
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        result = client.set_worksheet_perm(args.worksheet_spec, args.permission, args.group_spec)
        permission_code = result['permission']
        permission_label = 'no'
        from codalab.model.tables import (
            GROUP_OBJECT_PERMISSION_ALL,
            GROUP_OBJECT_PERMISSION_READ,
        )
        if permission_code == GROUP_OBJECT_PERMISSION_READ:
            permission_label = 'read'
        elif permission_code == GROUP_OBJECT_PERMISSION_ALL:
            permission_label = 'full'
        print "Group %s (%s) has %s permission on worksheet %s (%s)." % \
            (result['group_info']['name'], result['group_info']['uuid'],
             permission_label,
             result['worksheet']['name'], result['worksheet']['uuid'])

    #############################################################################
    # LocalBundleClient-only commands follow!
    #############################################################################

    def do_worker_command(self, argv, parser):
        # This command only works if client is a LocalBundleClient.
        parser.add_argument('--num-iterations', help="number of bundles to process before exiting", type=int, default=None)
        parser.add_argument('--sleep-time', type=int, help='Number of seconds to wait between successive polls', default=1)
        parser.add_argument('-t', '--worker-type', type=str, help="worker type (defined in config.json)", default='local')
        parser.add_argument('-p', '--parallelism', type=int, help="number of bundles we can run at once", default=1)
        args = parser.parse_args(argv)

        # Figure out machine settings
        worker_config = self.manager.config['workers']
        if args.worker_type in worker_config:
            config = worker_config[args.worker_type]
        else:
            print '\'' + args.worker_type + '\'' + \
                  ' is not specified in your config file: ' + self.manager.config_path()
            print 'Options are ' + str(map(str, worker_config.keys()))
            return

        if config['type'] == 'local':
            construct_func = lambda : local_machine.LocalMachine()
        elif config['type'] == 'remote':
            construct_func = lambda : remote_machine.RemoteMachine(config['host'], config['user'], config['working_directory'], config['verbose'])
        machine = pool_machine.PoolMachine(construct_func=construct_func, limit=args.parallelism)

        client = self.manager.current_client()
        worker = Worker(client.bundle_store, client.model, machine)
        worker.run_loop(args.num_iterations, args.sleep_time)

    def do_cleanup_command(self, argv, parser):
        # This command only works if client is a LocalBundleClient.
        '''
        Removes data hash directories which are not used by any bundle.
        '''
        parser.parse_args(argv)
        client = self.manager.current_client()
        client.bundle_store.full_cleanup(client.model)

    def do_reset_command(self, argv, parser):
        # This command only works if client is a LocalBundleClient.
        parser.add_argument(
          '--commit',
          action='store_true',
          help='reset is a no-op unless committed',
        )
        args = parser.parse_args(argv)
        if not args.commit:
            raise UsageError('If you really want to delete EVERYTHING, use --commit')
        client = self.manager.current_client()
        print 'Deleting entire bundle store...'
        client.bundle_store._reset()
        print 'Deleting entire database...'
        client.model._reset()
