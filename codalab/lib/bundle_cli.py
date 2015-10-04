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
import copy
import itertools
import os
import re
import sys
import time
import tempfile

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
  cli_util,
  canonicalize,
  formatting
)
from codalab.objects.permission import permission_str, group_permissions_str
from codalab.objects.worksheet import Worksheet
from codalab.objects.work_manager import Worker
from codalab.machines.remote_machine import RemoteMachine
from codalab.machines.local_machine import LocalMachine
from codalab.client.remote_bundle_client import RemoteBundleClient
from codalab.lib.formatting import contents_str

class BundleCLI(object):
    DESCRIPTIONS = {
      # Commands for bundles.
      'upload': 'Create a bundle by uploading an existing file/directory.',
      'make': 'Create a bundle out of existing bundles.',
      'run': 'Create a bundle by running a program bundle on an input bundle.',
      'edit': "Edit an existing bundle's metadata.",
      'detach': 'Detach a bundle from this worksheet, but doesn\'t remove the bundle.',
      'rm': 'Remove a bundle (permanent!).',
      'search': 'Search for bundles in the system.',
      'ls': 'List bundles in a worksheet.',
      'info': 'Show detailed information for a bundle.',
      'cat': 'Print the contents of a file/directory in a bundle.',
      'wait': 'Wait until a bundle finishes.',
      'download': 'Download bundle from an instance.',
      'cp': 'Copy bundles across instances.',
      'mimic': 'Creates a set of bundles based on analogy with another set.',
      'macro': 'Use mimicry to simulate macros.',
      'kill': 'Instruct the worker to terminate a running bundle.',
      # Commands for worksheets.
      'new': 'Create a new worksheet and add it to the current worksheet.',
      'add': 'Append a bundle to a worksheet.',
      'work': 'Set the current instance/worksheet.',
      'print': 'Print the contents of a worksheet.',
      'wedit': 'Edit the contents of a worksheet.',
      'wadd': 'Append a worksheet to a worksheet.',
      'wrm': 'Delete a worksheet.',
      'wls': 'List all worksheets.',
      'wcp': 'Copy the contents from one worksheet to another.',
      # Commands for groups and permissions.
      'gls': 'Show groups to which you belong.',
      'gnew': 'Create a new group.',
      'grm': 'Delete a group.',
      'ginfo': 'Show detailed information for a group.',
      'uadd': 'Add a user to a group.',
      'urm': 'Remove a user from a group.',
      'perm': 'Set a group\'s permissions for a bundle.',
      'wperm': 'Set a group\'s permissions for a worksheet.',
      'chown': 'Set the owner of bundles.',
      # Commands that can only be executed on a LocalBundleClient.
      'help': 'Show a usage message for cl or for a particular command.',
      'status': 'Show current client status.',
      'logout': 'Logout of the current worksheet.',
      'alias': 'Manage CodaLab instance aliases.',
      'work-manager': 'Run the CodaLab bundle work manager.',
      # Internal commands wihch are used for debugging.
      'events': 'Print the history of commands on this CodaLab instance (local only).',
      'cleanup': 'Clean up the CodaLab bundle store (local only).',
      'reset': 'Delete the CodaLab bundle store and reset the database (local only).',
      # Note: this is not actually handled in BundleCLI, but here just to show the help
      'server': 'Start an instance of the CodaLab server.',
    }

    BUNDLE_COMMANDS = (
        'upload',
        'make',
        'run',
        'edit',
        'detach',
        'rm',
        'search',
        'ls',
        'info',
        'cat',
        'wait',
        'download',
        'cp',
        'mimic',
        'macro',
        'kill',
    )

    WORKSHEET_COMMANDS = (
        'new',
        'add',
        'work',
        'print',
        'wedit',
        'wadd',
        'wrm',
        'wls',
        'wcp',
    )

    GROUP_AND_PERMISSION_COMMANDS = (
        'gls',
        'gnew',
        'grm',
        'ginfo',
        'uadd',
        'urm',
        'perm',
        'wperm',
        'chown',
    )

    OTHER_COMMANDS = (
        'help',
        'status',
        'alias',
        'work-manager',
        'server',
    )

    SHORTCUTS = {
        'up': 'upload',
        'down': 'download',
        'w': 'work',
        'p': 'print',
        'i': 'info',
        'e': 'edit',
        'de': 'detach',
        'we': 'wedit',
        's': 'search',
        'st': 'status',
    }

    def __init__(self, manager, headless=False):
        self.manager = manager
        self.verbose = manager.cli_verbose()
        self.headless = headless

    def exit(self, message, error_code=1):
        '''
        Print the message to stderr and exit with the given error code.
        '''
        precondition(error_code, 'exit called with error_code == 0')
        print >>sys.stderr, message
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

    def simple_bundle_str(self, info):
        return '%s(%s)' % (info.get('metadata', {}).get('name', 'MISSING'), info['uuid'])
    def simple_worksheet_str(self, info):
        return '%s(%s)' % (info.get('name', 'MISSING'), info['uuid'])
    def simple_user_str(self, info):
        return '%s(%s)' % (info.get('name', 'MISSING'), info['id'])

    def get_worksheet_bundles(self, worksheet_info):
        '''
        Return list of info dicts of distinct bundles in the worksheet.
        '''
        result = []
        for (bundle_info, subworksheet_info, value_obj, item_type) in worksheet_info['items']:
            if item_type == worksheet_util.TYPE_BUNDLE:
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
        targets = []
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
            targets.append((key, self.parse_target(client, worksheet_uuid, target)))
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
                if cell == None: cell = 'MISSING'
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
            if show_header or i > 0:
                print indent + '  '.join(row_strs)
            if i == 0:
                print indent + (sum(lengths) + 2*(len(columns) - 1)) * '-'

    GLOBAL_SPEC_FORMAT = "[<alias>::|<address>::]|(<uuid>|<name>)"
    ADDRESS_SEPC_FORMAT = "(<alias>|<address>)"
    TARGET_SPEC_FORMAT = '[<key>:](<uuid>|<name>)[%s<subpath within bundle>]' % (os.sep,)
    BUNDLE_SPEC_FORMAT = '(<uuid>|<name>|^<index>)'
    WORKSHEET_SPEC_FORMAT = GLOBAL_SPEC_FORMAT
    GROUP_SPEC_FORMAT = '(<uuid>|<name>|public)'
    PERMISSION_SPEC_FORMAT = '((n)one|(r)ead|(a)ll)'

    UUID_POST_FUNC = '[0:8]'  # Only keep first 8 characters

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
        return (self.manager.client(address), spec)

    def parse_client_worksheet_uuid(self, spec):
        '''
        Return the worksheet referred to by |spec|.
        '''
        if not spec or spec == '.':
            # Empty spec, just return current worksheet.
            client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        else:
            client_is_explicit = spec_util.client_is_explicit(spec)
            client, spec = self.parse_spec(spec)
            # If we're on the same client, then resolve spec with respect to
            # the current worksheet.
            if client_is_explicit:
                base_worksheet_uuid = None
            else:
                _, base_worksheet_uuid = self.manager.get_current_worksheet_uuid()
            worksheet_uuid = worksheet_util.get_worksheet_uuid(client, base_worksheet_uuid, spec)
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
            message = "'%s' is not a CodaLab command. Try 'cl help'." % (command,)
            if self.headless: raise UsageError(message)
            self.exit(message)
        parser = self.create_parser(command)
        if self.verbose >= 2:
            command_fn(remaining_args, parser)
        else:
            message = None
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
            except PermissionError, e:
                if self.headless: raise e
                self.exit(e.message)
            except UsageError, e:
                if self.headless: raise e
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
        self._fail_if_headless('status')
        print "codalab_home: %s" % self.manager.codalab_home()
        print "session: %s" % self.manager.session_name()
        address = self.manager.session()['address']
        print "address: %s" % address
        state = self.manager.state['auth'].get(address, {})
        if 'username' in state:
            print "username: %s" % state['username']
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        worksheet_info = client.get_worksheet_info(worksheet_uuid, False)
        print "worksheet: %s" % self.simple_worksheet_str(worksheet_info)
        print "user: %s" % self.simple_user_str(client.user_info(None))

    def do_logout_command(self, argv, parser):
        self._fail_if_headless('logout')
        client = self.manager.current_client()
        self.manager.logout(client)

    def do_alias_command(self, argv, parser):
        '''
        Show, add, modify, delete aliases (mappings from names to instances).
        Only modifies the CLI configuration, doesn't need a BundleClient.
        '''
        self._fail_if_headless('alias')
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
        parser.add_argument('path', help='path(s) of the file/directory to upload', nargs='*')
        parser.add_argument('-b', '--base', help='Inherit the metadata from this bundle specification.')
        parser.add_argument('-B', '--base-use-default-name', help='Inherit the metadata from the bundle with the same name as the path.', action='store_true')
        parser.add_argument('-L', '--follow-symlinks', help='always dereference symlinks', action='store_true')
        parser.add_argument('-x', '--exclude-patterns', help='exclude these file patterns', nargs='*')
        parser.add_argument('-c', '--contents', help='specify the contents of the bundle')
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
        if args.bundle_type not in ['program', 'dataset']:
            raise UsageError("Expected bundle type 'program' or 'dataset', but got '%s'" % args.bundle_type)

        # Check that the upload path exists.
        for path in args.path:
            if not path_util.path_is_url(path):
                self._fail_if_headless('upload')  # Important: don't allow uploading files if headless.
                path_util.check_isvalid(path_util.normalize(path), 'upload')

        # If contents of file are specified on the command-line, then just use that.
        if args.contents:
            if not args.md_name:
                args.md_name = 'contents'
            tmp_path = tempfile.mkstemp()[1]
            f = open(tmp_path, 'w')
            print >>f, args.contents
            f.close()
            args.path.append(tmp_path)

        if len(args.path) == 0:
            raise UsageError('Nothing to upload')

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
        # pass in a null owner to validate. Will be set to the correct owner in the client upload_bundle below.
        bundle_subclass.construct(owner_id=0, data_hash='', metadata=metadata).validate()

        # If only one path, strip away the list so that we make a bundle that
        # is this path rather than contains it.
        if len(args.path) == 1: args.path = args.path[0]

        # Finally, once everything has been checked, then call the client to upload.
        print client.upload_bundle(args.path, {'bundle_type': args.bundle_type, 'metadata': metadata}, worksheet_uuid, args.follow_symlinks, args.exclude_patterns, True)

        # Clean up if necessary (might have been deleted if we put it in the CodaLab temp directory)
        if args.contents:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def do_download_command(self, argv, parser):
        self._fail_if_headless('download')
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
            print 'Local file/directory', local_dir, 'already exists.'
            return

        # Download first to a local location path.  Note: don't follow symlinks.
        local_path, temp_path = client.download_target(target, False)
        path_util.copy(local_path, final_path, follow_symlinks=False)
        if temp_path:
          path_util.remove(temp_path)
        print 'Downloaded %s to %s.' % (self.simple_bundle_str(info), final_path)

    def do_cp_command(self, argv, parser):
        parser.add_argument('-d', '--copy-dependencies', help='Whether to copy dependencies of the bundles.', action='store_true')
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT, nargs='+')
        parser.add_argument('worksheet_spec', help='%s (copy to this worksheet)' % self.WORKSHEET_SPEC_FORMAT)
        args = parser.parse_args(argv)
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()

        # Source bundle
        source_bundle_uuids = []
        for bundle_spec in args.bundle_spec:
            (source_client, source_spec) = self.parse_spec(bundle_spec)
            # worksheet_uuid is only applicable if we're on the source client
            if source_client != client: worksheet_uuid = None
            source_bundle_uuid = worksheet_util.get_bundle_uuid(source_client, worksheet_uuid, source_spec)
            source_bundle_uuids.append(source_bundle_uuid)

        # Destination worksheet
        (dest_client, dest_worksheet_uuid) = self.parse_client_worksheet_uuid(args.worksheet_spec)

        # Copy!
        for source_bundle_uuid in source_bundle_uuids:
            self.copy_bundle(source_client, source_bundle_uuid, dest_client, dest_worksheet_uuid, copy_dependencies=args.copy_dependencies, add_to_worksheet=True)

    def copy_bundle(self, source_client, source_bundle_uuid, dest_client, dest_worksheet_uuid, copy_dependencies, add_to_worksheet):
        '''
        Helper function that supports cp and wcp.
        Copies the source bundle to the target worksheet.
        Currently, this goes between two clients by downloading to the local
        disk and then uploading, which is not the most efficient.
        But having two clients talk directly to each other is complicated...
        '''
        if copy_dependencies:
            source_info = source_client.get_bundle_info(source_bundle_uuid)
            # Copy all the dependencies, but only for run dependencies.
            for dep in source_info['dependencies']:
                self.copy_bundle(source_client, dep['parent_uuid'], dest_client, dest_worksheet_uuid, False, add_to_worksheet)
            self.copy_bundle(source_client, source_bundle_uuid, dest_client, dest_worksheet_uuid, False, add_to_worksheet)
            return

        # Check if the bundle already exists on the destination, then don't copy it
        # (although metadata could be different on source and destination).
        bundle = None
        try:
            bundle = dest_client.get_bundle_info(source_bundle_uuid)
        except:
            pass

        source_info = source_client.get_bundle_info(source_bundle_uuid)
        source_desc = self.simple_bundle_str(source_info)
        if not bundle:
            if source_info['state'] not in [State.READY, State.FAILED]:
                print 'Not copying %s because it has non-final state %s' % (source_desc, source_info['state'])
            else:
                print "Copying %s..." % source_desc
                info = source_client.get_bundle_info(source_bundle_uuid)

                if isinstance(source_client, RemoteBundleClient) and isinstance(dest_client, RemoteBundleClient):
                    # If copying from remote to remote, can streamline the process
                    source_client.copy_bundle(source_bundle_uuid, info, dest_client, dest_worksheet_uuid, add_to_worksheet)
                else:
                    # Download from source
                    if source_info['data_hash']:
                        source_path, temp_path = source_client.download_target((source_bundle_uuid, ''), False)
                    else:
                        # Would want to pass in None, but the upload process expects real files, so use this placeholder.
                        source_path = temp_path = None

                    # Upload to dest
                    print dest_client.upload_bundle(source_path, info, dest_worksheet_uuid, False, [], add_to_worksheet)

                    # Clean up
                    if temp_path: path_util.remove(temp_path)
        else:
            if add_to_worksheet:
                dest_client.add_worksheet_item(dest_worksheet_uuid, worksheet_util.bundle_item(source_bundle_uuid))

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
        args.target_spec, args.command = cli_util.desugar_command(args.target_spec, args.command)
        targets = self.parse_key_targets(client, worksheet_uuid, args.target_spec)
        metadata = metadata_util.request_missing_metadata(RunBundle, args)
        uuid = client.derive_bundle('run', targets, args.command, metadata, worksheet_uuid)
        print uuid
        self.wait(client, args, uuid)

    def do_edit_command(self, argv, parser):
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT)
        parser.add_argument('-n', '--name', help='new name: ' + spec_util.NAME_REGEX.pattern, nargs='?')
        parser.add_argument('-d', '--description', help='new description', nargs='?')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        bundle_uuid = worksheet_util.get_bundle_uuid(client, worksheet_uuid, args.bundle_spec)
        info = client.get_bundle_info(bundle_uuid)
        bundle_subclass = get_bundle_subclass(info['bundle_type'])

        metadata = info['metadata']
        new_metadata = copy.deepcopy(metadata)
        if args.name:
            new_metadata['name'] = args.name
        if args.description:
            new_metadata['description'] = args.description

        # Prompt user for all information
        if not self.headless and metadata == new_metadata:
            new_metadata = metadata_util.request_missing_metadata(bundle_subclass, args, new_metadata)

        if metadata != new_metadata:
            client.update_bundle_metadata(bundle_uuid, new_metadata)
            print "Saved metadata for bundle %s." % (bundle_uuid)

    def do_detach_command(self, argv, parser):
        '''
        Removes the given bundles from the given worksheet (but importantly
        doesn't delete the actual bundles, unlike rm).
        '''
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT, nargs='+')
        parser.add_argument('-n', '--index', help='index (1, 2, ...) of the bundle to delete', type=int)
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        # Resolve all the bundles first, then detach.
        # This is important since some of the bundle specs (^1 ^2) are relative.
        bundle_uuids = worksheet_util.get_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        worksheet_info = client.get_worksheet_info(worksheet_uuid, True)

        # Number the bundles: c c a b c => 3 2 1 1 1
        items = worksheet_info['items']
        indices = [None] * len(items)  # Parallel array to items that stores the index associated with that bundle uuid
        uuid2index = {}  # bundle uuid => index of the bundle (at the end, number of times it occurs on the worksheet)
        for i, item in reversed(list(enumerate(items))):
            (bundle_info, subworksheet_info, value_obj, item_type) = item
            if item_type == worksheet_util.TYPE_BUNDLE:
                uuid = bundle_info['uuid']
                indices[i] = uuid2index[uuid] = uuid2index.get(uuid, 0) + 1

        # Detach the items.
        new_items = []
        for i, item in enumerate(items):
            (bundle_info, subworksheet_info, value_obj, item_type) = item
            detach = False
            if item_type == worksheet_util.TYPE_BUNDLE:
                uuid = bundle_info['uuid']
                # If want to detach uuid, then make sure we're detaching the
                # right index or if the index is not specified, that it's
                # unique.
                if uuid in bundle_uuids:
                    if args.index == None:
                        if uuid2index[uuid] != 1:
                            raise UsageError('bundle %s shows up more than once, need to specify index' % uuid)
                        detach = True
                    else:
                        if args.index > uuid2index[uuid]:
                            raise UsageError('bundle %s shows up %d times, can\'t get index %d' % (uuid, uuid2index[uuid], args.index))
                        if args.index == indices[i]:
                            detach = True
            if not detach:
                new_items.append(item)

        client.update_worksheet_items(worksheet_info, new_items)

    def do_rm_command(self, argv, parser):
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT, nargs='+')
        parser.add_argument('--force', action='store_true', help='delete bundle (DANGEROUS - breaking dependencies!)')
        parser.add_argument('-r', '--recursive', action='store_true', help='delete all bundles downstream that depend on this bundle')
        parser.add_argument('-d', '--data-only', action='store_true', help='keep the bundle metadata, but remove the bundle contents')
        parser.add_argument('-i', '--dry-run', action='store_true', help='delete all bundles downstream that depend on this bundle')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        # Resolve all the bundles first, then delete.
        # This is important since some of the bundle specs (^1 ^2) are relative.
        bundle_uuids = worksheet_util.get_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        deleted_uuids = client.delete_bundles(bundle_uuids, args.force, args.recursive, args.data_only, args.dry_run)
        if args.dry_run:
            print 'This command would permanently remove the following bundles (not doing so yet):'
            bundle_infos = client.get_bundle_infos(deleted_uuids)
            bundle_info_list = [bundle_infos[uuid] for uuid in deleted_uuids]
            self.print_bundle_info_list(bundle_info_list, uuid_only=False, print_ref=False)
        else:
            for uuid in deleted_uuids: print uuid

    def do_search_command(self, argv, parser):
        parser.add_argument('keywords', help='keywords to search for', nargs='+')
        parser.add_argument('-a', '--append', help='append these bundles to the given worksheet', action='store_true')
        parser.add_argument('-u', '--uuid-only', help='print only uuids', action='store_true')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        bundle_uuids = client.search_bundle_uuids(worksheet_uuid, args.keywords)
        if not isinstance(bundle_uuids, list):  # Direct result
            print bundle_uuids
            return self.create_structured_info_map([('refs', None)])

        # Print out bundles
        bundle_infos = client.get_bundle_infos(bundle_uuids)
        bundle_info_list = [bundle_infos[uuid] for uuid in bundle_uuids if uuid in bundle_infos]
        reference_map = self.create_reference_map('bundle', bundle_info_list)
        if args.uuid_only:
            bundle_info_list = [{'uuid': uuid} for uuid in bundle_uuids]

        if len(bundle_info_list) > 0:
            self.print_bundle_info_list(bundle_info_list, uuid_only=args.uuid_only, print_ref=False)

        if args.append:
            # Add the bundles to the current worksheet
            # Consider batching this
            for bundle_uuid in bundle_uuids:
                client.add_worksheet_item(worksheet_uuid, worksheet_util.bundle_item(bundle_uuid))
            worksheet_info = client.get_worksheet_info(worksheet_uuid, False)
            print 'Added %d bundles to %s' % (len(bundle_uuids), self.worksheet_str(worksheet_info))
        return self.create_structured_info_map([('refs', reference_map)])

    def create_structured_info_map(self, structured_info_list):
        '''
        Return dict of info dicts (eg. bundle/worksheet reference_map) containing
        information associated to bundles/worksheets. cl wls, ls, etc. show uuids
        which are too short. This dict contains additional information that is
        needed to recover URL on the client side.
        '''
        return dict(structured_info_list)

    def create_reference_map(self, info_type, info_list):
        '''
        Return dict of dicts containing name, uuid and type for each bundle/worksheet
        in the info_list. This information is needed to recover URL on the cient side.
        '''
        if len(info_list) == 0:
            return {}
        return {
            worksheet_util.apply_func(self.UUID_POST_FUNC, info['uuid']) : {
                'type': info_type,
                'uuid': info['uuid'],
                'name': info['metadata']['name'] if 'metadata' in info else info['name']
            } for info in info_list
        }

    def do_ls_command(self, argv, parser):
        parser.add_argument('worksheet_spec', help='identifier: %s (default: current worksheet)' % self.GLOBAL_SPEC_FORMAT, nargs='?')
        parser.add_argument('-u', '--uuid-only', help='only print uuids', action='store_true')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        worksheet_info = client.get_worksheet_info(worksheet_uuid, True, True)
        bundle_info_list = self.get_worksheet_bundles(worksheet_info)
        if not args.uuid_only:
            print self._worksheet_description(worksheet_info)
        if len(bundle_info_list) > 0:
            self.print_bundle_info_list(bundle_info_list, args.uuid_only, print_ref=True)
        reference_map = self.create_reference_map('bundle', bundle_info_list)
        return self.create_structured_info_map([('refs', reference_map)])

    def _worksheet_description(self, worksheet_info):
        return '### Worksheet: %s\n### Title: %s\n### Owner: %s(%s)\n### Permissions: %s%s' % \
            (self.worksheet_str(worksheet_info),
            worksheet_info['title'],
            worksheet_info['owner_name'], worksheet_info['owner_id'], \
            group_permissions_str(worksheet_info['group_permissions']),
            ' [frozen]' if worksheet_info['frozen'] else '')

    def print_bundle_info_list(self, bundle_info_list, uuid_only, print_ref):
        '''
        Helper function: print a nice table showing all provided bundles.
        '''
        if uuid_only:
            for bundle_info in bundle_info_list:
                print bundle_info['uuid']
        else:
            def get(i, info, col):
                if col == 'ref':
                    return '^' + str(len(bundle_info_list) - i)
                else:
                    return info.get(col, info.get('metadata', {}).get(col))

            columns = (('ref',) if print_ref else ()) + ('uuid', 'name', 'bundle_type', 'owner', 'created', 'data_size', 'state')
            post_funcs = {'uuid': self.UUID_POST_FUNC, 'created': 'date', 'data_size': 'size'}
            justify = {'data_size': 1, 'ref': 1}
            bundle_dicts = [
              {col: get(i, info, col) for col in columns}
              for i, info in enumerate(bundle_info_list)
            ]
            self.print_table(columns, bundle_dicts, post_funcs=post_funcs, justify=justify)

    def do_info_command(self, argv, parser):
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT, nargs='+')
        parser.add_argument('-f', '--field', help='print out these fields', nargs='?')
        parser.add_argument('-r', '--raw', action='store_true', help='print out raw information (no rendering)')
        parser.add_argument('-v', '--verbose', action='store_true', help="print top-level contents of bundle, children bundles, and host worksheets")
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        bundle_uuids = worksheet_util.get_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        for i, bundle_uuid in enumerate(bundle_uuids):
            info = client.get_bundle_info(bundle_uuid, args.verbose, args.verbose, args.verbose)
            if info is None:
                raise UsageError('Unable to retrieve information about bundle with uuid %s' % bundle_uuid)

            if args.field:
                # Display individual fields (arbitrary genpath)
                values = []
                for genpath in args.field.split(','):
                    if worksheet_util.is_file_genpath(genpath):
                        value = contents_str(worksheet_util.interpret_file_genpath(client, {}, bundle_uuid, genpath, None))
                    else:
                        value = worksheet_util.interpret_genpath(info, genpath)
                    values.append(value)
                print '\t'.join(map(str, values))
            else:
                # Display all the fields
                if i > 0:
                    print
                self.print_basic_info(client, info, args.raw)
                if args.verbose:
                    self.print_children(info)
                    self.print_host_worksheets(info)
                    self.print_permissions(info)
                    self.print_contents(client, info)

    def key_value_str(self, key, value):
        return '%-21s: %s' % (key, value if value != None else '<none>')

    def print_basic_info(self, client, info, raw):
        '''
        Print the basic information for a bundle (key/value pairs).
        '''

        metadata = info['metadata']
        lines = []  # The output that we're accumulating

        # Bundle fields
        for key in ('bundle_type', 'uuid', 'data_hash', 'state', 'command', 'owner'):
            if not raw:
                if key not in info: continue
            lines.append(self.key_value_str(key, info.get(key)))

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
            lines.append(self.key_value_str(key, value))

        # Metadata fields (non-standard)
        standard_keys = set(spec.key for spec in cls.METADATA_SPECS)
        for key, value in metadata.items():
            if key in standard_keys: continue
            lines.append(self.key_value_str(key, value))

        # Dependencies (both hard dependencies and soft)
        def display_dependencies(label, deps):
            lines.append(label + ':')
            for dep in deps:
                child = dep['child_path']
                parent = path_util.safe_join((dep['parent_name'] or 'MISSING') + '(' + dep['parent_uuid'] + ')', dep['parent_path'])
                lines.append('  %s: %s' % (child, parent))
        if info['dependencies']:
            deps = info['dependencies']
            display_dependencies('dependencies', deps)

        print '\n'.join(lines)

    def print_children(self, info):
        print 'children:'
        for child in info['children']:
            print "  %s" % self.simple_bundle_str(child)

    def print_host_worksheets(self, info):
        print 'host_worksheets:'
        for host_worksheet_info in info['host_worksheets']:
            print "  %s" % self.simple_worksheet_str(host_worksheet_info)

    def print_permissions(self, info):
        print 'permission: %s' % permission_str(info['permission'])
        print 'group_permissions:'
        print '  %s' % group_permissions_str(info['group_permissions'])

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
        self._fail_if_headless('cat')  # Files might be too big
        parser.add_argument('target_spec', help=self.TARGET_SPEC_FORMAT)
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        target = self.parse_target(client, worksheet_uuid, args.target_spec)
        self.print_target_info(client, target, decorate=False, fail_if_not_exist=True)

    # Helper: shared between info and cat
    def print_target_info(self, client, target, decorate, maxlines=10, fail_if_not_exist=False):
        info = client.get_target_info(target, 1)
        info_type = info.get('type')
        if info_type is None:
            if fail_if_not_exist:
                raise UsageError('Target doesn\'t exist: %s/%s' % target)
            else:
                print "MISSING"
        if info_type == 'file':
            if decorate:
                import base64
                for line in client.head_target(target, maxlines):
                    print base64.b64decode(line),
            else:
                client.cat_target(target, sys.stdout)
        def size(x):
            t = x.get('type', '???')
            if t == 'file': return formatting.size_str(x['size'])
            if t == 'directory': return 'dir'
            return t
        if info_type == 'directory':
            contents = [
                {'name': x['name'] + (' -> ' + x['link'] if 'link' in x else ''),
                'size': size(x),
                'perm': oct(x['perm']) if 'perm' in x else ''}
                for x in info['contents']
            ]
            contents = sorted(contents, key=lambda r : r['name'])
            self.print_table(('name', 'perm', 'size'), contents, justify={'size':1}, indent='')
        return info

    def do_wait_command(self, argv, parser):
        self._fail_if_headless('wait')
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
        print bundle_uuid

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
        parser.add_argument('-i', '--dry-run', help='dry run (just show what will be done without doing it)', action='store_true')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        self.add_wait_args(parser)

    def mimic(self, args):
        '''
        Use args.bundles to generate a mimic call to the BundleClient.
        '''
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        bundle_uuids = worksheet_util.get_bundle_uuids(client, worksheet_uuid, args.bundles)

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

        plan = client.mimic(
            old_inputs, old_output, new_inputs, args.name,
            worksheet_uuid, args.depth, args.shadow, args.dry_run)
        for (old, new) in plan:
            print >>sys.stderr, '%s => %s' % (self.simple_bundle_str(old), self.simple_bundle_str(new))
        if len(plan) > 0:
            new_uuid = plan[-1][1]['uuid']  # Last new uuid to be created
            self.wait(client, args, new_uuid)
            print new_uuid
        else:
            print 'Nothing to be done.'

    def do_kill_command(self, argv, parser):
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT, nargs='+')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        bundle_uuids = worksheet_util.get_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        for bundle_uuid in bundle_uuids:
            print bundle_uuid
        client.kill_bundles(bundle_uuids)

    #############################################################################
    # CLI methods for worksheet-related commands follow!
    #############################################################################

    def worksheet_str(self, worksheet_info):
        return '%s::%s(%s)' % (self.manager.session()['address'], worksheet_info['name'], worksheet_info['uuid'])

    def do_new_command(self, argv, parser):
        parser.add_argument('name', help='name: ' + spec_util.NAME_REGEX.pattern)
        parser.add_argument('-u', '--uuid', help='specify the uuid of worksheet (if really want this)')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        uuid = client.new_worksheet(args.name, args.uuid)
        client.add_worksheet_item(worksheet_uuid, worksheet_util.subworksheet_item(uuid))  # Add new to current
        worksheet_info = client.get_worksheet_info(uuid, False)
        print worksheet_info['uuid']

    def do_add_command(self, argv, parser):
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT, nargs='*')
        parser.add_argument('-m', '--message', help='add a text element', nargs='?')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        bundle_uuids = worksheet_util.get_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        for bundle_uuid in bundle_uuids:
            client.add_worksheet_item(worksheet_uuid, worksheet_util.bundle_item(bundle_uuid))
        if args.message != None:
            if args.message.startswith('%'):
                client.add_worksheet_item(worksheet_uuid, worksheet_util.directive_item(args.message[1:].strip()))
            else:
                client.add_worksheet_item(worksheet_uuid, worksheet_util.markup_item(args.message))

    def do_work_command(self, argv, parser):
        parser.add_argument('-u', '--uuid-only', help='print worksheet uuid', action='store_true')
        parser.add_argument('worksheet_spec', help=self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        worksheet_info = client.get_worksheet_info(worksheet_uuid, False)
        if args.worksheet_spec:
            self.manager.set_current_worksheet_uuid(client, worksheet_uuid)
            if args.uuid_only:
                print worksheet_info['uuid']
            else:
                print 'Switched to worksheet %s.' % (self.worksheet_str(worksheet_info))
        else:
            if worksheet_info:
                if args.uuid_only:
                    print worksheet_info['uuid']
                else:
                    print 'Currently on worksheet %s.' % (self.worksheet_str(worksheet_info))
            else:
                print 'Not on any worksheet. Use `cl new` or `cl work` to switch to one.'

    def do_wedit_command(self, argv, parser):
        parser.add_argument('worksheet_spec', help=self.WORKSHEET_SPEC_FORMAT, nargs='?')
        parser.add_argument('-n', '--name', help='new name: ' + spec_util.NAME_REGEX.pattern, nargs='?')
        parser.add_argument('-t', '--title', help='change title')
        parser.add_argument('-o', '--owner_spec', help='change owner', nargs='?')
        parser.add_argument('--freeze', help='freeze worksheet', action='store_true')
        parser.add_argument('-f', '--file', help='overwrite the given worksheet with this file', nargs='?')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        worksheet_info = client.get_worksheet_info(worksheet_uuid, True)
        if args.name or args.title or args.owner_spec or args.freeze:
            # Update the worksheet metadata.
            info = {}
            if args.name:
                info['name'] = args.name
            if args.title:
                info['title'] = args.title
            if args.owner_spec:
                info['owner_spec'] = args.owner_spec
            if args.freeze:
                info['freeze'] = True
            client.update_worksheet_metadata(worksheet_uuid, info)
            print 'Saved worksheet metadata for %s(%s).' % (worksheet_info['name'], worksheet_info['uuid'])
        else:
            # Update the worksheet items.
            # Either get a list of lines from the given file or request it from the user in an editor.
            if args.file:
                lines = [line.rstrip() for line in open(args.file).readlines()]
            else:
                lines = worksheet_util.request_lines(worksheet_info, client)

            # Parse the lines.
            new_items, commands = worksheet_util.parse_worksheet_form(lines, client, worksheet_info['uuid'])

            # Save the worksheet.
            client.update_worksheet_items(worksheet_info, new_items)
            print 'Saved worksheet items for %s(%s).' % (worksheet_info['name'], worksheet_info['uuid'])

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
        self._fail_if_headless('print')
        parser.add_argument('worksheet_spec', help=self.WORKSHEET_SPEC_FORMAT, nargs='?')
        parser.add_argument('-r', '--raw', action='store_true', help='print out the raw contents')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        worksheet_info = client.get_worksheet_info(worksheet_uuid, True)
        if args.raw:
            lines = worksheet_util.get_worksheet_lines(worksheet_info)
            for line in lines:
                print line
        else:
            print self._worksheet_description(worksheet_info)
            interpreted = worksheet_util.interpret_items(worksheet_util.get_default_schemas(), worksheet_info['items'])
            self.display_interpreted(client, worksheet_info, interpreted)

    def display_interpreted(self, client, worksheet_info, interpreted):
        is_last_newline = False
        for item in interpreted['items']:
            mode = item['mode']
            data = item['interpreted']
            properties = item['properties']
            is_newline = (data == '')
            if mode == 'markup' or mode == 'contents':
                if not (is_newline and is_last_newline):
                    if mode == 'contents':
                        maxlines = properties.get('maxlines')
                        if maxlines:
                            maxlines = int(maxlines)
                        try:
                            self.print_target_info(client, data, decorate=True, maxlines=maxlines)
                        except UsageError, e:
                            print 'ERROR:', e
                    else:
                        print data
            elif mode == 'record' or mode == 'table':
                # header_name_posts is a list of (name, post-processing) pairs.
                header, contents = data
                contents = worksheet_util.interpret_genpath_table_contents(client, contents)
                # Print the table
                self.print_table(header, contents, show_header=(mode == 'table'), indent='  ')
            elif mode == 'html' or mode == 'image':
                # Placeholder
                print '[' + mode + ' ' + str(data) + ']'
            elif mode == 'search':
                search_interpreted = worksheet_util.interpret_search(client, worksheet_info['uuid'], data)
                self.display_interpreted(client, worksheet_info, search_interpreted)
            elif mode == 'worksheet':
                print '[Worksheet ' + self.simple_worksheet_str(data) + ']'
            else:
                raise UsageError('Invalid display mode: %s' % mode)
            is_last_newline = is_newline

    def do_wls_command(self, argv, parser):
        parser.add_argument('keywords', help='keywords to search for', nargs='*')
        parser.add_argument('-a', '--address', help=self.ADDRESS_SEPC_FORMAT, nargs='?')
        parser.add_argument('-u', '--uuid-only', help='only print uuids', action='store_true')
        args = parser.parse_args(argv)

        if args.address:
            address = self.manager.apply_alias(args.address)
            client = self.manager.client(address)
        else:
            client = self.manager.current_client()

        worksheet_dicts = client.search_worksheets(args.keywords)
        if args.uuid_only:
            for row in worksheet_dicts:
                print row['uuid']
        else:
            if worksheet_dicts:
                for row in worksheet_dicts:
                    row['owner'] = '%s(%s)' % (row['owner_name'], row['owner_id'])
                    row['permissions'] = group_permissions_str(row['group_permissions'])
                post_funcs = {'uuid': self.UUID_POST_FUNC}
                self.print_table(('uuid', 'name', 'owner', 'permissions'), worksheet_dicts, post_funcs)
        reference_map = self.create_reference_map('worksheet', worksheet_dicts)
        return self.create_structured_info_map([('refs', reference_map)])

    def do_wadd_command(self, argv, parser):
        parser.add_argument('subworksheet_spec', help='worksheets to add (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='+')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        for spec in args.subworksheet_spec:
            subworksheet_uuid = worksheet_util.get_worksheet_uuid(client, worksheet_uuid, spec)
            client.add_worksheet_item(worksheet_uuid, worksheet_util.subworksheet_item(subworksheet_uuid))

    def do_wrm_command(self, argv, parser):
        parser.add_argument('worksheet_spec', help='identifier: [<uuid>|<name>]', nargs='+')
        parser.add_argument('--force', action='store_true', help='delete even if non-empty and frozen')
        args = parser.parse_args(argv)

        for worksheet_spec in args.worksheet_spec:
            client, worksheet_uuid = self.parse_client_worksheet_uuid(worksheet_spec)
            client.delete_worksheet(worksheet_uuid, args.force)

    def do_wcp_command(self, argv, parser):
        parser.add_argument('source_worksheet_spec', help=self.WORKSHEET_SPEC_FORMAT)
        parser.add_argument('dest_worksheet_spec', help=self.WORKSHEET_SPEC_FORMAT)
        parser.add_argument('-c', '--clobber', help='clobber everything on destination worksheet with source (instead of appending)', action='store_true')
        args = parser.parse_args(argv)

        # Source worksheet
        (source_client, source_worksheet_uuid) = self.parse_client_worksheet_uuid(args.source_worksheet_spec)
        source_items = source_client.get_worksheet_info(source_worksheet_uuid, True)['items']

        # Destination worksheet
        (dest_client, dest_worksheet_uuid) = self.parse_client_worksheet_uuid(args.dest_worksheet_spec)
        dest_worksheet_info = dest_client.get_worksheet_info(dest_worksheet_uuid, True)
        dest_items = [] if args.clobber else dest_worksheet_info['items']

        # Save all items.
        dest_client.update_worksheet_items(dest_worksheet_info, dest_items + source_items)

        # Copy over the bundles
        for item in source_items:
            (source_bundle_info, source_worksheet_info, value_obj, item_type) = item
            if item_type == worksheet_util.TYPE_BUNDLE:
                self.copy_bundle(source_client, source_bundle_info['uuid'], dest_client, dest_worksheet_uuid, copy_dependencies=False, add_to_worksheet=False)

        print 'Copied %s worksheet items to %s.' % (len(source_items), dest_worksheet_uuid)


    #############################################################################
    # CLI methods for commands related to groups and permissions follow!
    #############################################################################

    def do_gls_command(self, argv, parser):
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        group_dicts = client.list_groups()
        if group_dicts:
            for row in group_dicts:
                row['owner'] = '%s(%s)' % (row['owner_name'], row['owner_id'])
            self.print_table(('name', 'uuid', 'owner', 'role'), group_dicts)
        else:
            print 'No groups found.'

    def do_gnew_command(self, argv, parser):
        parser.add_argument('name', help='name: ' + spec_util.NAME_REGEX.pattern)
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        group_dict = client.new_group(args.name)
        print 'Created new group %s(%s).' % (group_dict['name'], group_dict['uuid'])

    def do_grm_command(self, argv, parser):
        parser.add_argument('group_spec', help='group identifier: [<uuid>|<name>]')
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        group_dict = client.rm_group(args.group_spec)
        print 'Deleted group %s(%s).' % (group_dict['name'], group_dict['uuid'])

    def do_ginfo_command(self, argv, parser):
        parser.add_argument('group_spec', help='group identifier: [<uuid>|<name>]')
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        group_dict = client.group_info(args.group_spec)
        members = group_dict['members']
        for row in members:
            row['user'] = '%s(%s)' % (row['user_name'], row['user_id'])
        print 'Members of group %s(%s):' % (group_dict['name'], group_dict['uuid'])
        self.print_table(('user', 'role'), group_dict['members'])

    def do_uadd_command(self, argv, parser):
        parser.add_argument('user_spec', help='username')
        parser.add_argument('group_spec', help='group identifier: [<uuid>|<name>]')
        parser.add_argument('-a', '--admin', action='store_true',
                            help='Give admin privileges to the user for the group')
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

    def do_urm_command(self, argv, parser):
        parser.add_argument('user_spec', help='username')
        parser.add_argument('group_spec', help='group %s' + self.GROUP_SPEC_FORMAT)
        args = parser.parse_args(argv)

        client = self.manager.current_client()
        user_info = client.rm_user(args.user_spec, args.group_spec)
        if user_info is None:
            print '%s is not a member of group %s.' % (user_info['name'], user_info['group_uuid'])
        else:
            print 'Removed %s from group %s.' % (user_info['name'], user_info['group_uuid'])

    def do_perm_command(self, argv, parser):
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT, nargs='*')
        parser.add_argument('group_spec', help=self.GROUP_SPEC_FORMAT)
        parser.add_argument('permission_spec', help=self.PERMISSION_SPEC_FORMAT)
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        bundle_uuids = worksheet_util.get_bundle_uuids(client, worksheet_uuid, args.bundle_spec)

        result = client.set_bundles_perm(bundle_uuids, args.group_spec, args.permission_spec)
        print "Group %s(%s) has %s permission on %d bundles." % \
            (result['group_info']['name'], result['group_info']['uuid'],
             permission_str(result['permission']), len(bundle_uuids))

    def do_wperm_command(self, argv, parser):
        parser.add_argument('worksheet_spec', help=self.WORKSHEET_SPEC_FORMAT)
        parser.add_argument('group_spec', help=self.GROUP_SPEC_FORMAT)
        parser.add_argument('permission_spec', help=self.PERMISSION_SPEC_FORMAT)
        args = parser.parse_args(argv)
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        result = client.set_worksheet_perm(worksheet_uuid, args.group_spec, args.permission_spec)
        print "Group %s(%s) has %s permission on worksheet %s(%s)." % \
            (result['group_info']['name'], result['group_info']['uuid'],
             permission_str(result['permission']), result['worksheet']['name'], result['worksheet']['uuid'])

    def do_chown_command(self, argv, parser):
        '''
        Change the owner of bundles.
        '''
        parser.add_argument('user_spec', help='username')
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT, nargs='+')
        parser.add_argument('-w', '--worksheet_spec', help='operate on this worksheet (%s)' % self.WORKSHEET_SPEC_FORMAT, nargs='?')
        args = parser.parse_args(argv)
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        bundle_uuids = worksheet_util.get_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        client.chown_bundles(bundle_uuids, args.user_spec)
        for uuid in bundle_uuids: print uuid

    #############################################################################
    # LocalBundleClient-only commands follow!
    #############################################################################

    def do_work_manager_command(self, argv, parser):
        self._fail_if_headless('work-manager')
        # This command only works if client is a LocalBundleClient.
        parser.add_argument('-t', '--worker-type', type=str, help="worker type (defined in config.json)", default='local')
        parser.add_argument('--num-iterations', help="number of bundles to process before exiting", type=int, default=None)
        parser.add_argument('--sleep-time', type=int, help='Number of seconds to wait between successive polls', default=1)
        args = parser.parse_args(argv)

        worker_config = self.manager.config['workers']
        if args.worker_type == 'local':
            machine = LocalMachine()
        elif args.worker_type in worker_config:
            machine = RemoteMachine(worker_config[args.worker_type])
        else:
            print '\'' + args.worker_type + '\'' + \
                  ' is not specified in your config file: ' + self.manager.config_path()
            print 'Options are ' + str(map(str, worker_config.keys()))
            return

        client = self.manager.local_client()  # Always use the local bundle client
        worker = Worker(client.bundle_store, client.model, machine, client.auth_handler)
        worker.run_loop(args.num_iterations, args.sleep_time)

    def do_events_command(self, argv, parser):
        self._fail_if_headless('events')
        self._fail_if_not_local('events')
        # This command only works if client is a LocalBundleClient.
        parser.add_argument('-u', '--user', help='Filter by user id or username')
        parser.add_argument('-c', '--command', help='Filter by command')
        parser.add_argument('-a', '--args', help='Filter by arguments')
        parser.add_argument('--uuid', help='Filter by bundle or worksheet uuid')
        parser.add_argument('-o', '--offset', help='Offset in the result list', type=int, default=0)
        parser.add_argument('-l', '--limit', help='Limit in the result list', type=int, default=20)
        parser.add_argument('-n', '--count', help='Just count', action='store_true')
        parser.add_argument('-g', '--group_by', help='Group by this field (e.g., date)')
        args = parser.parse_args(argv)
        client = self.manager.current_client()

        # Build query
        query_info = {
            'user': args.user, 'command': args.command, 'args': args.args, 'uuid': args.uuid,
            'count': args.count, 'group_by': args.group_by
        }
        info = client.get_events_log_info(query_info, args.offset, args.limit)
        if 'counts' in info:
            for row in info['counts']:
                print '\t'.join(map(str, list(row)))
        if 'events' in info:
            for event in info['events']:
                row = [
                    event.end_time.strftime('%Y-%m-%d %X') if event.end_time != None else '',
                    '%.3f' % event.duration if event.duration != None else '',
                    '%s(%s)' % (event.user_name, event.user_id),
                    event.command, event.args]
                print '\t'.join(row)

    def do_cleanup_command(self, argv, parser):
        self._fail_if_headless('cleanup')
        self._fail_if_not_local('cleanup')
        '''
        Delete unused data and temp files (be careful!).
        '''
        parser.add_argument('-i', '--dry-run', action='store_true', help='don\'t actually do it, but see what the command would do')
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        client.bundle_store.full_cleanup(client.model, args.dry_run)

    def do_reset_command(self, argv, parser):
        '''
        Delete everything - be careful!
        '''
        self._fail_if_headless('reset')
        self._fail_if_not_local('reset')
        parser.add_argument('--commit', action='store_true', help='reset is a no-op unless committed')
        args = parser.parse_args(argv)
        if not args.commit:
            raise UsageError('If you really want to delete EVERYTHING, use --commit')
        client = self.manager.current_client()
        print 'Deleting entire bundle store...'
        client.bundle_store._reset()
        print 'Deleting entire database...'
        client.model._reset()

    def _fail_if_headless(self, message):
        if self.headless:
            raise UsageError('Cannot execute CLI command: %s' % message)

    def _fail_if_not_local(self, message):
        from codalab.client.local_bundle_client import LocalBundleClient
        if not isinstance(self.manager.current_client(), LocalBundleClient):
            raise UsageError('Cannot execute CLI command in non-local mode: %s' % message)
