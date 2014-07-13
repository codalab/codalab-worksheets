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
  canonicalize
)
from codalab.objects.worker import Worker
from codalab.objects.worksheet import Worksheet

class BundleCLI(object):
    DESCRIPTIONS = {
      # Commands for bundles.
      'upload': 'Create a bundle by uploading an existing file/directory.',
      'make': 'Create a bundle out of existing bundles.',
      'run': 'Create a bundle by running a program bundle on an input bundle.',
      'edit': "Edit an existing bundle's metadata.",
      'rm': 'Delete a bundle (and all bundles that depend on it).',
      'ls': 'List bundles in a worksheet.',
      'info': 'Show detailed information for a bundle.',
      'cat': 'Print the contents of a file/directory in a bundle.',
      'wait': 'Wait until a bundle finishes.',
      'download': 'Download bundle from an instance.',
      'cp': 'Copy bundles across instances.',
      'mimic': 'Creates a set of bundles based on analogy with another set.',
      'macro': 'Use mimicry to simulate macros.',
      # Commands for worksheets.
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
      'worker',
      'server',
    )

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
        #uuids_seen = set()
        result = []
        for (bundle_info, _, _) in worksheet_info['items']:
            if bundle_info and 'bundle_type' in bundle_info:
                #if bundle_info['uuid'] not in uuids_seen:
                #uuids_seen.add(bundle_info['uuid'])
                result.append(bundle_info)
        return result

    def parse_target(self, target_spec):
        '''
        Helper: A target_spec is a bundle_spec[/subpath].
        '''
        if os.sep in target_spec:
            bundle_spec, subpath = tuple(target_spec.split(os.sep, 1))
        else:
            bundle_spec, subpath = target_spec, ''
        # Resolve the bundle_spec to a particular bundle_uuid.
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        bundle_uuid = client.get_bundle_uuid(worksheet_uuid, bundle_spec)
        return (bundle_uuid, subpath)

    def parse_key_targets(self, items):
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
            targets[key] = self.parse_target(target)
        return targets

    def print_table(self, columns, row_dicts):
        '''
        Pretty-print a list of columns from each row in the given list of dicts.
        '''
        rows = list(itertools.chain([columns], (
          [row_dict.get(col, '') for col in columns] for row_dict in row_dicts
        )))
        lengths = [max(len(str(value)) for value in col) for col in zip(*rows)]
        for (i, row) in enumerate(rows):
            row_strs = []
            for (value, length) in zip(row, lengths):
                row_strs.append(str(value) + (length - len(str(value)))*' ')
            print '  '.join(row_strs)
            if i == 0:
                print (sum(lengths) + 2*(len(columns) - 1))*'-'

    def size_str(self, size):
        for unit in ('', 'K', 'M', 'G'):
            if size < 1024:
                return '%d%s' % (size, unit)
            size /= 1024

    def time_str(self, ts):
        return datetime.datetime.utcfromtimestamp(ts).isoformat().replace('T', ' ')

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
        return (self.manager.client(address), spec)

    def parse_client_worksheet_info(self, spec):
        client, spec = self.parse_spec(spec)
        return (client, client.get_worksheet_info(spec if spec else Worksheet.DEFAULT_WORKSHEET_NAME))

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
        command_fn = getattr(self, 'do_%s_command' % (command.replace('-', '_'),), None)
        if not command_fn:
            self.exit("'%s' is not a CodaLab command. Try 'cl help'." % (command,))
        parser = self.create_parser(command)
        if self.verbose >= 2:
            command_fn(remaining_args, parser)
        else:
            try:
                return command_fn(remaining_args, parser)
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
        print "session: %s" % self.manager.session_name()
        address = self.manager.session()['address']
        print "address: %s" % address
        state = self.manager.state['auth'].get(address, {})
        if 'username' in state:
            print "username: %s" % state['username']
        worksheet_info = self.get_current_worksheet_info()
        if worksheet_info:
            print "worksheet: %s [%s]" % (worksheet_info['name'], worksheet_info['uuid'])

    def do_upload_command(self, argv, parser):
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
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

        print client.upload_bundle(args.path, {'bundle_type': args.bundle_type, 'metadata': metadata}, worksheet_uuid)

    def do_download_command(self, argv, parser):
        parser.add_argument(
          'target_spec',
          help=self.TARGET_SPEC_FORMAT
        )
        parser.add_argument(
          '-o', '--output-dir',
          help='Directory to download file.  By default, the bundle or subpath name is used.',
        )
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        args = parser.parse_args(argv)
        target = self.parse_target(args.target_spec)
        bundle_uuid, subpath = target

        # Download first to a local location path.
        local_path, temp_path = client.download_target(target)

        # Copy into desired directory.
        info = client.get_bundle_info(bundle_uuid)
        if args.output_dir:
            local_dir = args.output_dir
        else:
            local_dir = info['metadata']['name'] if subpath == '' else os.path.basename(subpath)
        final_path = os.path.join(os.getcwd(), local_dir)
        if os.path.exists(final_path):
            print 'Local directory', local_dir, 'already exists. Bundle is available at:'
            print local_path
        else:
            path_util.copy(local_path, final_path)
            if temp_path: path_util.remove(temp_path)

    def do_cp_command(self, argv, parser):
        parser.add_argument(
          'bundle_spec',
          help=self.BUNDLE_SPEC_FORMAT
        )
        parser.add_argument(
          'worksheet_spec',
          help='%s (copy to this worksheet)' % self.WORKSHEET_SPEC_FORMAT,
        )
        args = parser.parse_args(argv)
        # Source bundle
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        bundle_uuid = client.get_bundle_uuid(worksheet_uuid, args.bundle_spec)
        # Destination worksheet
        (dest_client, spec) = self.parse_spec(args.worksheet_spec)
        new_worksheet_uuid = dest_client.get_worksheet_info(spec)['uuid']

        source_path, temp_path = client.download_target((bundle_uuid, ''))
        info = client.get_bundle_info(bundle_uuid)

        # TODO: copy all the hard dependencies
        print dest_client.upload_bundle(source_path, info, new_worksheet_uuid)
        if temp_path: path_util.remove(temp_path)

    def do_make_command(self, argv, parser):
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        parser.add_argument('target_spec', help=self.TARGET_SPEC_FORMAT, nargs='+')
        metadata_util.add_arguments(MakeBundle, set(), parser)
        metadata_util.add_auto_argument(parser)
        args = parser.parse_args(argv)
        targets = self.parse_key_targets(args.target_spec)
        metadata = metadata_util.request_missing_data(MakeBundle, args)
        print client.derive_bundle('make', targets, None, metadata, worksheet_uuid)

    def do_run_command(self, argv, parser):
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        parser.add_argument('target_spec', help=self.TARGET_SPEC_FORMAT, nargs='*')
        parser.add_argument('command', help='Command-line')
        parser.add_argument('-w', '--wait', action='store_true', help='Wait until run finishes')
        parser.add_argument('-t', '--tail', action='store_true', help='Wait until run finishes, writing output')
        metadata_util.add_arguments(RunBundle, set(), parser)
        metadata_util.add_auto_argument(parser)
        args = parser.parse_args(argv)
        targets = self.parse_key_targets(args.target_spec)
        command = args.command
        metadata = metadata_util.request_missing_data(RunBundle, args)
        uuid = client.derive_bundle('run', targets, command, metadata, worksheet_uuid)
        print uuid
        if args.wait:
            state = self.follow_targets(uuid, [])
            self.do_info_command([uuid], self.create_parser('info'))
        if args.tail:
            state = self.follow_targets(uuid, ['stdout', 'stderr'])
            self.do_info_command([uuid], self.create_parser('info'))

    def do_edit_command(self, argv, parser):
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT)
        args = parser.parse_args(argv)
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        bundle_uuid = client.get_bundle_uuid(worksheet_uuid, args.bundle_spec)
        info = client.get_bundle_info(bundle_uuid)
        bundle_subclass = get_bundle_subclass(info['bundle_type'])
        new_metadata = metadata_util.request_missing_data(
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
          help='delete all downstream dependencies',
        )
        args = parser.parse_args(argv)
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        # Resolve all the bundles first, then delete (this is important since
        # some of the bundle specs are relative).
        bundle_uuids = [client.get_bundle_uuid(worksheet_uuid, bundle_spec) for bundle_spec in args.bundle_spec]
        for bundle_uuid in bundle_uuids:
            client.delete_bundle(bundle_uuid, args.force)

    def do_ls_command(self, argv, parser):
        parser.add_argument(
          '-a', '--all',
          action='store_true',
          help='list all bundles, not just this worksheet',
        )
        parser.add_argument(
          'worksheet_spec',
          help='identifier: %s (default: current worksheet)' % self.GLOBAL_SPEC_FORMAT,
          nargs='?',
        )
        args = parser.parse_args(argv)
        if args.all and args.worksheet_spec:
            raise UsageError("Can't use both --all and a worksheet spec!")
        source = ''
        if args.all:
            client = self.manager.current_client()
            bundle_info_list = client.get_bundle_uuids()
        elif args.worksheet_spec:
            client, worksheet_info = self.parse_client_worksheet_info(args.worksheet_spec)
            bundle_info_list = self.get_worksheet_bundles(worksheet_info)
        else:
            worksheet_info = self.get_current_worksheet_info()
            bundle_info_list = self.get_worksheet_bundles(worksheet_info)
        if bundle_info_list:
            #print 'Bundles%s:\n' % (source,)
            columns = ('uuid', 'name', 'bundle_type', 'state')
            bundle_dicts = [
              {col: info.get(col, info['metadata'].get(col, '')) for col in columns}
              for info in bundle_info_list
            ]
            self.print_table(columns, bundle_dicts)
        else:
            print 'Worksheet %s(%s) is empty.' % (worksheet_info['name'], worksheet_info['uuid'])

    def do_info_command(self, argv, parser):
        parser.add_argument('bundle_spec', help=self.BUNDLE_SPEC_FORMAT)
        parser.add_argument(
          '-c', '--children',
          action='store_true',
          help="print only a list of this bundle's children",
        )
        parser.add_argument(
          '-v', '--verbose',
          action='store_true',
          help="print top-level contents of bundle"
        )
        args = parser.parse_args(argv)

        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        bundle_uuid = client.get_bundle_uuid(worksheet_uuid, args.bundle_spec)
        info = client.get_bundle_info(bundle_uuid, args.children)

        def wrap1(string): return '-- ' + string

        print self.format_basic_info(info)

        if args.children and info['children']:
            print 'children:'
            for child in info['children']:
                print "  %s" % child

        # Verbose output
        if args.verbose:
            print 'contents:'
            info = self.print_target_info((bundle_uuid, ''))
            # Print first 10 lines of stdin and stdout
            contents = info.get('contents')
            if contents:
                for item in contents:
                    if item['name'] not in ['stdout', 'stderr']: continue
                    print wrap1(item['name'])
                    for line in client.head_target((bundle_uuid, item['name']), 10):
                        print line,

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
            stats.append('created:     %s' % (self.time_str(metadata['created']),))
        if 'data_size' in metadata:
            stats.append('size:        %s' % (self.size_str(metadata['data_size']),))
        #fields['stats'] = 'Stats:\n  %s\n' % ('\n  '.join(stats),) if stats else ''
        fields['stats'] = '%s\n' % ('\n'.join(stats),) if stats else ''
        # Compute a nicely-formatted list of hard dependencies. Since this type of
        # dependency is realized within this bundle as a symlink to another bundle,
        # label these dependencies as 'references' in the UI.
        fields['hard_dependencies'] = ''
        fields['dependencies'] = ''
        if info['hard_dependencies']:
            deps = info['hard_dependencies']
            if len(deps) == 1 and not deps[0]['child_path']:
                fields['hard_dependencies'] = 'dependency:\n  %s\n' % (
                  path_util.safe_join(deps[0]['parent_uuid'], deps[0]['parent_path']),)
            else:
                fields['hard_dependencies'] = 'dependencies:\n%s\n' % ('\n'.join(
                  '  %s:%s' % (
                    dep['child_path'],
                    path_util.safe_join(dep['parent_uuid'], dep['parent_path']),
                  ) for dep in sorted(deps, key=lambda dep: dep['child_path'])
                ))
        elif info['dependencies']:
            deps = info['dependencies']
            fields['dependencies'] = 'provenance:\n%s\n' % ('\n'.join(
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
type:        {bundle_type}
name:        {name}
uuid:        {uuid}
data_hash:   {data_hash}
state:       {state}
{stats}description: {description}
{hard_dependencies}{dependencies}{failure_message}
        '''.format(**fields).strip()

    def do_cat_command(self, argv, parser):
        parser.add_argument(
          'target_spec',
          help=self.TARGET_SPEC_FORMAT
        )
        args = parser.parse_args(argv)
        target = self.parse_target(args.target_spec)
        self.print_target_info(target)

    # Helper: shared between info and cat
    def print_target_info(self, target):
        client = self.manager.current_client()
        info = client.get_target_info(target, 1)
        if 'type' not in info:
            self.exit('Target doesn\'t exist: %s/%s' % target)
        if info['type'] == 'file':
            client.cat_target(target, sys.stdout)
        if info['type'] == 'directory':
            contents = [{'name': x['name'], 'size': "%7s" % (self.size_str(x['size']) if x['type'] == 'file' else 'dir')} for x in info['contents']]
            contents = sorted(contents, key=lambda r : r['name'])
            self.print_table(('name', 'size'), contents)
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
        args = parser.parse_args(argv)
        target = self.parse_target(args.target_spec)
        (bundle_uuid, subpath) = target

        # Figure files to display
        subpaths = []
        if args.tail:
            if subpath == '':
                subpaths = ['stdout', 'stderr']
            else:
                subpaths = [subpath]
        state = self.follow_targets(bundle_uuid, subpaths)
        if state != State.READY:
            self.exit(state)

    def follow_targets(self, bundle_uuid, subpaths):
        '''
        Block on the execution of the given bundle.
        subpaths: list of files to print out output as we go along.
        Return READY or FAILED based on whether it was computed successfully.
        '''
        client = self.manager.current_client()
        handles = [None] * len(subpaths)

        # Constants for a simple exponential backoff routine that will decrease the
        # frequency at which we check this bundle's state from 1s to 1m.
        period = 1.0
        backoff = 1.1
        max_period = 60.0
        info = None
        while True:
            # Update bundle info
            info = client.get_bundle_info(bundle_uuid)
            if info['state'] in (State.READY, State.FAILED): break

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
                    result = handle.readline()
                    if result == '': break
                    change = True
                    sys.stdout.write(result)
            sys.stdout.flush()

            # Sleep if nothing happened
            if not change:
                time.sleep(period)
                period = min(backoff*period, max_period)
        for handle in handles:
            if handle: self.close_target_handle(handle)
        return info['state']

    def do_mimic_command(self, argv, parser):
        parser.add_argument(
          'old_input_bundle_spec',
          help=self.BUNDLE_SPEC_FORMAT
        )
        parser.add_argument(
          'new_input_bundle_spec',
          help=self.BUNDLE_SPEC_FORMAT
        )
        parser.add_argument(
          'old_output_bundle_spec',
          help=self.BUNDLE_SPEC_FORMAT
        )
        parser.add_argument(
          'new_output_bundle_name',
          help='name of the new bundle'
        )
        parser.add_argument(
          '-d', '--depth',
          type=int,
          default=10,
          help="number of parents to look back from the old output in search of the old input"
        )
        parser.add_argument(
          '-s', '--stop-early',
          action='store_true',
          default=False,
          help="stop traversing parents when we found old-input"
        )
        args = parser.parse_args(argv)
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        old_input_bundle_uuid = client.get_bundle_uuid(worksheet_uuid, args.old_input_bundle_spec)
        new_input_bundle_uuid = client.get_bundle_uuid(worksheet_uuid, args.new_input_bundle_spec)
        old_output_bundle_uuid = client.get_bundle_uuid(worksheet_uuid, args.old_output_bundle_spec)
        print client.mimic(
            old_input_bundle_uuid, new_input_bundle_uuid, \
            old_output_bundle_uuid, args.new_output_bundle_name, \
            worksheet_uuid, args.depth, args.stop_early)

    def do_macro_command(self, argv, parser):
        '''
        Just like do_mimic_command.
        '''
        parser.add_argument(
          'macro_name',
          help='name of the macro (look for <name>-in and <name>-out bundles)',
        )
        parser.add_argument(
          'new_input_bundle_spec',
          help=self.BUNDLE_SPEC_FORMAT
        )
        parser.add_argument(
          'new_output_bundle_name',
          help='name of the new bundle'
        )
        parser.add_argument(
          '-d', '--depth',
          type=int,
          default=10,
          help="number of parents to look back from the old output in search of the old input"
        )
        parser.add_argument(
          '-s', '--stop-early',
          action='store_true',
          default=False,
          help="stop traversing parents when we found old-input"
        )
        args = parser.parse_args(argv)
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        old_input_bundle_uuid = client.get_bundle_uuid(worksheet_uuid, args.macro_name + '-in')
        new_input_bundle_uuid = client.get_bundle_uuid(worksheet_uuid, args.new_input_bundle_spec)
        old_output_bundle_uuid = client.get_bundle_uuid(worksheet_uuid, args.macro_name + '-out')
        print client.mimic(
            old_input_bundle_uuid, new_input_bundle_uuid, \
            old_output_bundle_uuid, args.new_output_bundle_name, \
            worksheet_uuid, args.depth, args.stop_early)
    
    #############################################################################
    # CLI methods for worksheet-related commands follow!
    #############################################################################

    def get_current_worksheet_info(self):
        '''
        Return the current worksheet's info, or None, if there is none.
        '''
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        return client.get_worksheet_info(worksheet_uuid)

        # Try to resolve worksheet_spec, defaulting if necessary.
        #try:
        #except: UsageError:
        #    if self.verbose >= 1:
        #        print "Can't switch to %s, using %s instead" % (worksheet_spec, Worksheet.DEFAULT_WORKSHEET_NAME)
        #    # Can't switch to worksheet_spec, go to default
        #    if worksheet_spec == Worksheet.DEFAULT_WORKSHEET_NAME:
        #        # If want default and it doesn't exist, just create it
        #        uuid = client.new_worksheet(Worksheet.DEFAULT_WORKSHEET_NAME)
        #    else:
        #        return get_worksheet_info(Worksheet.DEFAULT_WORKSHEET_NAME)

    def do_new_command(self, argv, parser):
        parser.add_argument('name', help='name: ' + spec_util.NAME_REGEX.pattern)
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        uuid = client.new_worksheet(args.name)
        self.manager.set_current_worksheet_uuid(client, uuid)
        print 'Switched to worksheet %s.' % (args.name,)

    def do_add_command(self, argv, parser):
        parser.add_argument(
          'bundle_spec',
          help=self.BUNDLE_SPEC_FORMAT,
          nargs='?')
        parser.add_argument(
          'worksheet_spec',
          help=self.WORKSHEET_SPEC_FORMAT,
          nargs='?',
        )
        parser.add_argument(
          '-m', '--message',
          help='add a text element',
          nargs='?',
        )
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        if args.worksheet_spec:
            worksheet_info = client.get_worksheet_info(args.worksheet_spec)
        else:
            worksheet_info = self.get_current_worksheet_info()
            if not worksheet_info:
                raise UsageError('Specify a worksheet or switch to one with `cl work`.')
        if args.bundle_spec:
            worksheet_uuid = worksheet_info['uuid']
            bundle_uuid = client.get_bundle_uuid(worksheet_uuid, args.bundle_spec)
            client.add_worksheet_item(worksheet_uuid, bundle_uuid)
        if args.message:
            new_items = worksheet_util.get_current_items(worksheet_info) + [(None, args.message, None)]
            client.update_worksheet(worksheet_info, new_items)

    def do_work_command(self, argv, parser):
        parser.add_argument(
          'worksheet_spec',
          help=self.WORKSHEET_SPEC_FORMAT,
          nargs='?',
        )
        args = parser.parse_args(argv)
        if args.worksheet_spec:
            client, worksheet_info = self.parse_client_worksheet_info(args.worksheet_spec)
            if worksheet_info:
                self.manager.set_current_worksheet_uuid(client, worksheet_info['uuid'])
                print 'Switched to worksheet %s.' % (args.worksheet_spec,)
            else:
                self.manager.set_current_worksheet_uuid(client, None)
                print 'Not on any worksheet. Use `cl new` or `cl work` to switch to one.'
        else:
            worksheet_info = self.get_current_worksheet_info()
            if worksheet_info:
                name = worksheet_info['name']
                print 'Currently on worksheet %s.' % (name,)
            else:
                print 'Not on any worksheet. Use `cl new` or `cl work` to switch to one.'

    def do_wedit_command(self, argv, parser):
        parser.add_argument(
          'worksheet_spec',
          help='identifier: [<uuid>|<name>]',
          nargs='?',
        )
        parser.add_argument(
          '--name',
          help='new name: ' + spec_util.NAME_REGEX.pattern,
          nargs='?',
        )
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        if args.worksheet_spec:
            worksheet_info = client.get_worksheet_info(args.worksheet_spec)
        else:
            worksheet_info = self.get_current_worksheet_info()
            if not worksheet_info:
                raise UsageError('Specify a worksheet or switch to one with `cl work`.')
        if args.name:
            client.rename_worksheet(worksheet_info['uuid'], args.name)
        else:
            new_items = worksheet_util.request_new_items(worksheet_info)
            client.update_worksheet(worksheet_info, new_items)
            print 'Saved worksheet %s(%s).' % (worksheet_info['name'], worksheet_info['uuid'])

    def do_wls_command(self, argv, parser):
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        worksheet_dicts = client.list_worksheets()
        if worksheet_dicts:
            self.print_table(('uuid', 'name'), worksheet_dicts)
        else:
            print 'No worksheets found.'

    def do_print_command(self, argv, parser):
        parser.add_argument(
          'worksheet_spec',
          help='identifier: [<uuid>|<name>]',
          nargs='?',
        )
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        if args.worksheet_spec:
            worksheet_info = client.get_worksheet_info(args.worksheet_spec)
        else:
            worksheet_info = self.get_current_worksheet_info()
            if not worksheet_info:
                raise UsageError('Specify a worksheet or switch to one with `cl work`.')
        raw_lines = worksheet_util.get_worksheet_lines(worksheet_info)
        templetized_lines = raw_lines
        for line in templetized_lines:
            print line

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
        client = self.manager.current_client()

        args = parser.parse_args(argv)
        # Destination
        (dest_client, spec) = self.parse_spec(args.dest_worksheet_spec)
        dest_worksheet_uuid = dest_client.get_worksheet_info(spec)['uuid']
        # Things to copy
        bundle_tuples = worksheet_util.get_current_items(client.get_worksheet_info(args.source_worksheet_spec));

        for (bundle_uuid, bundle, type) in bundle_tuples:
            # TODO: copy the non-bundles too
            if bundle_uuid == None: continue
            # Hope that client is local, so it's not too expensive.
            source_path, temp_path = client.download_target((bundle_uuid, ''))
            #print 'source', bundle_uuid, bundle, source_path
            info = client.get_bundle_info(bundle_uuid)

            bundle_type = info['bundle_type']
            print dest_client.upload_bundle(source_path, info, dest_worksheet_uuid)
            if temp_path: path_util.remove(temp_path)

        print 'Copied %s bundles to %s.' % (len(bundle_tuples), dest_worksheet_uuid)


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
            print "%s is not a member of group %s." % (user_info['name'], user_info['group_uuid'])
        else:
            print "Removed %s from group %s." % (user_info['name'], user_info['group_uuid'])

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

    def do_cleanup_command(self, argv, parser):
        # This command only works if client is a LocalBundleClient.
        parser.parse_args(argv)
        client = self.manager.current_client()
        client.bundle_store.full_cleanup(client.model)

    def do_worker_command(self, argv, parser):
        # This command only works if client is a LocalBundleClient.
        parser.add_argument('iterations', type=int, default=None, nargs='?')
        parser.add_argument('sleep', type=int, help='Number of seconds to wait between successive polls', default=1, nargs='?')
        args = parser.parse_args(argv)
        client = self.manager.current_client()
        worker = Worker(client.bundle_store, client.model)
        worker.run_loop(args.iterations, args.sleep)

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
