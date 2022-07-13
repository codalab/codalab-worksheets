"""
BundleCLI is a class that provides one major API method, do_command, which takes
a list of CodaLab bundle system command-line arguments and executes them.

Each of the supported commands corresponds to a method on this class.
This function takes an argument list and does the action.

For example:
  cl upload foo

results in the following:

  BundleCLI.do_command(['upload', 'foo'])
  BundleCLI.do_upload_command(['foo'])
"""
# TODO(sckoo): Move this into a separate CLI directory/package
import argparse
import codecs
import datetime
import inspect
import itertools
import os
import shlex
import shutil
import sys
import time
import textwrap
import json
from collections import defaultdict
from contextlib import closing
from io import BytesIO
from shlex import quote
from typing import Dict
import webbrowser
import argcomplete
from argcomplete.completers import FilesCompleter, ChoicesCompleter

import codalab.model.bundle_model as bundle_model

from codalab.bundles import get_bundle_subclass
from codalab.bundles.make_bundle import MakeBundle
from codalab.bundles.uploaded_bundle import UploadedBundle
from codalab.bundles.run_bundle import RunBundle
from codalab.common import (
    CODALAB_VERSION,
    NotFoundError,
    PermissionError,
    precondition,
    UsageError,
    ensure_str,
    DiskQuotaExceededError,
    parse_linked_bundle_url,
)
from codalab.lib import (
    file_util,
    formatting,
    metadata_util,
    path_util,
    spec_util,
    ui_actions,
    upload_manager,
    worksheet_util,
    bundle_fuse,
)
from codalab.lib.cli_util import (
    nested_dict_get,
    parse_key_target,
    parse_target_spec,
    desugar_command,
    BUNDLES_URL_SEPARATOR,
    INSTANCE_SEPARATOR,
    ADDRESS_SPEC_FORMAT,
    WORKSHEET_SPEC_FORMAT,
    BUNDLE_SPEC_FORMAT,
    WORKSHEETS_URL_SEPARATOR,
    TARGET_SPEC_FORMAT,
    RUN_TARGET_SPEC_FORMAT,
    MAKE_TARGET_SPEC_FORMAT,
    GROUP_SPEC_FORMAT,
    PERMISSION_SPEC_FORMAT,
    UUID_POST_FUNC,
)
from codalab.objects.permission import group_permissions_str, parse_permission, permission_str
from codalab.client.json_api_client import JsonApiRelationship
from codalab.lib.formatting import contents_str
from codalab.lib.completers import (
    AddressesCompleter,
    BundlesCompleter,
    CodaLabCompleter,
    GroupsCompleter,
    NullCompleter,
    require_not_headless,
    TargetsCompleter,
    UnionCompleter,
    WorksheetsCompleter,
)
from codalab.lib.bundle_store import MultiDiskBundleStore
from codalab.lib.print_util import FileTransferProgress
from codalab.worker.un_tar_directory import un_tar_directory
from codalab.worker.download_util import BundleTarget
from codalab.worker.bundle_state import State, LinkFormat
from codalab.rest.worksheet_block_schemas import BlockModes
from codalab.worker.file_util import get_path_size


# Command groupings
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
    'mimic',
    'macro',
    'kill',
    'write',
    'mount',
    'netcat',
    'store',
)

WORKSHEET_COMMANDS = ('new', 'add', 'wadd', 'work', 'print', 'wedit', 'wrm', 'wls')

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

USER_COMMANDS = ('uinfo', 'uedit', 'ufarewell', 'uls')

SERVER_COMMANDS = (
    'workers',
    'bs-add-partition',
    'bs-rm-partition',
    'bs-ls-partitions',
    'bs-health-check',
)

OTHER_COMMANDS = ('help', 'status', 'alias', 'config', 'logout')
# Markdown headings
HEADING_LEVEL_2 = '## '
HEADING_LEVEL_3 = '### '

NO_RESULTS_FOUND = 'No results found'
DEFAULT_BUNDLE_INFO_LIST_FIELDS = (
    'uuid',
    'name',
    'summary',
    'owner',
    'created',
    'data_size',
    'state',
)


class CodaLabArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        # Get a reference to the CLI
        self.cli = kwargs.pop('cli')
        argparse.ArgumentParser.__init__(self, *args, **kwargs)

    def print_help(self, out_file=None):
        # Adapted from original:
        # https://hg.python.org/cpython/file/2.7/Lib/argparse.py
        if out_file is None:
            out_file = self.cli.stdout
        self._print_message(self.format_help(), out_file)

    def error(self, message):
        # Adapted from original:
        # https://hg.python.org/cpython/file/2.7/Lib/argparse.py
        if len(sys.argv) == 1 or (len(sys.argv) == 2 and sys.argv[1] in ['--help', '-h']):
            self.cli.do_command(['help'])
            self.exit(2)
        elif self.cli.headless:
            self.print_usage(self.cli.stderr)
            raise UsageError(message)
        else:
            self.print_usage(self.cli.stderr)
            self.exit(2, '%s: error: %s\n' % (self.prog, message))


class AliasedSubParsersAction(argparse._SubParsersAction):
    """
    Enables aliases for subcommands.
    Stolen from:
    https://gist.github.com/sampsyo/471779
    """

    class _AliasedPseudoAction(argparse.Action):
        def __init__(self, name, aliases=None, help=None):
            dest = name
            sup = super(AliasedSubParsersAction._AliasedPseudoAction, self)
            sup.__init__(option_strings=[], dest=dest, help=help)

    def add_parser(self, name, **kwargs):
        aliases = kwargs.pop('aliases', [])

        parser = super(AliasedSubParsersAction, self).add_parser(name, **kwargs)

        # Do not add aliases to argparser when just autocompleting.
        if '_ARGCOMPLETE' in os.environ:
            return parser

        # Make the aliases work.
        for alias in aliases:
            self._name_parser_map[alias] = parser

        # Make the help text reflect them, first removing old help entry.
        if 'help' in kwargs:
            help = kwargs.pop('help')[0]
            self._choices_actions.pop()
            pseudo_action = self._AliasedPseudoAction(name, aliases, help)
            self._choices_actions.append(pseudo_action)

        return parser


class Commands(object):
    """
    Class initialized once at interpretation-time that registers all the functions
    for building parsers and actions etc.
    """

    class Argument(object):
        """
        Dummy container class to hold the arguments that we will eventually pass into
        `ArgumentParser.add_argument`.
        """

        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    class Command(object):
        """
        A Commands.Command object defines a subcommand in the program argument parser.
        Created by the `Commands.command` function decorator and used internally
        to store information about the subcommands that will eventually be used to
        build a parser for the program.
        """

        def __init__(self, name, aliases, help, arguments, function):
            self.name = name
            self.aliases = aliases
            self.help = help if isinstance(help, list) else [help]
            self.arguments = arguments
            self.function = function

    commands: Dict[str, Command] = {}

    @classmethod
    def command(cls, name, aliases=(), help='', arguments=()):
        """
        Return a decorator function that registers the decoratee as the action function
        for the subcommand defined by the arguments passed here.

        `name`      - name of the subcommand
        `aliases`   - iterable of aliases for the subcommand
        `help`      - help string for the subcommand
        `arguments` - iterable of `Commands.Argument` instances defining the arguments
                      to this subcommand
        """

        def register_command(function):
            cls.commands[name] = cls.Command(name, aliases, help, arguments, function)
            return function

        return register_command

    @classmethod
    def help_text(cls, verbose, markdown):
        def command_name(command):
            name = command
            aliases = cls.commands[command].aliases
            if aliases:
                name += ' (%s)' % ', '.join(list(aliases))
            return name

        available_other_commands = [
            command for command in OTHER_COMMANDS if command in cls.commands
        ]

        indent = 2
        max_length = max(
            len(command_name(command))
            for command in itertools.chain(
                BUNDLE_COMMANDS,
                WORKSHEET_COMMANDS,
                GROUP_AND_PERMISSION_COMMANDS,
                USER_COMMANDS,
                SERVER_COMMANDS,
                available_other_commands,
            )
        )

        def command_help_text(command):
            name = command_name(command)
            command_obj = cls.commands[command]

            def render_args(arguments):
                table = []
                for arg in arguments:
                    if len(arg.args) == 1:
                        table.append([arg.args[0], arg.kwargs['help']])
                    else:
                        table.append([arg.args[0] + ', ' + arg.args[1], arg.kwargs['help']])
                if len(table) == 0:
                    return []
                width = max(len(row[0]) for row in table)
                return (
                    [(' ' * (indent * 2)) + 'Arguments:']
                    + [
                        (' ' * (indent * 3) + '%-' + str(width) + 's  %s') % (row[0], row[1])
                        for row in table
                    ]
                    + ['']
                )

            if verbose:
                if markdown:
                    name = HEADING_LEVEL_3 + name
                return '%s%s\n%s\n%s' % (
                    # This is to make GitHub Markdown format compatible with the Read the Docs theme.
                    ' ' * indent if not markdown else '',
                    name,
                    '\n'.join((' ' * (indent * 2)) + line for line in command_obj.help),
                    '\n'.join(render_args(command_obj.arguments)),
                )
            else:
                return '%s%s%s%s' % (
                    ' ' * indent,
                    name,
                    ' ' * (indent + max_length - len(name)),
                    command_obj.help[0],
                )

        def command_group_help_text(commands):
            return '\n'.join([command_help_text(command) for command in commands])

        def doc_formatter():
            return HEADING_LEVEL_2 if verbose and markdown else ''

        def command_formatter():
            return '`' if verbose and markdown else ''

        return (
            textwrap.dedent(
                """
        Usage: {inline_code}cl <command> <arguments>{inline_code}

        {heading}Commands for bundles
        {bundle_commands}

        {heading}Commands for worksheets
        {worksheet_commands}

        {heading}Commands for groups and permissions
        {group_and_permission_commands}

        {heading}Commands for users
        {user_commands}

        {heading}Commands for managing server
        {server_commands}

        {heading}Other commands
        {other_commands}
        """
            )
            .format(
                heading=doc_formatter(),
                inline_code=command_formatter(),
                bundle_commands=command_group_help_text(BUNDLE_COMMANDS),
                worksheet_commands=command_group_help_text(WORKSHEET_COMMANDS),
                group_and_permission_commands=command_group_help_text(
                    GROUP_AND_PERMISSION_COMMANDS
                ),
                user_commands=command_group_help_text(USER_COMMANDS),
                server_commands=command_group_help_text(SERVER_COMMANDS),
                other_commands=command_group_help_text(available_other_commands),
            )
            .strip()
        )

    @classmethod
    def build_parser(cls, cli):
        """
        Builds an `ArgumentParser` for the cl program, with all the subcommands registered
        through the `Commands.command` decorator.
        """
        parser = CodaLabArgumentParser(
            prog='cl', cli=cli, add_help=False, formatter_class=argparse.RawTextHelpFormatter
        )
        parser.register('action', 'parsers', AliasedSubParsersAction)
        parser.add_argument('-v', '--version', dest='print_version', action='store_true')
        subparsers = parser.add_subparsers(dest='command', metavar='command')

        # Build subparser for each subcommand
        for command in cls.commands.values():
            help = '\n'.join(command.help)
            subparser = subparsers.add_parser(
                command.name,
                cli=cli,
                help=help,
                description=help,
                aliases=command.aliases,
                add_help=True,
                formatter_class=argparse.RawTextHelpFormatter,
            )

            # Register arguments for the subcommand
            for argument in command.arguments:
                argument_kwargs = argument.kwargs.copy()
                completer = argument_kwargs.pop('completer', None)
                argument = subparser.add_argument(*argument.args, **argument_kwargs)

                if completer is not None:
                    # If the completer is subclass of CodaLabCompleter, give it the BundleCLI instance
                    completer_class = (
                        completer if inspect.isclass(completer) else completer.__class__
                    )
                    if issubclass(completer_class, CodaLabCompleter):
                        completer = completer(cli)

                    argument.completer = completer

                elif cli.headless and 'choices' not in argument_kwargs:
                    # If there's no completer, but the CLI is headless, put in a dummy completer to
                    # prevent argcomplete's fallback onto bash autocomplete (which will display
                    # the files in the current working directory by default).
                    # If the 'choices' kwarg is set, we don't have to worry, because argcomplete
                    # will fill in a ChoicesCompleter for us.
                    argument.completer = NullCompleter

            # Associate subcommand with its action function
            subparser.set_defaults(function=command.function)

        return parser

    @staticmethod
    def metadata_arguments(bundle_subclasses):
        """
        Build arguments to a command-line argument parser for all metadata keys
        needed by the given bundle subclasses.
        """
        arguments = {}
        key_to_classes = defaultdict(list)
        for bundle_subclass in bundle_subclasses:
            for spec in bundle_subclass.get_user_defined_metadata():
                key_to_classes[spec.key].append(bundle_subclass)

                # If multiple provided types have the same key, suffix might look like: " (for makes and runs)"
                help_suffix = ''
                if len(bundle_subclasses) > 1:
                    types_list = ' and '.join(
                        [
                            '%ss' % cls.BUNDLE_TYPE
                            for cls in key_to_classes[spec.key]
                            if cls.BUNDLE_TYPE
                        ]
                    )
                    help_suffix = ' (for %s)' % types_list

                args = []
                if spec.short_key:
                    args.append('-%s' % spec.short_key)
                args.append('--%s' % spec.key.replace('_', '-'))

                kwargs = {
                    'dest': metadata_util.metadata_key_to_argument(spec.key),
                    'help': spec.description + help_suffix,
                }
                if spec.completer is not None:
                    kwargs['completer'] = spec.completer
                if issubclass(spec.type, list):
                    kwargs['type'] = str
                    kwargs['nargs'] = '*'
                    kwargs['metavar'] = spec.metavar
                elif issubclass(spec.type, str):
                    kwargs['type'] = str
                    kwargs['metavar'] = spec.metavar
                elif spec.type is bool:
                    kwargs['action'] = 'store_true'
                    kwargs['default'] = None
                else:
                    kwargs['type'] = spec.type
                    kwargs['metavar'] = spec.metavar
                arguments[spec.key] = Commands.Argument(*args, **kwargs)

        return tuple(arguments.values())


class BundleCLI(object):
    def __init__(self, manager, headless=False, stdout=sys.stdout, stderr=sys.stderr):
        self.manager = manager
        self.verbose = manager.cli_verbose
        self.headless = headless
        self.stdout = stdout
        self.stderr = stderr

    def exit(self, message, error_code=1):
        """
        print >>self.stdout, the message to stderr and exit with the given error code.
        """
        precondition(error_code, 'exit called with error_code == 0')
        print(message, file=self.stderr)
        sys.exit(error_code)

    @staticmethod
    def simple_bundle_str(info):
        return '%s(%s)' % (contents_str(nested_dict_get(info, 'metadata', 'name')), info['uuid'])

    @staticmethod
    def simple_user_str(user):
        """
        For a user matching output of UserSchema, return 'user_name(id)'
        """
        if not user:
            return '<anonymous>'
        if 'user_name' not in user:
            return '<anonymous>(%s)' % user['id']
        return '%s(%s)' % (user['user_name'], user['id'])

    @staticmethod
    def simple_group_str(info):
        return '%s(%s)' % (contents_str(info.get('name')), info['id'])

    def target_specs_to_bundle_uuids(
        self, default_client, default_worksheet_uuid, target_specs, allow_remote=True
    ):
        """
        Wrapper for resolve_target that takes a list of target specs and returns a list of bundle uuids.
        Supports the new worksheet//bundle notation, doesn't support the old worksheet/bundle notation that was only partially
        supported
        """
        return [
            self.resolve_target(default_client, default_worksheet_uuid, spec, allow_remote)[
                2
            ].bundle_uuid
            for spec in target_specs
        ]

    def resolve_target(
        self, default_client, default_worksheet_uuid, target_spec, allow_remote=True
    ):
        """
        Input: Target spec in the form of
        [<instance>::][<worksheet_spec>//]<bundle_spec>[/<subpath>]
            where <bundle_spec> is required and the rest are optional.

        Returns:
            - client: A client connected to the instance the target is from if allow_remote is True and an instance is specified,
                otherwise default_client
            - worksheet_uuid: The uuid of the worksheet the target bundle is from,
                a new uuid if the target spec includes a worksheet spec
                same as default_worksheet_uuid otherwise
            - target: a worker.download_util.BundleTarget
        Raises UsageError if allow_remote is False but an instance is specified in the target_spec
        """
        instance, worksheet_spec, bundle_spec, subpath = parse_target_spec(target_spec)

        if bundle_spec is None:
            raise UsageError('Bundle spec is missing')

        if instance is not None:
            if self.headless:
                raise UsageError('Cannot use alias on web CLI')
            if not allow_remote:
                raise UsageError(
                    'Cannot execute command on a target on a remote instance. Please remove the instance reference (i.e. "prod::" in prod::worksheet//bundle)'
                )
            aliases = self.manager.config['aliases']
            if instance in aliases:
                instance = aliases.get(instance)
            client = self.manager.client(instance)
        else:
            client = default_client
        if worksheet_spec is not None:
            worksheet_uuid = BundleCLI.resolve_worksheet_uuid(client, '', worksheet_spec)
        else:
            worksheet_uuid = default_worksheet_uuid

        # Resolve the bundle_spec to a particular bundle_uuid.
        bundle_uuid = BundleCLI.resolve_bundle_uuid(client, worksheet_uuid, bundle_spec)

        # Rest of CLI treats empty string as no subpath and can't handle subpath being None
        subpath = '' if subpath is None else subpath

        return (client, worksheet_uuid, BundleTarget(bundle_uuid, subpath))

    def resolve_key_targets(self, client, worksheet_uuid, target_specs):
        """
        Helper: target_specs is a list of strings which are [<key>]:<target>
        Returns: [(key, worker.download_util.BundleTarget), ...]
        """

        def is_ancestor_or_descendant(path1, path2):
            """
            Return whether path1 is an ancestor of path2 or vice versa.
            """
            return path2.startswith(path1 + '/') or path1.startswith(path2 + '/')

        keys = []
        targets = []
        target_keys_values = [parse_key_target(spec) for spec in target_specs]
        for key, target_spec in target_keys_values:
            for other_key in keys:
                if key == other_key:
                    if key:
                        raise UsageError('Duplicate key: %s' % (key,))
                    else:
                        raise UsageError('Must specify keys when packaging multiple targets!')
                elif is_ancestor_or_descendant(key, other_key):
                    raise UsageError(
                        'A key cannot be an ancestor of another: {} {}'.format(key, other_key)
                    )

            _, worksheet_uuid, target = self.resolve_target(
                client, worksheet_uuid, target_spec, allow_remote=False
            )
            targets.append((key, target))
            keys.append(key)
        return targets

    @staticmethod
    def resolve_bundle_uuid(client, worksheet_uuid, bundle_spec):
        """
        Given a bundle spec, returns the uuid for the bundle, immediately
        returning if the spec is an uuid
        """
        if spec_util.UUID_REGEX.match(bundle_spec):
            return bundle_spec
        return BundleCLI.resolve_bundle_uuids(client, worksheet_uuid, [bundle_spec])[0]

    @staticmethod
    def resolve_bundle_uuids(client, worksheet_uuid, bundle_specs):
        """
        Given specs for bundles, returns their IDs, supports the
        worksheet/bundle notation
        """
        bundles = client.fetch(
            'bundles', params={'worksheet': worksheet_uuid, 'specs': bundle_specs}
        )
        return [b['id'] for b in bundles]

    @staticmethod
    def resolve_worksheet_uuid(client, base_worksheet_uuid, worksheet_spec):
        """
        Avoid making REST call if worksheet_spec is already a uuid.
        """
        if spec_util.UUID_REGEX.match(worksheet_spec):
            worksheet_uuid = worksheet_spec  # Already uuid, don't need to look up specification
        else:
            worksheet_uuid = client.fetch_one(
                'worksheets', params={'base': base_worksheet_uuid, 'specs': [worksheet_spec]}
            )['uuid']
        return worksheet_uuid

    def uls_print_table(self, columns, row_dicts, user_defined=False):
        """
        Pretty-print a list of user info from each row in the given list of dicts.
        """

        # display restricted fields if the server returns those fields - which suggests the user is root
        try:
            if row_dicts and row_dicts[0].get('last_login') and not user_defined:
                columns += ('last_login', 'time', 'disk', 'parallel_run_quota')
                rows = [columns]
            else:
                rows = [columns]
        except KeyError:
            pass

        # Get the contents of the table
        for row_dict in row_dicts:
            row = []
            for col in columns:
                try:
                    if col == 'time':
                        cell = formatting.ratio_str(
                            formatting.duration_str, row_dict['time_used'], row_dict['time_quota']
                        )
                    elif col == 'disk':
                        cell = formatting.ratio_str(
                            formatting.size_str, row_dict['disk_used'], row_dict['disk_quota']
                        )
                    else:
                        cell = row_dict.get(col)
                except KeyError:
                    row.append(' ')
                    continue

                if cell is None:
                    cell = contents_str(cell)
                row.append(cell)
            rows.append(row)

        # Display the table
        lengths = [max(len(str(value)) for value in col) for col in zip(*rows)]
        for (i, row) in enumerate(rows):
            row_strs = []
            for (j, value) in enumerate(row):
                value = str(value)
                length = lengths[j]
                padding = (length - len(value)) * ' '
                if {}.get(columns[j], -1) < 0:
                    row_strs.append(value + padding)
                else:
                    row_strs.append(padding + value)
            print('' + '  '.join(row_strs), file=self.stdout)
            if i == 0:
                print('' + (sum(lengths) + 2 * (len(columns) - 1)) * '-', file=self.stdout)

    def print_table(
        self, columns, row_dicts, post_funcs={}, justify={}, show_header=True, indent=''
    ):
        """
        Pretty-print a list of columns from each row in the given list of dicts.
        """
        rows = [columns]
        # Get the contents of the table
        for row_dict in row_dicts:
            row = []
            for col in columns:
                cell = row_dict.get(col)
                func = post_funcs.get(col)
                if func:
                    cell = worksheet_util.apply_func(func, cell)
                if cell is None:
                    cell = contents_str(cell)
                row.append(cell)
            rows.append(row)

        # Display the table
        lengths = [max(len(str(value)) for value in col) for col in zip(*rows)]
        for (i, row) in enumerate(rows):
            row_strs = []
            for (j, value) in enumerate(row):
                value = str(value)
                length = lengths[j]
                padding = (length - len(value)) * ' '
                if justify.get(columns[j], -1) < 0:
                    row_strs.append(value + padding)
                else:
                    row_strs.append(padding + value)
            if show_header or i > 0:
                print(indent + '  '.join(row_strs), file=self.stdout)
            if i == 0:
                print(indent + (sum(lengths) + 2 * (len(columns) - 1)) * '-', file=self.stdout)

    def parse_spec(self, spec):
        """
        Parse a global spec, which includes the instance and either a bundle or worksheet spec.
        Example: https://worksheets.codalab.org::wine
        Return (client, spec)
        """
        tokens = spec.split(INSTANCE_SEPARATOR)
        if len(tokens) == 1:
            address = self.manager.session()['address']
            spec = tokens[0]
        else:
            address = self.manager.apply_alias(tokens[0])
            spec = tokens[1]
        return self.manager.client(address), spec

    def parse_client_worksheet_uuid(self, spec):
        """
        Return the worksheet referred to by |spec|, and a client for its host.
        """
        if not spec or spec == worksheet_util.CURRENT_WORKSHEET:
            # Empty spec, just return current worksheet.
            client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        else:
            client_is_explicit = spec_util.client_is_explicit(spec)
            client, parsed_spec = self.parse_spec(spec)
            # If we're on the same client, then resolve spec with respect to
            # the current worksheet.
            if client_is_explicit:
                base_worksheet_uuid = None
            else:
                _, base_worksheet_uuid = self.manager.get_current_worksheet_uuid()
            try:
                worksheet_uuid = self.resolve_worksheet_uuid(
                    client, base_worksheet_uuid, parsed_spec
                )
            except ValueError:
                raise UsageError('Invalid spec: "{}"'.format(spec))
        return client, worksheet_uuid

    @staticmethod
    def get_missing_metadata(bundle_subclass, args, initial_metadata=None):
        """
        Return missing metadata for bundles by either returning default metadata values or
        pop up an editor and request that data from the user.
        """
        if not initial_metadata:
            initial_metadata = {
                spec.key: getattr(args, metadata_util.metadata_key_to_argument(spec.key))
                for spec in bundle_subclass.get_user_defined_metadata()
            }
        return metadata_util.fill_missing_metadata(bundle_subclass, args, initial_metadata)

    @staticmethod
    def get_provided_metadata(args):
        """
        Return a dictionary of only the metadata specified by the user in the arguments.
        """
        return {
            metadata_util.metadata_argument_to_key(key): value
            for key, value in vars(args).items()
            if key.startswith('md_') and value is not None
        }

    #############################################################################
    # CLI methods
    #############################################################################

    EDIT_ARGUMENTS = (
        Commands.Argument(
            '-e',
            '--edit',
            action='store_true',
            help='Show an editor to allow editing of the bundle metadata.',
        ),
    )

    # After running a bundle, we can wait for it, possibly observing it's output.
    # These functions are shared across run and mimic.
    WAIT_ARGUMENTS = (
        Commands.Argument('-W', '--wait', action='store_true', help='Wait until run finishes.'),
        Commands.Argument(
            '-t',
            '--tail',
            action='store_true',
            help='Wait until run finishes, displaying stdout/stderr.',
        ),
        Commands.Argument('-v', '--verbose', action='store_true', help='Display verbose output.'),
    )

    MIMIC_ARGUMENTS = (
        Commands.Argument(
            '--depth',
            type=int,
            default=10,
            help='Number of parents to look back from the old output in search of the old input.',
        ),
        Commands.Argument(
            '-s',
            '--shadow',
            action='store_true',
            help='Add the newly created bundles right after the old bundles that are being mimicked.',
        ),
        Commands.Argument(
            '-i',
            '--dry-run',
            help='Perform a dry run (just show what will be done without doing it)',
            action='store_true',
        ),
        Commands.Argument(
            '-w',
            '--worksheet-spec',
            help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
            completer=WorksheetsCompleter,
        ),
        Commands.Argument(
            '-m',
            '--memoize',
            help='If a bundle with the same command and dependencies already exists, return it instead of creating a new one.',
            action='store_true',
        ),
    ) + WAIT_ARGUMENTS

    @staticmethod
    def collapse_bare_command(argv):
        """
        In order to allow specifying a command (i.e. for `cl run`) across multiple tokens,
        we use a special notation '---' to indicate the start of a single contiguous argument.
          key:target ... key:target "command_1 ... command_n"
          <==>
          key:target ... key:target --- command_1 ... command_n
        """
        try:
            i = argv.index('---')
            # Convert the command after '---' to a shell-escaped version of the string.
            shell_escaped_command = [quote(x) for x in argv[i + 1 :]]
            argv = argv[0:i] + [' '.join(shell_escaped_command)]
        except Exception:
            pass

        return argv

    def complete_command(self, command):
        """
        Given a command string, return a list of suggestions to complete the last token.
        """
        parser = Commands.build_parser(self)
        cf = argcomplete.CompletionFinder(parser)
        cword_prequote, cword_prefix, _, comp_words, first_colon_pos = argcomplete.split_line(
            command, len(command)
        )

        # Strip whitespace and parse according to shell escaping rules
        try:
            clean = lambda s: shlex.split(s.strip())[0] if s else ''
        except ValueError as e:
            raise UsageError(str(e))
        return list(
            map(
                clean,
                cf._get_completions(comp_words, cword_prefix, cword_prequote, first_colon_pos),
            )
        )

    def do_command(self, argv, stdout=None, stderr=None):
        parser = Commands.build_parser(self)

        # Call autocompleter (no side effect if os.environ['_ARGCOMPLETE'] is not set)
        argcomplete.autocomplete(parser)

        # Parse arguments
        argv = self.collapse_bare_command(argv)
        if len(argv) > 0 and (argv[0] == '-v' or argv[0] == '--version'):
            self.print_version()
            return

        if len(argv) == 0:
            # In Python 2, running "cl" without any subparsers specified would
            # lead to help being printed. In Python 3, this was removed, so this code
            # re-adds that functionality. See https://bugs.python.org/issue16308
            args = parser.parse_args(['help'])
        else:
            args = parser.parse_args(argv)

        # Bind self (BundleCLI instance) and args to command function
        command_fn = lambda: args.function(self, args)

        if self.verbose >= 2:
            structured_result = command_fn()
        else:
            try:
                structured_result = command_fn()
            except PermissionError as e:
                if self.headless:
                    raise e
                self.exit(str(e))
            except UsageError as e:
                if self.headless:
                    raise e
                self.exit('%s: %s' % (e.__class__.__name__, e))
        return structured_result

    def print_version(self):
        print('CodaLab CLI version %s' % CODALAB_VERSION, file=self.stdout)

    def print_result_limit_info(self, result_size):
        """
        Print at most SEARCH_RESULTS_LIMIT (10) results are shown by default to stderr.
        Args:
            result_size: number of results returned.
        Returns:
            None
        """
        if result_size == bundle_model.SEARCH_RESULTS_LIMIT:
            print(
                'Only {} results are shown. Use .limit=N to show the first N results.'.format(
                    bundle_model.SEARCH_RESULTS_LIMIT
                ),
                file=self.stderr,
            )

    @Commands.command(
        'help',
        help=[
            'Show usage information for commands.',
            '  help           : Show brief description for all commands.',
            '  help -v        : Show full usage information for all commands.',
            '  help -v -m     : Show full usage information for all commands in Markdown format.',
            '  help <command> : Show full usage information for <command>.',
        ],
        arguments=(
            Commands.Argument('command', help='name of command to look up', nargs='?'),
            Commands.Argument(
                '-v', '--verbose', action='store_true', help='Display all options of all commands.'
            ),
            Commands.Argument(
                '-m',
                '--markdown',
                action='store_true',
                help='Auto-generate all options of all commands for CLI markdown in Markdown format.',
            ),
        ),
    )
    def do_help_command(self, args):
        self.print_version()
        if args.command:
            self.do_command([args.command, '--help'])
            return
        print(Commands.help_text(args.verbose, args.markdown), file=self.stdout)

    @Commands.command(
        'store',
        help=['Add a bundle store.'],
        arguments=(
            Commands.Argument(
                'command',
                help='Set to "add" to add a new bundle store, "ls" to list bundle stores, and "rm" to remove a bundle store.',
                nargs='?',
            ),
            Commands.Argument(
                'bundle_store_uuid',
                help='Bundle store uuid. Specified when running "cl store rm [uuid]".',
                nargs='?',
            ),
            Commands.Argument(
                '-n', '--name', help='Name of the bundle store; must be globally unique.',
            ),
            Commands.Argument(
                '--storage-type',
                help='Storage type of the bundle store. Acceptable values are "disk" and "azure_blob".',
            ),
            Commands.Argument(
                '--storage-format',
                help='Storage format of the bundle store. Acceptable values are "uncompressed" and "compressed_v1". Optional; if unspecified, will be set to an optimal default.',
            ),
            Commands.Argument(
                '--url', help='A self-referential URL that points to the bundle store.',
            ),
            Commands.Argument(
                '--authentication', help='Key for authentication that the bundle store uses.',
            ),
        ),
    )
    def do_store_command(self, args):
        client = self.manager.current_client()
        if args.command == 'add':
            bundle_store_info = {
                "name": args.name,
                "storage_type": args.storage_type,
                "storage_format": args.storage_format,
                "url": args.url,
                "authentication": args.authentication,
            }
            if args.url is not None:
                inferred_type = parse_linked_bundle_url(args.url).storage_type
                if args.storage_type is None:
                    bundle_store_info["storage_type"] = inferred_type
                elif args.storage_type != inferred_type:
                    raise UsageError(
                        f"Bundle store {args.url} only supports storage type: {inferred_type}"
                    )
            new_bundle_store = client.create('bundle_stores', bundle_store_info)
            print(new_bundle_store["id"], file=self.stdout)
        elif args.command == 'ls':
            bundle_stores = client.fetch('bundle_stores')
            self.print_table(["id", "name", "storage_type", "storage_format"], bundle_stores)
        elif args.command == 'rm':
            client.delete('bundle_stores', resource_ids=[args.bundle_store_uuid])
            print(args.bundle_store_uuid, file=self.stdout)
        else:
            raise UsageError(
                f"cl store {args.command} is not supported. Only the following subcommands are supported: 'cl store add', 'cl store ls', 'cl store rm'."
            )

    @Commands.command('status', aliases=('st',), help='Show current client status.')
    def do_status_command(self, args):
        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        worksheet_info = client.fetch('worksheets', worksheet_uuid)

        if not self.headless:
            print("codalab_home: %s" % self.manager.codalab_home, file=self.stdout)
            print("session: %s" % self.manager.session_name(), file=self.stdout)
            address = self.manager.session()['address']
            print("client_version: %s" % CODALAB_VERSION, file=self.stdout)
            print("server_version: %s" % worksheet_info['meta']['version'], file=self.stdout)
            print("address: %s" % address, file=self.stdout)
            state = self.manager.state['auth'].get(address, {})
            if 'username' in state:
                print("username: %s" % state['username'], file=self.stdout)

        print(
            "current_worksheet: %s" % self.worksheet_url_and_name(worksheet_info), file=self.stdout
        )
        print("user: %s" % self.simple_user_str(client.fetch('user')), file=self.stdout)

    @Commands.command(
        'logout',
        help='Logout of the current session, or a specific instance.',
        arguments=(
            Commands.Argument(
                'alias',
                help='Alias or URL of instance from which to logout. Default is the current session.',
                nargs='?',
            ),
        ),
    )
    def do_logout_command(self, args):
        self._fail_if_headless(args)
        if args.alias:
            address = self.manager.apply_alias(args.alias)
            self.manager.logout(address)
        else:
            client = self.manager.current_client()
            self.manager.logout(client.address)

    @Commands.command(
        'alias',
        help=[
            'Manage CodaLab instance aliases. These are mappings from names to CodaLab Worksheet servers.',
            '  alias                   : List all aliases.',
            '  alias <name>            : Shows which instance <name> is bound to.',
            '  alias <name> <instance> : Binds <name> to <instance>.',
        ],
        arguments=(
            Commands.Argument('name', help='Name of the alias (e.g., main).', nargs='?'),
            Commands.Argument(
                'instance',
                help='Instance to bind the alias to (e.g., https://worksheets.codalab.org).',
                nargs='?',
            ),
            Commands.Argument('-r', '--remove', help='Remove this alias.', action='store_true'),
        ),
    )
    def do_alias_command(self, args):
        """
        Show, add, modify, delete aliases (mappings from names to instances).
        Only modifies the CLI configuration, doesn't need a REST client.
        """
        self._fail_if_headless(args)
        aliases = self.manager.config['aliases']
        if args.name:
            instance = aliases.get(args.name)
            if args.remove:
                del aliases[args.name]
                self.manager.save_config()
            elif args.instance:
                aliases[args.name] = args.instance
                self.manager.save_config()
            else:
                print(
                    args.name + ': ' + formatting.verbose_contents_str(instance), file=self.stdout
                )
        else:
            for name, instance in aliases.items():
                print(name + ': ' + instance, file=self.stdout)

    @Commands.command(
        'config',
        help=[
            'Set CodaLab configuration.',
            '  config <key>         : Shows the value of <key>.',
            '  config <key> <value> : Sets <key> to <value>.',
        ],
        arguments=(
            Commands.Argument('key', help='key to set (e.g., cli/verbose).'),
            Commands.Argument(
                'value',
                help='Instance to bind the alias to (e.g., https://worksheets.codalab.org).',
                nargs='?',
            ),
            Commands.Argument('-r', '--remove', help='Remove this key.', action='store_true'),
        ),
    )
    def do_config_command(self, args):
        """
        Only modifies the CLI configuration, doesn't need a REST client.
        """
        self._fail_if_headless(args)
        config = self.manager.config

        # Suppose key = "a/b/c".

        # Traverse "a/b" to the appropriate section of the config.
        path = args.key.split('/')
        for x in path[:-1]:
            if x not in config:
                config[x] = {}
            config = config[x]

        def auto_convert_type(value):
            if value == 'true':
                return True
            if value == 'false':
                return False
            try:
                return int(value)
            except Exception:
                pass
            try:
                return float(value)
            except Exception:
                pass
            return value

        # Set "c" to the value.
        key = path[-1]
        if args.remove:  # Remove key
            del config[key]
            self.manager.save_config()
        if args.value:  # Modify value
            config[key] = auto_convert_type(args.value)
            self.manager.save_config()
        else:  # Print out value
            print(config[key])

    @Commands.command(
        'workers',
        help=['Display information about workers that you have connected to the CodaLab instance.'],
        arguments=(),
    )
    def do_workers_command(self, args):
        client = self.manager.current_client()
        raw_info = client.get_workers_info()
        raw_info.sort(key=lambda r: r['worker_id'])

        columns = [
            'worker_id',
            'cpus',
            'gpus',
            'memory',
            'free_disk',
            'last_checkin',
            'group',
            'tag',
            'runs',
            'shared_file_system',
            'tag_exclusive',
            'exit_after_num_runs',
            'is_terminating',
        ]

        data = []

        for worker in raw_info:
            data.append(
                {
                    'worker_id': worker['worker_id'],
                    'cpus': '{}/{}'.format(worker['cpus_in_use'], worker['cpus']),
                    'gpus': '{}/{}'.format(worker['gpus_in_use'], worker['gpus']),
                    'memory': formatting.size_str(worker['memory_bytes']),
                    'free_disk': formatting.size_str(worker['free_disk_bytes']),
                    'last_checkin': '{} ago'.format(
                        formatting.duration_str(int(time.time()) - worker['checkin_time'])
                    ),
                    'group': worker['group_uuid'],
                    'tag': worker['tag'],
                    'runs': ",".join([uuid[0:8] for uuid in worker['run_uuids']]),
                    'shared_file_system': worker['shared_file_system'],
                    'tag_exclusive': worker['tag_exclusive'],
                    'exit_after_num_runs': worker['exit_after_num_runs'],
                    'is_terminating': worker['is_terminating'],
                }
            )

        self.print_table(columns, data)

    @Commands.command(
        'upload',
        aliases=('up',),
        help=[
            'Create a bundle by uploading an existing file/directory.',
            '  upload <path>            : Upload contents of file/directory <path> as a bundle.',
            '  upload <path> ... <path> : Upload one bundle whose directory contents contain <path> ... <path>.',
            '  upload -c <text>         : Upload one bundle whose file contents is <text>.',
            '  upload <url>             : Upload one bundle whose file contents is downloaded from <url>.',
            'Most of the other arguments specify metadata fields.',
        ],
        arguments=(
            Commands.Argument(
                'path',
                help='Paths of the files/directories to upload, or a single URL to upload.',
                nargs='*',
                completer=require_not_headless(FilesCompleter()),
            ),
            Commands.Argument(
                '-c', '--contents', help='Specify the string contents of the bundle.'
            ),
            Commands.Argument(
                '-L',
                '--follow-symlinks',
                help='Always dereference (follow) symlinks.',
                action='store_true',
            ),
            Commands.Argument(
                '-x', '--exclude-patterns', help='Exclude these file patterns.', nargs='*'
            ),
            Commands.Argument(
                '-g', '--git', help='Path is a git repository, git clone it.', action='store_true'
            ),
            Commands.Argument(
                '-p',
                '--pack',
                help='If path is an archive file (e.g., zip, tar.gz), keep it packed.',
                action='store_true',
                default=False,
            ),
            Commands.Argument(
                '-z',
                '--force-compression',
                help='Always use compression (this may speed up single-file uploads over a slow network).',
                action='store_true',
                default=False,
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Upload to this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
            Commands.Argument(
                '-i',
                '--ignore',
                help='Name of file containing patterns matching files and directories to exclude from upload. '
                'This option is currently only supported with the GNU tar library.',
            ),
            Commands.Argument(
                '-l',
                '--link',
                help='Makes the path the source of truth of the bundle, meaning that the server will retrieve the '
                'bundle directly from the specified path rather than storing its contents'
                'in its own bundle store.',
                action='store_true',
                default=False,
            ),
            Commands.Argument(
                '-a',
                '--use-azure-blob-beta',
                help='Use Azure Blob Storage to store files (beta feature).',
                action='store_true',
                default=False,
            ),
        )
        + Commands.metadata_arguments([UploadedBundle])
        + EDIT_ARGUMENTS,
    )
    def do_upload_command(self, args):
        from codalab.lib import zip_util

        if args.contents is None and not args.path:
            raise UsageError("Nothing to upload.")

        if args.contents is not None and args.path:
            raise UsageError(
                "Upload does not support mixing content strings and paths(local files and URLs)."
            )

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        # Build bundle info
        metadata = self.get_missing_metadata(UploadedBundle, args, initial_metadata={})
        if args.contents is not None and metadata['name'] is None:
            metadata['name'] = 'contents'
        if not args.pack and zip_util.path_is_archive(metadata['name']):
            # name = 'test.zip' => name = 'test'
            metadata['name'] = zip_util.strip_archive_ext(metadata['name'])
        bundle_info = {
            'bundle_type': 'dataset',  # TODO: deprecate Dataset and ProgramBundles
            'metadata': metadata,
        }

        # Option 1: --link
        if args.link:
            if len(args.path) != 1:
                raise UsageError("Only a single path can be uploaded when using --link.")
            # If link_url is a relative path, prepend the current working directory to it.
            bundle_info['metadata']['link_url'] = (
                args.path[0]
                if os.path.isabs(args.path[0])
                else os.path.join(os.getcwd(), args.path[0])
            )
            bundle_info['metadata']['link_format'] = LinkFormat.RAW

            new_bundle = client.create('bundles', bundle_info, params={'worksheet': worksheet_uuid})

        # Option 2: Upload contents string
        elif args.contents is not None:
            contents_buffer = BytesIO(args.contents.encode())
            new_bundle = client.create('bundles', bundle_info, params={'worksheet': worksheet_uuid})
            client.upload_contents_blob(
                new_bundle['id'],
                fileobj=contents_buffer,
                params={
                    'filename': 'contents',
                    'unpack': False,
                    'state_on_success': State.READY,
                    'finalize_on_success': True,
                    'use_azure_blob_beta': args.use_azure_blob_beta,
                    'store': metadata.get('store') or '',
                },
            )

        # Option 3: Upload URL(s)
        elif any(map(path_util.path_is_url, args.path)):
            if not all(map(path_util.path_is_url, args.path)):
                raise UsageError("URLs and local files cannot be uploaded in the same bundle.")
            if len(args.path) > 1:
                raise UsageError("Only one URL can be specified at a time.")
            bundle_info['metadata']['source_url'] = str(args.path)

            new_bundle = client.create('bundles', bundle_info, params={'worksheet': worksheet_uuid})
            client.upload_contents_blob(
                new_bundle['id'],
                params={
                    'urls': args.path,
                    'git': args.git,
                    'state_on_success': State.READY,
                    'finalize_on_success': True,
                    'use_azure_blob_beta': args.use_azure_blob_beta,
                    'store': metadata.get('store') or '',
                },
            )

        # Option 4: Upload file(s) from the local filesystem
        else:
            if self.headless:
                raise UsageError("Local file paths not allowed without a filesystem.")
            # Check that the upload paths exist
            for path in args.path:
                path_util.check_isvalid(path_util.normalize(path), 'upload')

            # Canonicalize paths (e.g., removing trailing /)
            sources = [path_util.normalize(path) for path in args.path]
            # Calculate size of sources
            total_bundle_size = sum([get_path_size(source) for source in sources])
            user = client.fetch('user')
            disk_left = user['disk_quota'] - user['disk_used']
            if disk_left - total_bundle_size <= 0:
                raise DiskQuotaExceededError(
                    'Attempted to upload bundle of size %s with only %s remaining in user\'s disk quota.'
                    % (formatting.size_str(total_bundle_size), formatting.size_str(disk_left))
                )

            print("Preparing upload archive...", file=self.stderr)
            if args.ignore:
                print(
                    "Excluding files and directories specified by %s." % args.ignore,
                    file=self.stderr,
                )

            packed = zip_util.pack_files_for_upload(
                sources,
                should_unpack=(not args.pack),
                follow_symlinks=args.follow_symlinks,
                exclude_patterns=args.exclude_patterns,
                force_compression=args.force_compression,
                ignore_file=args.ignore,
            )

            # Create bundle.
            # We must create the bundle right before we upload it because we
            # perform some input validation in functions such as
            # zip_util.pack_files_for_upload that we want to fail fast before
            # we try to create or upload the bundle, otherwise you will be left
            # with empty shells of failed uploading bundles on your worksheet.
            new_bundle = client.create(
                'bundles',
                bundle_info,
                params={'worksheet': worksheet_uuid, 'wait_for_upload': True},
            )

            print(
                'Uploading %s (%s) to %s' % (packed['filename'], new_bundle['id'], client.address),
                file=self.stderr,
            )
            uploader = upload_manager.ClientUploadManager(
                client, stdout=self.stdout, stderr=self.stderr
            )
            uploader.upload_to_bundle_store(
                bundle=new_bundle,
                packed_source=packed,
                use_azure_blob_beta=args.use_azure_blob_beta,
                destination_bundle_store=metadata.get('store'),
            )

        print(new_bundle['id'], file=self.stdout)

    @Commands.command(
        'download',
        aliases=('down',),
        help='Download bundle from a CodaLab instance.',
        arguments=(
            Commands.Argument('target_spec', help=TARGET_SPEC_FORMAT, completer=BundlesCompleter),
            Commands.Argument(
                '-o',
                '--output-path',
                help='Path to download bundle to.  By default, the bundle or subpath name in the current directory is used.',
            ),
            Commands.Argument(
                '-f',
                '--force',
                action='store_true',
                help='Overwrite the output path if a file already exists.',
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_download_command(self, args):
        self._fail_if_headless(args)

        default_client, default_worksheet_uuid = self.parse_client_worksheet_uuid(
            args.worksheet_spec
        )
        client, worksheet_uuid, target = self.resolve_target(
            default_client, default_worksheet_uuid, args.target_spec
        )

        # Figure out where to download.
        info = client.fetch('bundles', target.bundle_uuid)
        if args.output_path:
            local_path = args.output_path
        else:
            local_path = (
                nested_dict_get(info, 'metadata', 'name', default='untitled')
                if target.subpath == ''
                else os.path.basename(target.subpath)
            )
        final_path = os.path.join(os.getcwd(), local_path)
        if os.path.exists(final_path):
            if args.force:
                shutil.rmtree(final_path)
            else:
                print('Local file/directory \'%s\' already exists.' % local_path, file=self.stdout)
                return

        # Do the download.
        target_info = client.fetch_contents_info(target, 0)
        if target_info['type'] == 'link':
            raise UsageError('Downloading symlinks is not allowed.')

        print(
            'Downloading %s/%s => %s' % (self.simple_bundle_str(info), target.subpath, final_path),
            file=self.stdout,
        )

        progress = FileTransferProgress('Received ', f=self.stderr)
        contents = file_util.tracked(
            client.fetch_contents_blob(target_info['resolved_target']), progress.update
        )
        with progress, closing(contents):
            if target_info['type'] == 'directory':
                un_tar_directory(contents, final_path, 'gz', force=args.force)
            elif target_info['type'] == 'file':
                with open(final_path, 'wb') as out:
                    shutil.copyfileobj(contents, out)

    def copy_bundle(
        self,
        source_client,
        source_bundle_uuid,
        dest_client,
        dest_worksheet_uuid,
        copy_dependencies,
        add_to_worksheet,
    ):
        """
        Helper function that supports cp and wadd.
        Copies the source bundle to the target worksheet.
        Currently, this goes between two clients by downloading to the local
        disk and then uploading, which is not the most efficient.
        But having two clients talk directly to each other is complicated...
        """
        if copy_dependencies:
            source_info = source_client.fetch('bundles', source_bundle_uuid)
            # Copy all the dependencies, but only for run dependencies.
            for dep in source_info['dependencies']:
                self.copy_bundle(
                    source_client,
                    dep['parent_uuid'],
                    dest_client,
                    dest_worksheet_uuid,
                    False,
                    add_to_worksheet,
                )
            self.copy_bundle(
                source_client,
                source_bundle_uuid,
                dest_client,
                dest_worksheet_uuid,
                False,
                add_to_worksheet,
            )
            return

        # Check if the bundle already exists on the destination, then don't copy it
        # (although metadata could be different on source and destination).
        # TODO: sync the metadata.
        try:
            dest_client.fetch('bundles', source_bundle_uuid)
        except NotFoundError:
            bundle_exists = False
        else:
            bundle_exists = True

        # Bundle already exists, just need to add to worksheet if desired.
        if bundle_exists:
            if add_to_worksheet:
                dest_client.create(
                    'worksheet-items',
                    data={
                        'type': worksheet_util.TYPE_BUNDLE,
                        'worksheet': JsonApiRelationship('worksheets', dest_worksheet_uuid),
                        'bundle': JsonApiRelationship('bundles', source_bundle_uuid),
                    },
                    params={'uuid': dest_worksheet_uuid},
                )
            return

        source_info = source_client.fetch('bundles', source_bundle_uuid)
        if source_info is None:
            print('Unable to read bundle %s' % source_bundle_uuid, file=self.stdout)
            return

        source_desc = self.simple_bundle_str(source_info)
        if source_info['state'] not in [State.READY, State.FAILED]:
            print(
                'Not copying %s because it has non-final state %s'
                % (source_desc, source_info['state']),
                file=self.stdout,
            )
            return

        print("Copying %s..." % source_desc, file=self.stdout)

        # Create the bundle, copying over metadata from the source bundle
        dest_bundle = dest_client.create(
            'bundles',
            source_info,
            params={
                'worksheet': dest_worksheet_uuid,
                'detached': not add_to_worksheet,
                'wait_for_upload': True,
            },
        )

        # Fetch bundle metadata of bundle contents from source client
        try:
            target_info = source_client.fetch_contents_info(BundleTarget(source_bundle_uuid, ''))
        except NotFoundError:
            # When bundle content doesn't exist, update the bundle state with final states and return
            dest_client.upload_contents_blob(
                dest_bundle['id'],
                params={
                    'state_on_success': source_info['state'],  # copy bundle state
                    'finalize_on_success': True,
                },
            )
            return

        # Collect information about how server should unpack
        filename = nested_dict_get(source_info, 'metadata', 'name')
        # Zip bundle directory if there is any
        if target_info['type'] == 'directory':
            filename += '.tar.gz'
            unpack = True
        else:
            unpack = False
        # Fetch bundle content from source client
        source_file = source_client.fetch_contents_blob(BundleTarget(source_bundle_uuid, ''))
        # Send file over
        progress = FileTransferProgress('Copied ', f=self.stderr)
        with closing(source_file), progress:
            dest_client.upload_contents_blob(
                dest_bundle['id'],
                fileobj=source_file,
                params={
                    'filename': filename,
                    'unpack': unpack,
                    'state_on_success': source_info['state'],  # copy bundle state
                    'finalize_on_success': True,
                },
                progress_callback=progress.update,
            )

    @Commands.command(
        'make',
        help=[
            'Create a bundle by combining parts of existing bundles.',
            '  make <bundle>/<subpath>                : New bundle\'s contents are copied from <subpath> in <bundle>.',
            '  make <key>:<bundle> ... <key>:<bundle> : New bundle contains file/directories <key> ... <key>, whose contents are given.',
        ],
        arguments=(
            Commands.Argument(
                'target_spec', help=MAKE_TARGET_SPEC_FORMAT, nargs='+', completer=BundlesCompleter
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        )
        + Commands.metadata_arguments([MakeBundle])
        + EDIT_ARGUMENTS,
    )
    def do_make_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        targets = self.resolve_key_targets(client, worksheet_uuid, args.target_spec)
        # Support anonymous make calls by replacing None keys with ''
        targets = [('' if key is None else key, val) for key, val in targets]
        metadata = self.get_missing_metadata(MakeBundle, args)
        new_bundle = client.create(
            'bundles',
            self.derive_bundle(MakeBundle.BUNDLE_TYPE, None, targets, metadata),
            params={'worksheet': worksheet_uuid},
        )

        print(new_bundle['uuid'], file=self.stdout)

    def wait(self, client, args, uuid):
        """Wait for a run bundle to finish. Called by run and mimic."""
        # Build new args for a hacky artificial call to the info command
        info_args = argparse.Namespace()
        info_args.worksheet_spec = args.worksheet_spec
        info_args.verbose = args.verbose
        info_args.bundle_spec = [uuid]
        info_args.field = None
        info_args.raw = False

        if args.wait:
            self.follow_targets(client, uuid, [])
            self.do_info_command(info_args)
        if args.tail:
            # Follow from the beginnings of the files since we just start running them
            self.follow_targets(client, uuid, ['stdout', 'stderr'], from_start=True)
            if args.verbose:
                self.do_info_command(info_args)

    def derive_bundle(self, bundle_type, command, targets, metadata):
        # List the dependencies of this bundle on its targets.
        dependencies = []
        for (child_path, parent_target) in targets:
            dependencies.append(
                {
                    'child_path': child_path,
                    'parent_uuid': parent_target.bundle_uuid,
                    'parent_path': parent_target.subpath,
                }
            )
        return {
            'bundle_type': bundle_type,
            'command': command,
            'metadata': metadata,
            'dependencies': dependencies,
        }

    @Commands.command(
        'run',
        help='Create a bundle by running a program bundle on an input bundle.',
        arguments=(
            Commands.Argument(
                'target_spec', help=RUN_TARGET_SPEC_FORMAT, nargs='*', completer=TargetsCompleter
            ),
            Commands.Argument(
                'command',
                metavar='[---] command',
                help='Arbitrary Linux command to execute.',
                completer=NullCompleter,
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
            Commands.Argument(  # Internal for web FE positioned insert.
                '-a', '--after_sort_key', help='Insert after this sort_key', completer=NullCompleter
            ),
            Commands.Argument(
                '-m',
                '--memoize',
                help='If a bundle with the same command and dependencies already exists, return it instead of creating a new one.',
                action='store_true',
            ),
            Commands.Argument(
                '-i',
                '--interactive',
                help='Beta feature - Start an interactive session to construct your run command.',
                action='store_true',
            ),
        )
        + Commands.metadata_arguments([RunBundle])
        + EDIT_ARGUMENTS
        + WAIT_ARGUMENTS,
    )
    def do_run_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        args.target_spec, args.command = desugar_command(args.target_spec, args.command)
        metadata = self.get_missing_metadata(RunBundle, args)
        targets = self.resolve_key_targets(client, worksheet_uuid, args.target_spec)

        if args.interactive:
            from codalab.lib.interactive_session import InteractiveSession

            # Disable cl run --interactive on headless systems
            self._fail_if_headless(args)

            # Fetch bundle locations from the server
            bundle_uuids = [bundle_target.bundle_uuid for _, bundle_target in targets]
            bundles_locations = client.get_bundles_locations(bundle_uuids)

            docker_image = metadata.get('request_docker_image', None)
            if not docker_image:
                # If a Docker image is not specified, use the default CPU worker image for the interactive session
                docker_image = self.manager.config['workers']['default_cpu_image']

            # Start an interactive session to allow users to figure out the command to run
            session = InteractiveSession(
                docker_image, args.command, self.manager, targets, bundles_locations, args.verbose
            )
            command = session.start()
            session.cleanup()
        else:
            command = args.command

        if not command:
            raise UsageError('The command cannot be empty.')

        params = {'worksheet': worksheet_uuid}
        if args.after_sort_key:
            params['after_sort_key'] = args.after_sort_key
        if args.memoize:
            dependencies = [
                {'child_path': key, 'parent_uuid': bundle_target.bundle_uuid}
                for key, bundle_target in targets
            ]
            # A list of matched uuids in the order they were created.
            memoized_bundles = client.fetch(
                'bundles', params={'command': command, 'dependencies': json.dumps(dependencies)}
            )

        if args.memoize and len(memoized_bundles) > 0:
            new_bundle = memoized_bundles[-1]
            print(new_bundle['uuid'], file=self.stdout)
            self.copy_bundle(
                source_client=client,
                source_bundle_uuid=new_bundle['uuid'],
                dest_client=client,
                dest_worksheet_uuid=worksheet_uuid,
                copy_dependencies=False,
                add_to_worksheet=True,
            )
        else:
            new_bundle = client.create(
                'bundles',
                self.derive_bundle(RunBundle.BUNDLE_TYPE, command, targets, metadata),
                params=params,
            )
            print(new_bundle['uuid'], file=self.stdout)
            self.wait(client, args, new_bundle['uuid'])

    @Commands.command(
        'edit',
        aliases=('e',),
        help=[
            'Edit an existing bundle\'s metadata.',
            '  edit           : Popup an editor.',
            '  edit -n <name> : Edit the name metadata field (same for other fields).',
            '  edit -T <tag> ... <tag> : Set the tags of the bundle (e.g., training-dataset).',
        ],
        arguments=(
            Commands.Argument('bundle_spec', help=BUNDLE_SPEC_FORMAT, completer=BundlesCompleter),
            Commands.Argument(
                '-n',
                '--name',
                help='Change the bundle name (format: %s).' % spec_util.NAME_REGEX.pattern,
            ),
            Commands.Argument(
                '-T', '--tags', help='Change tags (must appear after worksheet_spec).', nargs='*'
            ),
            Commands.Argument('-d', '--description', help='New bundle description.'),
            Commands.Argument(
                '--freeze',
                help='Freeze bundle to prevent future metadata modification.',
                action='store_true',
            ),
            Commands.Argument(
                '--unfreeze',
                help='Unfreeze bundle to allow future metadata modification.',
                action='store_true',
            ),
            Commands.Argument(
                '--anonymous',
                help='Set bundle to be anonymous (identity of the owner will NOT be visible to users without \'all\' permission on the bundle).',
                dest='anonymous',
                action='store_true',
                default=None,
            ),
            Commands.Argument(
                '--not-anonymous',
                help='Set bundle to be NOT anonymous.',
                dest='anonymous',
                action='store_false',
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
            Commands.Argument(
                '-f',
                '--field',
                help='Edit any specified bundle metadata field.',
                nargs=2,
                metavar=('FIELD', 'VALUE'),
            ),
        ),
    )
    def do_edit_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        info = client.fetch_one(
            'bundles', params={'specs': args.bundle_spec, 'worksheet': worksheet_uuid}
        )

        bundle_subclass = get_bundle_subclass(info['bundle_type'])

        metadata_update = {}
        bundle_update = {}
        if args.name:
            metadata_update['name'] = args.name
        if args.description:
            metadata_update['description'] = args.description
        if args.tags:
            metadata_update['tags'] = args.tags
        if args.anonymous is not None:
            bundle_update['is_anonymous'] = args.anonymous
        if args.freeze:
            bundle_update['frozen'] = datetime.datetime.utcnow().isoformat()
        if args.unfreeze:
            bundle_update['frozen'] = None
        if args.field:
            metadata_update[args.field[0]] = args.field[1]

        # Prompt user for edits via an editor when no edits provided by command line options
        if not self.headless and not metadata_update and not bundle_update:
            metadata_update = metadata_util.request_missing_metadata(
                bundle_subclass, info['metadata']
            )

        if bundle_update or metadata_update:
            bundle_update.update({'id': info['id'], 'bundle_type': info['bundle_type']})
            if metadata_update:
                bundle_update['metadata'] = metadata_update

            client.update('bundles', bundle_update)
            print("Saved metadata for bundle %s." % (info['id']), file=self.stdout)

    @Commands.command(
        'detach',
        aliases=('de',),
        help='Detach a bundle from this worksheet, but doesn\'t remove the bundle.',
        arguments=(
            Commands.Argument(
                'bundle_spec', help=BUNDLE_SPEC_FORMAT, nargs='+', completer=BundlesCompleter
            ),
            Commands.Argument(
                '-n',
                '--index',
                help='Specifies which occurrence (1, 2, ...) of the bundle to detach, counting from the end.',
                type=int,
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_detach_command(self, args):
        """
        Removes the given bundles from the given worksheet (but importantly
        doesn't delete the actual bundles, unlike rm).
        """
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        # Resolve all the bundles first, then detach.
        # This is important since some of the bundle specs (^1 ^2) are relative.
        bundle_uuids = self.target_specs_to_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        worksheet_info = client.fetch(
            'worksheets', worksheet_uuid, params={'include': ['items', 'items.bundle']}
        )

        # Number the bundles: c c a b c => 3 2 1 1 1
        items = worksheet_info['items']
        indices = [None] * len(
            items
        )  # Parallel array to items that stores the index associated with that bundle uuid
        uuid2index = (
            {}
        )  # bundle uuid => index of the bundle (at the end, number of times it occurs on the worksheet)
        for i, item in reversed(list(enumerate(items))):
            if item['type'] == worksheet_util.TYPE_BUNDLE:
                uuid = item['bundle']['id']
                indices[i] = uuid2index[uuid] = uuid2index.get(uuid, 0) + 1

        # Detach the items.
        new_items = []
        for i, item in enumerate(items):
            detach = False
            if item['type'] == worksheet_util.TYPE_BUNDLE:
                uuid = item['bundle']['id']
                # If want to detach uuid, then make sure we're detaching the
                # right index or if the index is not specified, that it's
                # unique.
                if uuid in bundle_uuids:
                    if args.index is None:
                        if uuid2index[uuid] != 1:
                            raise UsageError(
                                'bundle %s shows up more than once, need to specify index' % uuid
                            )
                        detach = True
                    else:
                        if args.index > uuid2index[uuid]:
                            raise UsageError(
                                'bundle %s shows up %d times, can\'t get index %d'
                                % (uuid, uuid2index[uuid], args.index)
                            )
                        if args.index == indices[i]:
                            detach = True
            if not detach:
                new_items.append(item)

        client.create(
            'worksheet-items', data=new_items, params={'replace': True, 'uuid': worksheet_uuid}
        )

    @Commands.command(
        'rm',
        help='Remove a bundle (permanent!).',
        arguments=(
            Commands.Argument(
                'bundle_spec', help=BUNDLE_SPEC_FORMAT, nargs='*', completer=BundlesCompleter
            ),
            Commands.Argument(
                '--force',
                action='store_true',
                help='Delete bundle (DANGEROUS - breaking dependencies!)',
            ),
            Commands.Argument(
                '-r',
                '--recursive',
                action='store_true',
                help='Delete all bundles downstream that depend on this bundle (DANGEROUS - could be a lot!).',
            ),
            Commands.Argument(
                '-d',
                '--data-only',
                action='store_true',
                help='Keep the bundle metadata, but remove the bundle contents on disk.',
            ),
            Commands.Argument(
                '-i',
                '--dry-run',
                help='Perform a dry run (just show what will be done without doing it).',
                action='store_true',
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
            # TODO: this feature is not implemented yet, implement as part of https://github.com/codalab/codalab-worksheets/issues/3923.
            # Commands.Argument(
            #     '-b',
            #     '--store',
            #     help='Keeps the bundle, but removes the bundle contents from the specified bundle store.',
            # ),
        ),
    )
    def do_rm_command(self, args):
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        # Resolve all the bundles first, then delete.
        # This is important since some of the bundle specs (^1 ^2) are relative.
        bundle_uuids = self.target_specs_to_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        deleted_uuids = client.delete(
            'bundles',
            bundle_uuids,
            params={
                'force': args.force,
                'recursive': args.recursive,
                'data-only': args.data_only,
                'dry-run': args.dry_run,
            },
        )['meta']['ids']

        if args.dry_run:
            bundles = client.fetch('bundles', params={'specs': deleted_uuids, 'include': ['owner']})
            print(
                'This command would permanently remove the following bundles (not doing so yet):',
                file=self.stdout,
            )
            self.print_bundle_info_list(bundles, uuid_only=False, print_ref=False)
        else:
            for uuid in deleted_uuids:
                print(uuid, file=self.stdout)

    @Commands.command(
        'search',
        aliases=('s',),
        help=[
            'Search for bundles on a CodaLab instance (returns 10 results by default).',
            '  search <keyword> ... <keyword>         : Name or uuid contains each <keyword>.',
            '  search name=<value>                    : Name is <value>, where `name` can be any metadata field (e.g., description).',
            '  search type=<type>                     : Bundle type is <type> (`run` or `dataset`).',
            '  search id=<id>                         : Has <id> (integer used for sorting, strictly increasing over time).',
            '  search uuid=<uuid>                     : UUID is <uuid> (e.g., 0x...).',
            '  search state=<state>                   : State is <state> (e.g., staged, running, ready, failed).',
            '  search command=<command>               : Command to run is <command>.',
            '  search dependency=<uuid>               : Has a dependency with <uuid>.',
            '  search dependency/<name>=<uuid>        : Has a dependency <name>:<uuid>.',
            '',
            '  search owner=<owner>                   : Owned by <owner> (e.g., `pliang`).',
            '  search .mine                           : Owned by me.',
            '  search group=<group>                   : Shared with <group>.',
            '  search .shared                         : Shared with any of the groups I\'m in.',
            '',
            '  search host_worksheet=<worksheet>      : On <worksheet>.',
            '  search .floating                       : Not on any worksheet.',
            '',
            '  search .limit=<limit>                  : Limit the number of results to the top <limit> (e.g., 50).',
            '  search .offset=<offset>                : Return results starting at <offset>.',
            '',
            '  search .before=<datetime>              : Returns bundles created before (inclusive) given ISO 8601 timestamp (e.g., .before=2042-03-14).',
            '  search .after=<datetime>               : Returns bundles created after (inclusive) given ISO 8601 timestamp (e.g., .after=2120-10-15T00:00:00-08).',
            '',
            '  search size=.sort                      : Sort by a particular field (where `size` can be any metadata field).',
            '  search size=.sort-                     : Sort by a particular field in reverse (e.g., `size`).',
            '  search .last                           : Sort in reverse chronological order (equivalent to id=.sort-).',
            '  search .count                          : Count the number of matching bundles.',
            '  search size=.sum                       : Compute total of a particular field (e.g., `size`).',
            '  search .format=<format>                : Apply <format> function (see worksheet markdown).',
        ],
        arguments=(
            Commands.Argument('keywords', help='Keywords to search for.', nargs='+'),
            Commands.Argument(
                '-f',
                '--field',
                type=str,
                default=','.join(DEFAULT_BUNDLE_INFO_LIST_FIELDS),
                help='Print out these comma-separated fields in the results table',
            ),
            Commands.Argument(
                '-a',
                '--append',
                help='Append these bundles to the current worksheet.',
                action='store_true',
            ),
            Commands.Argument('-u', '--uuid-only', help='Print only uuids.', action='store_true'),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_search_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        bundles = client.fetch(
            'bundles',
            params={'worksheet': worksheet_uuid, 'keywords': args.keywords, 'include': ['owner']},
        )

        # Print direct numeric result
        if 'meta' in bundles:
            print(bundles['meta']['result'], file=self.stdout)
            return

        # Print table
        if len(bundles) > 0:
            self.print_bundle_info_list(
                bundles, uuid_only=args.uuid_only, print_ref=False, fields=args.field.split(",")
            )
        elif not args.uuid_only:
            print(NO_RESULTS_FOUND, file=self.stderr)

        # Add the bundles to the current worksheet
        if args.append:
            client.create(
                'worksheet-items',
                data=[
                    {
                        'worksheet': JsonApiRelationship('worksheets', worksheet_uuid),
                        'bundle': JsonApiRelationship('bundles', bundle['uuid']),
                        'type': worksheet_util.TYPE_BUNDLE,
                    }
                    for bundle in bundles
                ],
                params={'uuid': worksheet_uuid},
            )
            worksheet_info = client.fetch('worksheets', worksheet_uuid)
            print(
                'Added %d bundles to %s'
                % (len(bundles), self.worksheet_url_and_name(worksheet_info)),
                file=self.stdout,
            )

        return {'refs': self.create_reference_map('bundle', bundles)}

    def create_reference_map(self, info_type, info_list):
        """
        Return dict of dicts containing name, uuid and type for each
        bundle/worksheet in the info_list. This information is needed to recover
        URL on the web client.
        """
        return {
            worksheet_util.apply_func(UUID_POST_FUNC, info['uuid']): {
                'type': info_type,
                'uuid': info['uuid'],
                'name': info.get('metadata', info).get('name', None),
            }
            for info in info_list
            if 'uuid' in info
        }

    @Commands.command(
        name='ls',
        help='List bundles in a worksheet.',
        arguments=(
            Commands.Argument(
                '-f',
                '--field',
                type=str,
                default=','.join(DEFAULT_BUNDLE_INFO_LIST_FIELDS),
                help='Print out these comma-separated fields in the results table',
            ),
            Commands.Argument('-u', '--uuid-only', help='Print only uuids.', action='store_true'),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_ls_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        worksheet_info = client.fetch(
            'worksheets',
            worksheet_uuid,
            params={
                'include': [
                    'owner',
                    'group_permissions',
                    'items',
                    'items.bundle',
                    'items.bundle.owner',
                ]
            },
        )
        if not args.uuid_only:
            print(self._worksheet_description(worksheet_info), file=self.stdout)
        bundle_info_list = [
            item['bundle'] for item in worksheet_info['items'] if item['type'] == 'bundle'
        ]
        self.print_bundle_info_list(
            bundle_info_list, args.uuid_only, print_ref=True, fields=args.field.split(",")
        )
        return {'refs': self.create_reference_map('bundle', bundle_info_list)}

    def _worksheet_description(self, worksheet_info):
        fields = [
            ('Worksheet', self.worksheet_url_and_name(worksheet_info)),
            ('Title', formatting.verbose_contents_str(worksheet_info['title'])),
            ('Tags', ' '.join(worksheet_info['tags'])),
            (
                'Owner',
                self.simple_user_str(worksheet_info['owner'])
                + (' [anonymous]' if worksheet_info['is_anonymous'] else ''),
            ),
            (
                'Permissions',
                group_permissions_str(worksheet_info['group_permissions'])
                + (' [frozen]' if worksheet_info['frozen'] else ''),
            ),
        ]
        return '\n'.join('### %s: %s' % (k, v) for k, v in fields)

    def print_bundle_info_list(
        self, bundle_info_list, uuid_only, print_ref, fields=DEFAULT_BUNDLE_INFO_LIST_FIELDS
    ):
        """
        Helper function: print >>self.stdout, a nice table showing all provided bundles.
        """
        if uuid_only:
            for bundle_info in bundle_info_list:
                print(bundle_info['uuid'], file=self.stdout)
        else:

            def get(i, info, col):
                if col == 'ref':
                    return '^' + str(len(bundle_info_list) - i)
                else:
                    return info.get(col, nested_dict_get(info, 'metadata', col))

            self.print_result_limit_info(len(bundle_info_list))

            for bundle_info in bundle_info_list:
                bundle_info['owner'] = nested_dict_get(bundle_info, 'owner', 'user_name')

            columns = (('ref',) if print_ref else ()) + tuple(fields)
            post_funcs = {'uuid': UUID_POST_FUNC, 'created': 'date', 'data_size': 'size'}
            justify = {'data_size': 1, 'ref': 1}
            bundle_dicts = [
                {col: get(i, info, col) for col in columns}
                for i, info in enumerate(bundle_info_list)
            ]
            self.print_table(columns, bundle_dicts, post_funcs=post_funcs, justify=justify)

    @Commands.command(
        'info',
        aliases=('i',),
        help='Show detailed information for a bundle.',
        arguments=(
            Commands.Argument(
                'bundle_spec', help=BUNDLE_SPEC_FORMAT, nargs='+', completer=BundlesCompleter
            ),
            Commands.Argument('-f', '--field', help='Print out these comma-separated fields.'),
            Commands.Argument(
                '-r',
                '--raw',
                action='store_true',
                help='Print out raw information (no rendering of numbers/times).',
            ),
            Commands.Argument(
                '-v',
                '--verbose',
                action='store_true',
                help='Print top-level contents of bundle, children bundles, and host worksheets.',
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_info_command(self, args):
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        bundles = client.fetch(
            'bundles',
            params={
                'specs': args.bundle_spec,
                'worksheet': worksheet_uuid,
                'include': ['owner']
                + (['children', 'group_permissions', 'host_worksheets'] if args.verbose else []),
            },
        )

        for i, info in enumerate(bundles):
            if args.field:
                # Display individual fields (arbitrary genpath)
                values = []
                for genpath in args.field.split(','):
                    if worksheet_util.is_file_genpath(genpath):
                        value = contents_str(
                            client.interpret_file_genpaths([(info['id'], genpath, None)])[0]
                        )
                    else:
                        value = worksheet_util.interpret_genpath(info, genpath)
                    values.append(value)
                print('\t'.join(map(str, values)), file=self.stdout)
            else:
                # Display all the fields
                if i > 0:
                    print()
                self.print_basic_info(client, info, args.raw)
                if args.verbose:
                    self.print_children(info)
                    self.print_host_worksheets(info)
                    self.print_permissions(info)
                    self.print_contents(client, info)

        # Headless client should fire OpenBundle UI action if no special flags used
        if self.headless and not (args.field or args.raw or args.verbose):
            return ui_actions.serialize([ui_actions.OpenBundle(bundle['id']) for bundle in bundles])

    @staticmethod
    def key_value_str(key, value):
        return '%-26s: %s' % (
            key,
            formatting.verbose_contents_str(str(value) if value is not None else None),
        )

    def print_basic_info(self, client, info, raw):
        """
        print >>self.stdout, the basic information for a bundle (key/value pairs).
        """

        metadata = info['metadata']
        lines = []  # The output that we're accumulating

        # Bundle fields
        for key in (
            'bundle_type',
            'uuid',
            'data_hash',
            'state',
            'command',
            'frozen',
            'is_anonymous',
        ):
            if not raw:
                if key not in info:
                    continue
            lines.append(self.key_value_str(key, info.get(key)))

        # Owner info
        lines.append(self.key_value_str('owner', self.simple_user_str(info['owner'])))

        # Metadata fields (standard)
        cls = get_bundle_subclass(info['bundle_type'])

        # Show all hidden fields for root user
        show_hidden = client.fetch('user')['is_root_user']

        for key, value in worksheet_util.get_formatted_metadata(cls, metadata, raw, show_hidden):
            lines.append(self.key_value_str(key, value))

        bundle_locations = client.get_bundle_locations((info.get('uuid')))
        if len(bundle_locations) > 0:
            if raw:
                bundle_locations = str(bundle_locations)
                lines.append(self.key_value_str('bundle stores', bundle_locations))
            else:
                bundle_locations = [
                    location.get('attributes').get('name') for location in bundle_locations
                ]
                lines.append(self.key_value_str('bundle stores', ','.join(bundle_locations)))

        # Metadata fields (non-standard)
        standard_keys = set(spec.key for spec in cls.METADATA_SPECS)
        for key, value in metadata.items():
            if key in standard_keys:
                continue
            lines.append(self.key_value_str(key, value))

        # Dependencies (both hard dependencies and soft)
        def display_dependencies(label, deps):
            lines.append(label + ':')
            for dep in deps:
                child = dep['child_path']
                parent = path_util.safe_join(
                    contents_str(dep['parent_name']) + '(' + dep['parent_uuid'] + ')',
                    dep['parent_path'],
                )
                lines.append('  %s: %s' % (child, parent))

        if info['dependencies']:
            deps = info['dependencies']
            display_dependencies('dependencies', deps)

        print('\n'.join(lines), file=self.stdout)

    def print_children(self, info):
        print('children:', file=self.stdout)
        for child in info['children']:
            print("  %s" % self.simple_bundle_str(child), file=self.stdout)

    def print_host_worksheets(self, info):
        print('host_worksheets:', file=self.stdout)
        for host_worksheet_info in info['host_worksheets']:
            print("  %s" % self.worksheet_url_and_name(host_worksheet_info), file=self.stdout)

    def print_permissions(self, info):
        print('permission: %s' % permission_str(info['permission']), file=self.stdout)
        print('group_permissions:', file=self.stdout)
        print('  %s' % group_permissions_str(info.get('group_permissions', [])), file=self.stdout)

    def print_contents(self, client, info):
        def wrap(string):
            return '=== ' + string + ' preview ==='

        print(wrap('contents'), file=self.stdout)
        bundle_uuid = info['uuid']
        info = self.print_target_info(client, BundleTarget(bundle_uuid, ''), head=10)
        if info is not None and info['type'] == 'directory':
            for item in info['contents']:
                if item['name'] not in ['stdout', 'stderr']:
                    continue
                print(wrap(item['name']), file=self.stdout)
                self.print_target_info(client, BundleTarget(bundle_uuid, item['name']), head=10)

    @Commands.command(
        'mount',
        help=[
            'Beta feature: this command may change in a future release. Mount the contents of a bundle at a read-only mountpoint.'
        ],
        arguments=(
            Commands.Argument('target_spec', help=TARGET_SPEC_FORMAT, completer=TargetsCompleter),
            Commands.Argument(
                '--mountpoint', help='Empty directory path to set up as the mountpoint for FUSE.'
            ),
            Commands.Argument(
                '--verbose', help='Verbose mode for BundleFUSE.', action='store_true', default=False
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_mount_command(self, args):
        if bundle_fuse.fuse_is_available:
            self._fail_if_headless(args)  # Disable on headless systems

            default_client, default_worksheet_uuid = self.parse_client_worksheet_uuid(
                args.worksheet_spec
            )
            client, worksheet_uuid, target = self.resolve_target(
                default_client, default_worksheet_uuid, args.target_spec
            )

            mountpoint = path_util.normalize(args.mountpoint)
            path_util.check_isvalid(mountpoint, 'mount')
            print(
                'BundleFUSE mounting bundle {} on {}'.format(target.bundle_uuid, mountpoint),
                file=self.stdout,
            )
            print(
                'BundleFUSE will run and maintain the mounted filesystem in the foreground. CTRL-C to cancel.',
                file=self.stdout,
            )
            bundle_fuse.bundle_mount(client, mountpoint, target.bundle_uuid, args.verbose)
            print('BundleFUSE shutting down.', file=self.stdout)
        else:
            print('fuse is not installed', file=self.stdout)

    @Commands.command(
        'netcat',
        help=[
            'Beta feature: this command may change in a future release. Send raw data into a port of a running bundle'
        ],
        arguments=(
            Commands.Argument('bundle_spec', help=BUNDLE_SPEC_FORMAT, completer=BundlesCompleter),
            Commands.Argument('port', type=int, help='Port'),
            Commands.Argument(
                'message',
                metavar='[---] message',
                help='Arbitrary message to send.',
                completer=NullCompleter,
            ),
            Commands.Argument('-f', '--file', help='Add this file at end of message'),
            Commands.Argument(
                '--verbose', help='Verbose mode.', action='store_true', default=False
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_netcat_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        client, worksheet_uuid, target = self.resolve_target(
            client, worksheet_uuid, args.bundle_spec
        )
        message = args.message
        if args.file:
            with open(args.file) as f:
                message += f.read()
        contents = client.netcat(target.bundle_uuid, port=args.port, data={"message": message})
        with closing(contents):
            shutil.copyfileobj(contents, self.stdout.buffer)

    @Commands.command(
        'cat',
        help=[
            'Print the contents of a file/directory in a bundle.',
            'Note that cat on a directory will list its files.',
        ],
        arguments=(
            Commands.Argument('target_spec', help=TARGET_SPEC_FORMAT, completer=TargetsCompleter),
            Commands.Argument(
                '--head', type=int, metavar='NUM', help='Display first NUM lines of contents.'
            ),  # `-h` conflicts with help flag
            Commands.Argument(
                '-t', '--tail', type=int, metavar='NUM', help='Display last NUM lines of contents'
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_cat_command(self, args):

        default_client, default_worksheet_uuid = self.parse_client_worksheet_uuid(
            args.worksheet_spec
        )
        client, worksheet_uuid, target = self.resolve_target(
            default_client, default_worksheet_uuid, args.target_spec
        )
        info = self.print_target_info(client, target, head=args.head, tail=args.tail)
        if info is None:
            raise UsageError(
                "Target '{}' doesn't exist in bundle {}".format(target.subpath, target.bundle_uuid)
            )

    # Helper: shared between info and cat
    def print_target_info(self, client, target, head=None, tail=None):
        try:
            info = client.fetch_contents_info(target, 1)
        except NotFoundError:
            print(formatting.verbose_contents_str(None), file=self.stdout)
            return None

        info_type = info.get('type')
        if info_type == 'file':
            kwargs = {}
            if head is not None:
                kwargs['head'] = head
            if tail is not None:
                kwargs['tail'] = tail

            # uses the same parameters as the front-end bundle interface
            if self.headless:
                kwargs['head'] = 50
                kwargs['tail'] = 50
                kwargs['truncation_text'] = '\n... truncated ...\n\n'

            contents = client.fetch_contents_blob(info['resolved_target'], **kwargs)
            with closing(contents):
                try:
                    shutil.copyfileobj(contents, self.stdout.buffer)
                except AttributeError:
                    # self.stdout will have buffer attribute when it's an io.TextIOWrapper object. However, when
                    # self.stdout gets reassigned to an io.StringIO object, self.stdout.buffer won't exist.
                    # Therefore, we try to directly write file content as a String object to self.stdout.
                    self.stdout.write(ensure_str(contents.read()))

            if self.headless:
                print(
                    '--Web CLI detected, truncated output to first 50 and last 50 lines.--',
                    file=self.stdout,
                )

        def size(x):
            t = x.get('type', '???')
            if t == 'file':
                return formatting.size_str(x['size'])
            elif t == 'directory':
                return 'dir'
            else:
                return t

        if info_type == 'directory':
            contents = [
                {
                    'name': x['name'] + (' -> ' + x['link'] if 'link' in x else ''),
                    'size': size(x),
                    'perm': oct(x['perm']) if 'perm' in x else '',
                }
                for x in info['contents']
            ]
            contents = sorted(contents, key=lambda r: r['name'])
            self.print_table(('name', 'perm', 'size'), contents, justify={'size': 1}, indent='')

        if info_type == 'link':
            print(' -> ' + info['link'], file=self.stdout)

        return info

    @Commands.command(
        'wait',
        help='Wait until a run bundle finishes.',
        arguments=(
            Commands.Argument('target_spec', help=TARGET_SPEC_FORMAT, completer=BundlesCompleter),
            Commands.Argument(
                '-t',
                '--tail',
                action='store_true',
                help='Print out the tail of the file or bundle and block until the run bundle has finished running.',
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_wait_command(self, args):
        self._fail_if_headless(args)

        default_client, default_worksheet_uuid = self.parse_client_worksheet_uuid(
            args.worksheet_spec
        )
        client, worksheet_uuid, target = self.resolve_target(
            default_client, default_worksheet_uuid, args.target_spec
        )

        # Figure files to display
        subpaths = []
        if args.tail:
            if target.subpath == '':
                subpaths = ['stdout', 'stderr']
            else:
                subpaths = [target.subpath]
        state = self.follow_targets(client, target.bundle_uuid, subpaths)
        if state != State.READY:
            self.exit(state)
        print(target.bundle_uuid, file=self.stdout)

    def follow_targets(self, client, bundle_uuid, subpaths, from_start=False):
        """
        Block on the execution of the given bundle.

        :param client: JsonApiClient
        :param bundle_uuid: uuid of bundle to follow
        :param subpaths: list of files to print >>self.stdout, out output as we go along.
        :param from_start: whether to follow from the beginning of the file, or
            start from near the end of the file (like tail)
        :return: 'ready' or 'failed' based on whether it was computed successfully.
        """
        subpath_is_file = [None] * len(subpaths)
        subpath_offset = [None] * len(subpaths)
        subpath_targets = [None] * len(subpaths)

        SLEEP_PERIOD = 1.0

        # Wait for all files to become ready (or until run finishes)
        run_state = client.fetch('bundles', bundle_uuid)['state']
        for subpath in subpaths:
            while run_state not in State.FINAL_STATES:
                run_state = client.fetch('bundles', bundle_uuid)['state']
                try:
                    client.fetch_contents_info(BundleTarget(bundle_uuid, subpath), 0)
                except NotFoundError:
                    time.sleep(SLEEP_PERIOD)
                    continue
                break

        while True:
            if run_state not in State.FINAL_STATES:
                run_state = client.fetch('bundles', bundle_uuid)['state']

            # Read data.
            for i in range(0, len(subpaths)):
                # If the subpath we're interested in appears, check if it's a
                # file and if so, initialize the offset.
                if subpath_is_file[i] is None:
                    target_info = client.fetch_contents_info(
                        BundleTarget(bundle_uuid, subpaths[i]), 0
                    )
                    subpath_targets[i] = target_info['resolved_target']
                    if target_info['type'] == 'file':
                        subpath_is_file[i] = True
                        if from_start:
                            subpath_offset[i] = 0
                        else:
                            # Go to near the end of the file (TODO: make this match up with lines)
                            subpath_offset[i] = max(target_info['size'] - 64, 0)
                    else:
                        subpath_is_file[i] = False

                if not subpath_is_file[i]:
                    continue

                # Read from that file.
                while True:
                    READ_LENGTH = 16384
                    byte_range = (subpath_offset[i], subpath_offset[i] + READ_LENGTH - 1)
                    with closing(
                        client.fetch_contents_blob(subpath_targets[i], byte_range)
                    ) as contents:
                        result = contents.read()
                    if not result:
                        break
                    subpath_offset[i] += len(result)
                    self.stdout.write(ensure_str(result))
                    if len(result) < READ_LENGTH:
                        # No more to read.
                        break

            self.stdout.flush()

            # The run finished and we read all the data.
            if run_state in State.FINAL_STATES:
                break

            # Sleep, since we've finished reading all the data available.
            time.sleep(SLEEP_PERIOD)

        return run_state

    @Commands.command(
        'mimic',
        help=[
            'Creates a set of bundles based on analogy with another set.',
            '  mimic <run>      : Rerun the <run> bundle.',
            '  mimic A B        : For all run bundles downstream of A, rerun with B instead.',
            '  mimic A X B -n Y : For all run bundles used to produce X depending on A, rerun with B instead to produce Y.',
            'Any provided metadata arguments will override the original metadata in mimicked bundles.',
        ],
        arguments=(
            Commands.Argument(
                'bundles',
                help='Bundles: old_input_1 ... old_input_n old_output new_input_1 ... new_input_n (%s).'
                % BUNDLE_SPEC_FORMAT,
                nargs='+',
                completer=BundlesCompleter,
            ),
        )
        + Commands.metadata_arguments([MakeBundle, RunBundle])
        + MIMIC_ARGUMENTS,
    )
    def do_mimic_command(self, args):
        self.mimic(args)

    @Commands.command(
        'macro',
        help=[
            'Use mimicry to simulate macros.',
            '  macro M A B <name1>:C <name2>:D <=> mimic M-in1 M-in2 M-in-name1 M-in-name2 M-out A B C D',
        ],
        arguments=(
            Commands.Argument(
                'macro_name',
                help='Name of the macro (look for <macro_name>-in1, <macro_name>-in-<name>, ..., and <macro_name>-out bundles).',
            ),
            Commands.Argument(
                'bundles',
                help='Bundles: new_input_1 ... new_input_n named_input_name:named_input_bundle other_named_input_name:other_named_input_bundle (%s)'
                % BUNDLE_SPEC_FORMAT,
                nargs='+',
                completer=BundlesCompleter,
            ),
        )
        + Commands.metadata_arguments([MakeBundle, RunBundle])
        + MIMIC_ARGUMENTS,
    )
    def do_macro_command(self, args):
        """
        Just like do_mimic_command.
        """
        # For a macro, it's important that the name be not-null, so that we
        # don't create bundles called '<macro_name>-out', which would clash
        # next time we try to use the macro.
        if not getattr(args, metadata_util.metadata_key_to_argument('name')):
            setattr(args, metadata_util.metadata_key_to_argument('name'), 'new')

        # Reduce to the mimic case
        named_user_inputs, named_macro_inputs, numbered_user_inputs = [], [], []

        for bundle in args.bundles:
            if ':' in bundle:
                input_name, input_bundle = bundle.split(':', 1)
                named_user_inputs.append(input_bundle)
                named_macro_inputs.append(args.macro_name + '-in-' + input_name)
            else:
                numbered_user_inputs.append(bundle)

        numbered_macro_inputs = [
            args.macro_name + '-in' + str(i + 1) for i in range(len(numbered_user_inputs))
        ]

        args.bundles = (
            numbered_macro_inputs
            + named_macro_inputs
            + [args.macro_name + '-out']
            + numbered_user_inputs
            + named_user_inputs
        )

        self.mimic(args)

    def mimic(self, args):
        """
        Use args.bundles to generate a call to bundle_util.mimic_bundles()
        """
        from codalab.lib import bundle_util

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        try:
            bundle_uuids = self.target_specs_to_bundle_uuids(client, worksheet_uuid, args.bundles)
        except NotFoundError as e:
            # Maybe they're trying with old syntax (worksheet/bundle)
            try:
                bundle_uuids = BundleCLI.resolve_bundle_uuids(client, worksheet_uuid, args.bundles)
            except NotFoundError:
                # If this doesn't work either, raise the outer error as that's the non-deprecated
                # interpretation of what happened
                raise e
        metadata = self.get_provided_metadata(args)
        output_name = metadata.pop('name', None)

        # Two cases for args.bundles
        # (A) old_input_1 ... old_input_n            new_input_1 ... new_input_n [go to all outputs]
        # (B) old_input_1 ... old_input_n old_output new_input_1 ... new_input_n [go from inputs to given output]
        n = len(bundle_uuids) // 2
        if len(bundle_uuids) % 2 == 0:  # (A)
            old_inputs = bundle_uuids[0:n]
            old_output = None
            new_inputs = bundle_uuids[n:]
        else:  # (B)
            old_inputs = bundle_uuids[0:n]
            old_output = bundle_uuids[n]
            new_inputs = bundle_uuids[n + 1 :]

        plan = bundle_util.mimic_bundles(
            client,
            old_inputs,
            old_output,
            new_inputs,
            output_name,
            worksheet_uuid,
            args.depth,
            args.shadow,
            args.dry_run,
            metadata_override=metadata,
            memoize=args.memoize,
        )
        for (old, new) in plan:
            print(
                '%s => %s' % (self.simple_bundle_str(old), self.simple_bundle_str(new)),
                file=self.stderr,
            )
        if len(plan) > 0:
            new_uuid = plan[-1][1]['uuid']  # Last new uuid to be created
            self.wait(client, args, new_uuid)
            print(new_uuid, file=self.stdout)
        else:
            print('Nothing to be done.', file=self.stdout)

    @Commands.command(
        'kill',
        help='Instruct the appropriate worker to terminate the running bundle(s).',
        arguments=(
            Commands.Argument(
                'bundle_spec', help=BUNDLE_SPEC_FORMAT, nargs='+', completer=BundlesCompleter
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_kill_command(self, args):
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        bundle_uuids = self.target_specs_to_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        for bundle_uuid in bundle_uuids:
            print(bundle_uuid, file=self.stdout)
        client.create('bundle-actions', [{'type': 'kill', 'uuid': uuid} for uuid in bundle_uuids])

    @Commands.command(
        'write',
        help='Instruct the appropriate worker to write a small file into the running bundle(s).',
        arguments=(
            Commands.Argument('target_spec', help=TARGET_SPEC_FORMAT, completer=BundlesCompleter),
            Commands.Argument('string', help='Write this string to the target file.'),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_write_command(self, args):
        default_client, default_worksheet_uuid = self.parse_client_worksheet_uuid(
            args.worksheet_spec
        )
        client, worksheet_uuid, target = self.resolve_target(
            default_client, default_worksheet_uuid, args.target_spec
        )
        client.create(
            'bundle-actions',
            {
                'type': 'write',
                'uuid': target.bundle_uuid,
                'subpath': target.subpath,
                'string': args.string,
            },
        )
        print(target.bundle_uuid, file=self.stdout)

    @Commands.command(
        'open',
        aliases=('o',),
        help='Open bundle(s) detail page(s) in a local web browser.',
        arguments=(
            Commands.Argument(
                'bundle_spec', help=BUNDLE_SPEC_FORMAT, nargs='+', completer=BundlesCompleter
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_open_command(self, args):
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        bundles = client.fetch(
            'bundles', params={'specs': args.bundle_spec, 'worksheet': worksheet_uuid},
        )

        for info in bundles:
            webbrowser.open(self.bundle_url(info['id']))

        # Headless client should fire OpenBundle UI action
        if self.headless:
            return ui_actions.serialize([ui_actions.OpenBundle(bundle['id']) for bundle in bundles])

    def bundle_url(self, bundle_uuid):
        return '%s%s%s' % (self.manager.session()['address'], BUNDLES_URL_SEPARATOR, bundle_uuid)

    #############################################################################
    # CLI methods for worksheet-related commands follow!
    #############################################################################

    def worksheet_url(self, worksheet_uuid):
        return '%s%s%s' % (
            self.manager.session()['address'],
            WORKSHEETS_URL_SEPARATOR,
            worksheet_uuid,
        )

    def worksheet_url_and_name(self, worksheet_info):
        return '%s (%s)' % (self.worksheet_url(worksheet_info['uuid']), worksheet_info['name'],)

    @Commands.command(
        'new',
        help='Create a new worksheet.',
        arguments=(
            Commands.Argument(
                'name', help='Name of worksheet (%s).' % spec_util.NAME_REGEX.pattern
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_new_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        new_worksheet = client.create('worksheets', data={'name': args.name})
        print(new_worksheet['uuid'], file=self.stdout)
        if self.headless:
            return ui_actions.serialize([ui_actions.OpenWorksheet(new_worksheet['uuid'])])

    ITEM_DESCRIPTION = (
        textwrap.dedent(
            """
    Item specifications, with the format depending on the specified item_type.
        text:      (<text>|%%<directive>)
        bundle:    {0}
        worksheet: {1}"""
        )
        .format(BUNDLE_SPEC_FORMAT, WORKSHEET_SPEC_FORMAT)
        .strip()
    )

    @Commands.command(
        'add',
        help=[
            'Append text items, bundles, or subworksheets to a worksheet (possibly on a different instance).',
            'Bundles that do not yet exist on the destination instance will be copied over.',
        ],
        arguments=(
            Commands.Argument(
                'item_type',
                help='Type of item(s) to add {text, bundle, worksheet}.',
                choices=('text', 'bundle', 'worksheet'),
                metavar='item_type',
            ),
            Commands.Argument(
                'item_spec',
                help=ITEM_DESCRIPTION,
                nargs='+',
                completer=UnionCompleter(WorksheetsCompleter, BundlesCompleter),
            ),
            Commands.Argument(
                '--dest-worksheet',
                help='Worksheet to which to add items (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
                default='.',
            ),
            Commands.Argument(
                '-d',
                '--copy-dependencies',
                help='If adding bundles, also add dependencies of the bundles.',
                action='store_true',
            ),
        ),
    )
    def do_add_command(self, args):
        curr_client, curr_worksheet_uuid = self.manager.get_current_worksheet_uuid()
        dest_client, dest_worksheet_uuid = self.parse_client_worksheet_uuid(args.dest_worksheet)

        if args.item_type != 'bundle' and args.copy_dependencies:
            raise UsageError("-d/--copy_dependencies flag only applies when adding bundles.")

        if args.item_type == 'text':
            for item_spec in args.item_spec:
                if item_spec.startswith('%'):
                    dest_client.create(
                        'worksheet-items',
                        data={
                            'type': worksheet_util.TYPE_DIRECTIVE,
                            'worksheet': JsonApiRelationship('worksheets', dest_worksheet_uuid),
                            'value': item_spec[1:].strip(),
                        },
                        params={'uuid': dest_worksheet_uuid},
                    )
                else:
                    dest_client.create(
                        'worksheet-items',
                        data={
                            'type': worksheet_util.TYPE_MARKUP,
                            'worksheet': JsonApiRelationship('worksheets', dest_worksheet_uuid),
                            'value': item_spec,
                        },
                        params={'uuid': dest_worksheet_uuid},
                    )

        elif args.item_type == 'bundle':
            for bundle_spec in args.item_spec:
                (source_client, source_worksheet_uuid, source_target) = self.resolve_target(
                    curr_client, curr_worksheet_uuid, bundle_spec
                )
                # copy (or add only if bundle already exists on destination)
                self.copy_bundle(
                    source_client,
                    source_target.bundle_uuid,
                    dest_client,
                    dest_worksheet_uuid,
                    copy_dependencies=args.copy_dependencies,
                    add_to_worksheet=True,
                )

        elif args.item_type == 'worksheet':
            for worksheet_spec in args.item_spec:
                source_client, worksheet_spec = self.parse_spec(worksheet_spec)
                if source_client.address != dest_client.address:
                    raise UsageError("You cannot add worksheet links across instances.")

                # a base_worksheet_uuid is only applicable if we're on the source client
                base_worksheet_uuid = curr_worksheet_uuid if source_client is curr_client else None
                subworksheet_uuid = self.resolve_worksheet_uuid(
                    source_client, base_worksheet_uuid, worksheet_spec
                )

                # add worksheet
                dest_client.create(
                    'worksheet-items',
                    data={
                        'type': worksheet_util.TYPE_WORKSHEET,
                        'worksheet': JsonApiRelationship('worksheets', dest_worksheet_uuid),
                        'subworksheet': JsonApiRelationship('worksheets', subworksheet_uuid),
                    },
                    params={'uuid': dest_worksheet_uuid},
                )

    @Commands.command(
        'work',
        aliases=('w',),
        help=[
            'Set the current instance/worksheet.',
            '  work <worksheet>          : Switch to the given worksheet on the current instance.',
            '  work <alias>::            : Switch to the home worksheet on instance <alias>.',
            '  work <alias>::<worksheet> : Switch to the given worksheet on instance <alias>.',
        ],
        arguments=(
            Commands.Argument(
                '-u', '--uuid-only', help='Print only the worksheet uuid.', action='store_true'
            ),
            Commands.Argument(
                'worksheet_spec',
                help=WORKSHEET_SPEC_FORMAT,
                nargs='?',
                completer=UnionCompleter(AddressesCompleter, WorksheetsCompleter),
            ),
        ),
    )
    def do_work_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        worksheet_info = client.fetch('worksheets', worksheet_uuid)
        if args.worksheet_spec:
            if args.uuid_only:
                print(worksheet_info['uuid'], file=self.stdout)
            return self.change_current_worksheet(
                client, worksheet_uuid, verbose=(not args.uuid_only)
            )
        else:
            if worksheet_info:
                if args.uuid_only:
                    print(worksheet_info['uuid'], file=self.stdout)
                else:
                    print(
                        'Currently on worksheet: %s'
                        % (self.worksheet_url_and_name(worksheet_info)),
                        file=self.stdout,
                    )
            else:
                print(
                    'Not on any worksheet. Use `cl new` or `cl work` to switch to one.',
                    file=self.stdout,
                )

    def change_current_worksheet(self, client, worksheet_uuid, verbose=False):
        """
        :param client: REST client of the target worksheet
        :param worksheet_uuid: UUID of worksheet to change to, or None to indicate home worksheet
        :param verbose: print feedback to self.stdout if True
        :return: None, or a UI action to open the worksheet if self.headless
        """
        if worksheet_uuid is None:
            # Find home worksheet
            worksheet_uuid = self.resolve_worksheet_uuid(client, '', '/')

        if self.headless:
            return ui_actions.serialize([ui_actions.OpenWorksheet(worksheet_uuid)])

        self.manager.set_current_worksheet_uuid(client.address, worksheet_uuid)

        if verbose:
            worksheet_info = client.fetch('worksheets', worksheet_uuid)
            print(
                'Switched to worksheet: %s' % (self.worksheet_url_and_name(worksheet_info)),
                file=self.stdout,
            )

    @Commands.command(
        'wedit',
        aliases=('we',),
        help=[
            'Edit the contents of a worksheet.',
            'See https://codalab-worksheets.readthedocs.io/en/latest/User_Worksheet-Markdown for the markdown syntax.',
            '  wedit -n <name>          : Change the name of the worksheet.',
            '  wedit -T <tag> ... <tag> : Set the tags of the worksheet (e.g., paper).',
            '  wedit -o <username>      : Set the owner of the worksheet to <username>.',
        ],
        arguments=(
            Commands.Argument(
                'worksheet_spec',
                help=WORKSHEET_SPEC_FORMAT,
                nargs='?',
                completer=WorksheetsCompleter,
            ),
            Commands.Argument(
                '-n',
                '--name',
                help='Changes the name of the worksheet (%s).' % spec_util.NAME_REGEX.pattern,
            ),
            Commands.Argument('-t', '--title', help='Change title of worksheet.'),
            Commands.Argument(
                '-T', '--tags', help='Change tags (must appear after worksheet_spec).', nargs='*'
            ),
            Commands.Argument('-o', '--owner-spec', help='Change owner of worksheet.'),
            Commands.Argument(
                '--freeze',
                help='Freeze worksheet to prevent future modification.',
                action='store_true',
            ),
            Commands.Argument(
                '--unfreeze',
                help='Unfreeze worksheet to allow future modification.',
                action='store_true',
            ),
            Commands.Argument(
                '--anonymous',
                help='Set worksheet to be anonymous (identity of the owner will NOT be visible to users without \'all\' permission on the worksheet).',
                dest='anonymous',
                action='store_true',
                default=None,
            ),
            Commands.Argument(
                '--not-anonymous',
                help='Set worksheet to be NOT anonymous.',
                dest='anonymous',
                action='store_false',
            ),
            Commands.Argument(
                '-f',
                '--file',
                help='Replace the contents of the current worksheet with this file.',
                completer=require_not_headless(FilesCompleter(directories=False)),
            ),
        ),
    )
    def do_wedit_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        worksheet_info = client.fetch(
            'worksheets',
            worksheet_uuid,
            params={'include': ['items', 'items.bundle', 'items.subworksheet']},
        )
        if (
            args.freeze
            or args.unfreeze
            or any(
                arg is not None
                for arg in (args.name, args.title, args.tags, args.owner_spec, args.anonymous)
            )
        ):
            # Update the worksheet metadata.
            info = {'id': worksheet_info['id']}
            if args.name is not None:
                info['name'] = args.name
            if args.title is not None:
                info['title'] = args.title
            if args.tags is not None:
                info['tags'] = args.tags
            if args.owner_spec is not None:
                owner = client.fetch('users', args.owner_spec)
                info['owner'] = JsonApiRelationship('users', owner['id'])
            if args.freeze:
                info['frozen'] = datetime.datetime.utcnow().isoformat()
            if args.unfreeze:
                info['frozen'] = None
            if args.anonymous is not None:
                info['is_anonymous'] = args.anonymous

            client.update('worksheets', info)
            print(
                'Saved worksheet metadata for %s(%s).'
                % (worksheet_info['name'], worksheet_info['uuid']),
                file=self.stdout,
            )
        else:
            if self.headless:
                return ui_actions.serialize([ui_actions.SetEditMode(True)])

            # Either get a list of lines from the given file or request it from the user in an editor.
            if args.file:
                if args.file == '-':
                    lines = sys.stdin.readlines()
                else:
                    with codecs.open(args.file, encoding='utf-8', mode='r') as infile:
                        lines = infile.readlines()
                lines = [line.rstrip() for line in lines]
            else:
                worksheet_info['items'] = list(map(self.unpack_item, worksheet_info['items']))
                lines = worksheet_util.request_lines(worksheet_info)

            # Update worksheet
            client.update_worksheet_raw(worksheet_info['id'], lines)
            print(
                'Saved worksheet items for %s(%s).'
                % (worksheet_info['name'], worksheet_info['uuid']),
                file=self.stdout,
            )

    @staticmethod
    def unpack_item(item):
        """Unpack item serialized by WorksheetItemSchema, for our legacy interpretation code."""
        bundle_info = item['bundle']
        if bundle_info:
            bundle_info['uuid'] = bundle_info['id']
        subworksheet_info = item['subworksheet']
        if subworksheet_info:
            subworksheet_info['uuid'] = subworksheet_info['id']
        item_type = item['type']
        value = item['value']
        value_obj = (
            formatting.string_to_tokens(value)
            if item_type == worksheet_util.TYPE_DIRECTIVE
            else value
        )
        return bundle_info, subworksheet_info, value_obj, item_type

    @Commands.command(
        'print',
        aliases=('p',),
        help='Print the rendered contents of a worksheet.',
        arguments=(
            Commands.Argument(
                'worksheet_spec',
                help=WORKSHEET_SPEC_FORMAT,
                nargs='?',
                completer=WorksheetsCompleter,
            ),
            Commands.Argument(
                '-r', '--raw', action='store_true', help='Print out the raw contents (for editing).'
            ),
        ),
    )
    def do_print_command(self, args):
        self._fail_if_headless(args)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        worksheet_info = client.fetch(
            'worksheets',
            worksheet_uuid,
            params={
                'include': [
                    'owner',
                    'group_permissions',
                    'items',
                    'items.bundle',
                    'items.bundle.owner',
                    'items.subworksheet',
                ]
            },
        )
        worksheet_info['items'] = list(map(self.unpack_item, worksheet_info['items']))

        if args.raw:
            lines = worksheet_util.get_worksheet_lines(worksheet_info)
            for line in lines:
                print(line, file=self.stdout)
        else:
            print(self._worksheet_description(worksheet_info), file=self.stdout)
            interpreted_blocks = client.fetch_interpreted_worksheet(worksheet_uuid)['blocks']
            self.display_blocks(client, worksheet_info, interpreted_blocks)

    def display_blocks(self, client, worksheet_info, interpreted_blocks):
        for block in interpreted_blocks:
            mode = block['mode']
            print('', file=self.stdout)  # Separate interpreted items
            if mode == BlockModes.markup_block:
                print(block['text'], file=self.stdout)
            elif mode == BlockModes.contents_block:
                bundle_info = block['bundles_spec']['bundle_infos'][0]
                maxlines = block['max_lines']
                if maxlines:
                    maxlines = int(maxlines)
                try:
                    self.print_target_info(
                        client,
                        BundleTarget(bundle_info['uuid'], block['target_genpath']),
                        head=maxlines,
                    )
                except UsageError as e:
                    print('ERROR:', e, file=self.stdout)
            elif mode == BlockModes.record_block or mode == BlockModes.table_block:
                # header_name_posts is a list of (name, post-processing) pairs.
                header, rows = (block['header'], block['rows'])
                rows = client.interpret_genpath_table_contents(rows)
                # print >>self.stdout, the table
                self.print_table(
                    header, rows, show_header=(mode == BlockModes.table_block), indent='  '
                )
            elif mode == BlockModes.image_block:
                # Placeholder
                print('[Image]', file=self.stdout)
            elif mode == BlockModes.graph_block:
                # Placeholder
                print('[Graph]', file=self.stdout)
            elif mode == BlockModes.subworksheets_block:
                for worksheet_info in block['subworksheet_infos']:
                    print(
                        '[Worksheet ' + self.worksheet_url_and_name(worksheet_info) + ']',
                        file=self.stdout,
                    )
            elif mode == BlockModes.placeholder_block:
                print('[Placeholder]', block['directive'], file=self.stdout)
            elif mode == BlockModes.schema_block:
                print('[SchemaBlock]', file=self.stdout)
            else:
                raise UsageError('Invalid display mode: %s' % mode)

    @Commands.command(
        'wls',
        aliases=('wsearch', 'ws'),
        help=[
            'List worksheets on the current instance matching the given keywords (returns 10 results by default).',
            'Searcher\'s own worksheets are prioritized.',
            '  wls tag=paper           : List worksheets tagged as "paper".',
            '  wls group=<group_spec>  : List worksheets shared with the group identfied by group_spec.',
            '  wls .mine               : List my worksheets.',
            '  wls .notmine            : List the worksheets not owned by me.',
            '  wls .shared             : List worksheets that have been shared with any of the groups I am in.',
            '  wls .limit=10           : Limit the number of results to the top 10.',
        ],
        arguments=(
            Commands.Argument('keywords', help='Keywords to search for.', nargs='*'),
            Commands.Argument(
                '-a', '--address', help=ADDRESS_SPEC_FORMAT, completer=AddressesCompleter
            ),
            Commands.Argument('-u', '--uuid-only', help='Print only uuids.', action='store_true'),
        ),
    )
    def do_wls_command(self, args):
        if args.address:
            address = self.manager.apply_alias(args.address)
            client = self.manager.client(address)
        else:
            client = self.manager.current_client()

        worksheet_dicts = client.fetch(
            'worksheets',
            params={'keywords': args.keywords, 'include': ['owner', 'group_permissions']},
        )

        if args.uuid_only:
            for row in worksheet_dicts:
                print(row['uuid'], file=self.stdout)
        else:
            if worksheet_dicts:
                self.print_result_limit_info(len((worksheet_dicts)))
                for row in worksheet_dicts:
                    row['owner'] = self.simple_user_str(row['owner'])
                    row['permissions'] = group_permissions_str(row['group_permissions'])
                post_funcs = {'uuid': UUID_POST_FUNC}
                self.print_table(
                    ('uuid', 'name', 'owner', 'permissions'), worksheet_dicts, post_funcs
                )
            else:
                print(NO_RESULTS_FOUND, file=self.stderr)
        return {'refs': self.create_reference_map('worksheet', worksheet_dicts)}

    @Commands.command(
        'wrm',
        help=[
            'Delete a worksheet.',
            'To be safe, you can only delete a worksheet if it has no items and is not frozen.',
        ],
        arguments=(
            Commands.Argument(
                'worksheet_spec',
                help=WORKSHEET_SPEC_FORMAT,
                nargs='+',
                completer=WorksheetsCompleter,
            ),
            Commands.Argument(
                '--force',
                action='store_true',
                help='Delete worksheet even if it is non-empty and frozen.',
            ),
        ),
    )
    def do_wrm_command(self, args):
        delete_current = False
        client, current_worksheet = self.manager.get_current_worksheet_uuid()
        for worksheet_spec in args.worksheet_spec:
            client, worksheet_uuid = self.parse_client_worksheet_uuid(worksheet_spec)
            if (client, worksheet_uuid) == (client, current_worksheet):
                delete_current = True
            client.delete('worksheets', worksheet_uuid, params={'force': args.force})

        if delete_current:
            # Go to home worksheet
            return self.change_current_worksheet(client, None, verbose=True)

    @Commands.command(
        'wadd',
        help=[
            'Append all the items of the source worksheet to the destination worksheet.',
            'Bundles that do not yet exist on the destination service will be copied over.',
            'Bundles in non-terminal states (READY or FAILED) will not be copied over to destination worksheet.',
            'The existing items on the destination worksheet are not affected unless the -r/--replace flag is set.',
        ],
        arguments=(
            Commands.Argument(
                'source_worksheet_spec', help=WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter
            ),
            Commands.Argument(
                'dest_worksheet_spec', help=WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter
            ),
            Commands.Argument(
                '-r',
                '--replace',
                help='Replace everything on the destination worksheet with the items from the source worksheet, instead of appending (does not delete old bundles, just detaches).',
                action='store_true',
            ),
        ),
    )
    def do_wadd_command(self, args):
        # Source worksheet
        (source_client, source_worksheet_uuid) = self.parse_client_worksheet_uuid(
            args.source_worksheet_spec
        )
        source_items = source_client.fetch(
            'worksheets', source_worksheet_uuid, params={'include': ['items', 'items.bundle']}
        )['items']

        # Destination worksheet
        (dest_client, dest_worksheet_uuid) = self.parse_client_worksheet_uuid(
            args.dest_worksheet_spec
        )

        valid_source_items = []
        # Save all items to the destination worksheet
        for item in source_items:
            if item['type'] == worksheet_util.TYPE_BUNDLE:
                if item['bundle']['state'] not in [State.READY, State.FAILED]:
                    print(
                        'Skipping bundle {} because it has non-final state {}'.format(
                            item['bundle']['id'], item['bundle']['state']
                        ),
                        file=self.stdout,
                    )
                    continue
            item['worksheet'] = JsonApiRelationship('worksheets', dest_worksheet_uuid)
            valid_source_items.append(item)

        dest_client.create(
            'worksheet-items',
            valid_source_items,
            params={'replace': args.replace, 'uuid': dest_worksheet_uuid},
        )

        # Copy over the bundles
        for item in valid_source_items:
            if item['type'] == worksheet_util.TYPE_BUNDLE:
                self.copy_bundle(
                    source_client,
                    item['bundle']['id'],
                    dest_client,
                    dest_worksheet_uuid,
                    copy_dependencies=False,
                    add_to_worksheet=False,
                )

        print(
            'Copied %s worksheet items to %s.' % (len(valid_source_items), dest_worksheet_uuid),
            file=self.stdout,
        )

    @Commands.command(
        'wopen',
        aliases=('wo',),
        help=[
            'Open worksheet(s) in a local web browser.',
            '  wopen                   : Open the current worksheet in a local web browser.',
            '  wopen <worksheet_spec>  : Open worksheets identified by <worksheet_spec> in a local web browser.',
        ],
        arguments=(
            Commands.Argument(
                'worksheet_spec',
                help=WORKSHEET_SPEC_FORMAT,
                nargs='*',
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_wopen_command(self, args):
        worksheet_uuids = (
            [self.manager.get_current_worksheet_uuid()[1]]
            if not args.worksheet_spec
            else [
                self.parse_client_worksheet_uuid(worksheet_spec)[1]
                for worksheet_spec in args.worksheet_spec
            ]
        )

        for worksheet_uuid in worksheet_uuids:
            webbrowser.open(self.worksheet_url(worksheet_uuid))

        # Headless client should fire OpenWorksheet UI action
        if self.headless:
            return ui_actions.serialize(
                [ui_actions.OpenWorksheet(worksheet_uuid) for worksheet_uuid in worksheet_uuids]
            )

    #############################################################################
    # CLI methods for commands related to groups and permissions follow!
    #############################################################################

    @Commands.command(
        'gls',
        help='Show groups to which you belong.',
        arguments=(
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_gls_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        user_id = client.fetch('user')['id']
        groups = client.fetch('groups')

        if groups:
            for group in groups:
                group['uuid'] = group['id']
                if any(member['id'] == user_id for member in group['admins']):
                    group['role'] = 'admin'
                elif group['owner'] and group['owner']['id'] == user_id:
                    group['role'] = 'owner'
                else:
                    group['role'] = 'member'
                # Set owner string for print_table
                # group['owner'] may be None (i.e. for the public group)
                if group['owner']:
                    group['owner'] = self.simple_user_str(group['owner'])

            self.print_table(('name', 'uuid', 'owner', 'role'), groups)
        else:
            print('No groups found.', file=self.stdout)

    @Commands.command(
        'gnew',
        help='Create a new group.',
        arguments=(
            Commands.Argument(
                'name', help='Name of new group (%s).' % spec_util.NAME_REGEX.pattern
            ),
        ),
    )
    def do_gnew_command(self, args):
        client = self.manager.current_client()
        group = client.create('groups', {'name': args.name})
        print('Created new group %s(%s).' % (group['name'], group['id']), file=self.stdout)

    @Commands.command(
        'grm',
        help='Delete a group.',
        arguments=(
            Commands.Argument(
                'group_spec',
                help='Group to delete (%s).' % GROUP_SPEC_FORMAT,
                completer=GroupsCompleter,
            ),
        ),
    )
    def do_grm_command(self, args):
        client = self.manager.current_client()
        group = client.fetch('groups', args.group_spec)
        client.delete('groups', group['id'])
        print('Deleted group %s(%s).' % (group['name'], group['id']), file=self.stdout)

    @Commands.command(
        'ginfo',
        help='Show detailed information for a group.',
        arguments=(
            Commands.Argument(
                'group_spec',
                help='Group to show information about (%s).' % GROUP_SPEC_FORMAT,
                completer=GroupsCompleter,
            ),
        ),
    )
    def do_ginfo_command(self, args):
        client = self.manager.current_client()
        group = client.fetch('groups', args.group_spec)

        members = []
        # group['owner'] may be a falsey null-relationship (i.e. for the public group)
        if group['owner']:
            members.append(
                {
                    'role': 'owner',
                    'user': '%s(%s)' % (group['owner']['user_name'], group['owner']['id']),
                }
            )
        for member in group['admins']:
            members.append(
                {
                    'role': 'admin',
                    'user': '%s(%s)' % (member.get('user_name', '[deleted user]'), member['id']),
                }
            )
        for member in group['members']:
            members.append(
                {
                    'role': 'member',
                    'user': '%s(%s)' % (member.get('user_name', '[deleted user]'), member['id']),
                }
            )

        print('Members of group %s(%s):' % (group['name'], group['id']), file=self.stdout)
        self.print_table(('user', 'role'), members)

    @Commands.command(
        'uadd',
        help='Add a user to a group.',
        arguments=(
            Commands.Argument('user_spec', help='Username to add.'),
            Commands.Argument(
                'group_spec',
                help='Group to add user to (%s).' % GROUP_SPEC_FORMAT,
                completer=GroupsCompleter,
            ),
            Commands.Argument(
                '-a',
                '--admin',
                action='store_true',
                help='Give admin privileges to the user for the group.',
            ),
        ),
    )
    def do_uadd_command(self, args):
        client = self.manager.current_client()

        user = client.fetch('users', args.user_spec)
        group = client.fetch('groups', args.group_spec)
        client.create_relationship(
            'groups',
            group['id'],
            'admins' if args.admin else 'members',
            JsonApiRelationship('users', user['id']),
        )

        print(
            '%s in group %s as %s'
            % (user['user_name'], group['name'], 'admin' if args.admin else 'member'),
            file=self.stdout,
        )

    @Commands.command(
        'urm',
        help='Remove a user from a group.',
        arguments=(
            Commands.Argument('user_spec', help='Username to remove.'),
            Commands.Argument(
                'group_spec',
                help='Group to remove user from (%s).' % GROUP_SPEC_FORMAT,
                completer=GroupsCompleter,
            ),
        ),
    )
    def do_urm_command(self, args):
        client = self.manager.current_client()
        user = client.fetch('users', args.user_spec)
        group = client.fetch('groups', args.group_spec)

        # Get the first member that matches the target user ID
        member = next(
            [m for m in group['members'] + group['admins'] if m['id'] == user['id']], None
        )

        if member is None:
            print(
                '%s is not a member of group %s.' % (user['user_name'], group['name']),
                file=self.stdout,
            )
        else:
            client.delete_relationship(
                'groups', group['id'], 'members', JsonApiRelationship('users', user['id'])
            )
            print(
                'Removed %s from group %s.' % (user['user_name'], group['name']), file=self.stdout
            )

    @Commands.command(
        'perm',
        help='Set a group\'s permissions for a bundle.',
        arguments=(
            Commands.Argument(
                'bundle_spec', help=BUNDLE_SPEC_FORMAT, nargs='+', completer=BundlesCompleter
            ),
            Commands.Argument('group_spec', help=GROUP_SPEC_FORMAT, completer=GroupsCompleter),
            Commands.Argument(
                'permission_spec',
                help=PERMISSION_SPEC_FORMAT,
                completer=ChoicesCompleter(['none', 'read', 'all']),
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_perm_command(self, args):
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        group = client.fetch('groups', args.group_spec)

        bundle_uuids = self.target_specs_to_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        new_permission = parse_permission(args.permission_spec)

        client.create(
            'bundle-permissions',
            [
                {
                    'group': JsonApiRelationship('groups', group['id']),
                    'bundle': JsonApiRelationship('bundles', uuid),
                    'permission': new_permission,
                }
                for uuid in bundle_uuids
            ],
        )

        print(
            "Group %s(%s) has %s permission on %d bundles."
            % (group['name'], group['id'], permission_str(new_permission), len(bundle_uuids)),
            file=self.stdout,
        )

    @Commands.command(
        'wperm',
        help='Set a group\'s permissions for a worksheet.',
        arguments=(
            Commands.Argument(
                'worksheet_spec', help=WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter
            ),
            Commands.Argument('group_spec', help=GROUP_SPEC_FORMAT, completer=GroupsCompleter),
            Commands.Argument('permission_spec', help=PERMISSION_SPEC_FORMAT),
        ),
    )
    def do_wperm_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        worksheet = client.fetch('worksheets', worksheet_uuid)
        group = client.fetch('groups', args.group_spec)
        new_permission = parse_permission(args.permission_spec)

        client.create(
            'worksheet-permissions',
            {
                'group': JsonApiRelationship('groups', group['id']),
                'worksheet': JsonApiRelationship('worksheets', worksheet_uuid),
                'permission': new_permission,
            },
        )

        print(
            "Group %s has %s permission on worksheet %s."
            % (
                self.simple_group_str(group),
                permission_str(new_permission),
                self.worksheet_url_and_name(worksheet),
            ),
            file=self.stdout,
        )

    @Commands.command(
        'chown',
        help='Set the owner of bundles.',
        arguments=(
            Commands.Argument('user_spec', help='Username to set as the owner.'),
            Commands.Argument(
                'bundle_spec', help=BUNDLE_SPEC_FORMAT, nargs='+', completer=BundlesCompleter
            ),
            Commands.Argument(
                '-w',
                '--worksheet-spec',
                help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT,
                completer=WorksheetsCompleter,
            ),
        ),
    )
    def do_chown_command(self, args):
        """
        Change the owner of bundles.
        """
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        bundle_uuids = self.target_specs_to_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        owner_id = client.fetch('users', args.user_spec)['id']

        client.update(
            'bundles',
            [{'id': id_, 'owner': JsonApiRelationship('users', owner_id)} for id_ in bundle_uuids],
        )
        for uuid in bundle_uuids:
            print(uuid, file=self.stdout)

    #############################################################################
    # CLI methods for commands related to users follow!
    #############################################################################

    @Commands.command(
        'uedit',
        help=[
            'Edit user information.',
            'Note that password and email can only be changed through the web interface.',
        ],
        arguments=(
            Commands.Argument(
                'user_spec',
                nargs='?',
                help='Username or id of user to update [default: the authenticated user]',
            ),
            Commands.Argument('--first-name', help='First name'),
            Commands.Argument('--last-name', help='Last name'),
            Commands.Argument('--affiliation', help='Affiliation'),
            Commands.Argument('--url', help='Website URL'),
            Commands.Argument(
                '-t', '--time-quota', help='Total amount of time allowed (e.g., 3, 3m, 3h, 3d)'
            ),
            Commands.Argument(
                '-p',
                '--parallel-run-quota',
                type=int,
                help='Total amount of runs the user may have running at a time on shared public workers',
            ),
            Commands.Argument(
                '-d', '--disk-quota', help='Total amount of disk allowed (e.g., 3, 3k, 3m, 3g, 3t)'
            ),
            Commands.Argument(
                '--grant-access',
                action='store_true',
                help='Grant access to the user if the CodaLab instance is in protected mode',
            ),
            Commands.Argument(
                '--remove-access',
                action='store_true',
                help='Remove the user\'s access if the CodaLab instance is in protected mode',
            ),
        ),
    )
    def do_uedit_command(self, args):
        """
        Edit properties of users.
        """
        if args.grant_access and args.remove_access:
            raise UsageError('Can\'t both grant and remove access for a user.')
        client = self.manager.current_client()

        # Build user info
        user_info = {
            key: getattr(args, key)
            for key in ('first_name', 'last_name', 'affiliation', 'url')
            if getattr(args, key) is not None
        }
        if args.time_quota is not None:
            user_info['time_quota'] = formatting.parse_duration(args.time_quota)
        if args.parallel_run_quota is not None:
            user_info['parallel_run_quota'] = args.parallel_run_quota
        if args.disk_quota is not None:
            user_info['disk_quota'] = formatting.parse_size(args.disk_quota)
        if args.grant_access:
            user_info['has_access'] = True
        if args.remove_access:
            user_info['has_access'] = False
        if not user_info:
            raise UsageError("No fields to update.")

        # Send update request
        if args.user_spec is None:
            # If user id is not specified, update the authenticated user
            user = client.update_authenticated_user(user_info)
        else:
            # Resolve user id from user spec
            user_info['id'] = client.fetch('users', args.user_spec)['id']
            user = client.update('users', user_info)
        self.print_user_info(user)

    @Commands.command(
        'uls',
        help=[
            'Lists users on CodaLab (returns 10 results by default).',
            '  uls <keyword> ... <keyword>         : Username or id contains each <keyword>.',
            '  uls user_name=<value>               : Name is <value>, where `user_name` can be any metadata field (e.g., first_name).',
            '',
            '  uls .limit=<limit>                  : Limit the number of results to the top <limit> (e.g., 50).',
            '  uls .offset=<offset>                : Return results starting at <offset>.',
            '',
            '  uls .joined_before=<datetime>       : Returns users joined before (inclusive) given ISO 8601 timestamp (e.g., .before=2042-03-14).',
            '  uls .joined_after=<datetime>        : Returns users joined after (inclusive) given ISO 8601 timestamp (e.g., .after=2120-10-15T00:00:00-08).',
            '  uls .active_before=<datetime>       : (Root user only) Returns users last logged in before (inclusive) given ISO 8601 timestamp (e.g., .before=2042-03-14).',
            '  uls .active_after=<datetime>        : (Root user only) Returns users last logged in after (inclusive) given ISO 8601 timestamp (e.g., .after=2120-10-15T00:00:00-08).',
            '',
            '  uls .disk_used_less_than=<percentage> or <float>       : (Root user only) Returns users whose disk usage less than (inclusive) given value (e.g., .disk_used_less_than=70% or 0.3).',
            '  uls .disk_used_more_than=<percentage> or <float>       : (Root user only) Returns users whose disk usage less than (inclusive) given value (e.g., .disk_used_more_than=70% or 0.3).',
            '  uls .time_used_less_than=<<percentage> or <float>      : (Root user only) Returns users whose time usage less than (inclusive) given value (e.g., .time_used_less_than=70% or 0.3).',
            '  uls .time_used_more_than=<percentage> or <float>       : (Root user only) Returns users whose time usage less than (inclusive) given value (e.g., .time_used_more_than=70% or 0.3).',
            '',
            '  uls size=.sort                      : Sort by a particular field (where `size` can be any metadata field).',
            '  uls size=.sort-                     : Sort by a particular field in reverse (e.g., `size`).',
            '  uls .last                           : Sort in reverse chronological order (equivalent to id=.sort-).',
            '  uls .count                          : Count the number of matching bundles.',
            '  uls .format=<format>                : Apply <format> function (see worksheet markdown).',
        ],
        arguments=(
            Commands.Argument('keywords', help='Keywords to search for.', nargs='*'),
            Commands.Argument('-f', '--field', help='Print out these comma-separated fields.'),
        ),
    )
    def do_uls_command(self, args):
        """
        Search for specific users.
        If no argument is passed, we assume the user is searching for a keyword of an empty string.
        """
        client = self.manager.current_client()
        users = client.fetch('users', params={'keywords': args.keywords or ''})
        # Print direct numeric result
        if 'meta' in users:
            print(users['meta']['results'], file=self.stdout)
            return

        # Print table
        if len(users) > 0:
            if args.field:
                columns = args.field.split(',')
            else:
                columns = ('user_name', 'first_name', 'last_name', 'affiliation', 'date_joined')
            self.print_result_limit_info(len(users))
            self.uls_print_table(columns, users, user_defined=args.field)
        else:
            print(NO_RESULTS_FOUND, file=self.stderr)

    @Commands.command(
        'uinfo',
        help=['Show user information.'],
        arguments=(
            Commands.Argument(
                'user_spec',
                nargs='?',
                help='Username or id of user to show [default: the authenticated user]',
            ),
            Commands.Argument('-f', '--field', help='Print out these comma-separated fields.'),
        ),
    )
    def do_uinfo_command(self, args):
        """
        Edit properties of users.
        """
        client = self.manager.current_client()
        if args.user_spec is None:
            user = client.fetch('user')
        else:
            user = client.fetch('users', args.user_spec)
        self.print_user_info(user, args.field)

    def print_users_info(self, users):
        for user in users:
            self.print_user_info(user, fields=None)

    def print_user_info(self, user, fields=None):
        def print_attribute(key, user, should_pretty_print):
            # These fields will not be returned by the server if the
            # authenticated user is not root, so don't crash if you can't read them
            if key in (
                'last_login',
                'email',
                'time',
                'disk',
                'parallel_run_quota',
                'is_verified',
                'has_access',
            ):
                try:
                    if key == 'time':
                        value = formatting.ratio_str(
                            formatting.duration_str, user['time_used'], user['time_quota']
                        )
                    elif key == 'disk':
                        value = formatting.ratio_str(
                            formatting.size_str, user['disk_used'], user['disk_quota']
                        )
                    else:
                        value = user[key]
                except KeyError:
                    return
            else:
                value = user[key]

            if should_pretty_print:
                print('{:<15}: {}'.format(key, value), file=self.stdout)
            else:
                print(value, file=self.stdout)

        default_fields = (
            'id',
            'user_name',
            'is_verified',
            'has_access',
            'first_name',
            'last_name',
            'affiliation',
            'url',
            'date_joined',
            'last_login',
            'email',
            'time',
            'disk',
            'parallel_run_quota',
        )
        if fields:
            should_pretty_print = False
            fields = fields.split(',')
        else:
            should_pretty_print = True
            fields = default_fields

        for field in fields:
            print_attribute(field, user, should_pretty_print)

    @Commands.command(
        'ufarewell',
        help=[
            'Delete user permanently. Only root user can delete other users. Non-root user can delete his/her own account.',
            'To be safe, you can only delete a user if user does not own any bundles, worksheets, or groups.',
        ],
        arguments=(Commands.Argument('user_spec', help='Username or id of user to delete.'),),
    )
    def do_ufarewell_command(self, args):
        """
        Delete user.
        """
        client = self.manager.current_client()
        user = client.fetch('users', args.user_spec)

        client.delete('users', user['id'])
        print('Deleted user %s(%s).' % (user['user_name'], user['id']), file=self.stdout)

    #############################################################################
    # Local-only commands follow!
    #############################################################################

    @Commands.command(
        'events',
        help='Print the history of commands on this CodaLab instance (local only).',
        arguments=(
            Commands.Argument('-u', '--user', help='Filter by user id or username.'),
            Commands.Argument('-c', '--command', dest='match_command', help='Filter by command.'),
            Commands.Argument('-a', '--args', help='Filter by arguments.'),
            Commands.Argument('--uuid', help='Filter by bundle or worksheet uuid.'),
            Commands.Argument(
                '-o', '--offset', help='Offset in the result list.', type=int, default=0
            ),
            Commands.Argument(
                '-l', '--limit', help='Limit in the result list.', type=int, default=20
            ),
            Commands.Argument('-n', '--count', help='Just count.', action='store_true'),
            Commands.Argument('-g', '--group-by', help='Group by this field (e.g., date).'),
        ),
    )
    def do_events_command(self, args):
        self._fail_if_headless(args)
        self._fail_if_not_local(args)

        # Build query
        query_info = {
            'user': args.user,
            'command': args.match_command,
            'args': args.args,
            'uuid': args.uuid,
            'count': args.count,
            'group_by': args.group_by,
        }
        info = self.manager.model().get_events_log_info(query_info, args.offset, args.limit)
        if 'counts' in info:
            for row in info['counts']:
                print('\t'.join(map(str, list(row))), file=self.stdout)
        if 'events' in info:
            for event in info['events']:
                row = [
                    event.end_time.strftime('%Y-%m-%d %X') if event.end_time is not None else '',
                    '%.3f' % event.duration if event.duration is not None else '',
                    '%s(%s)' % (event.user_name, event.user_id),
                    event.command,
                    event.args,
                ]
                print('\t'.join(row), file=self.stdout)

    @Commands.command(
        'reset',
        help='Delete the CodaLab bundle store and reset the database (local only).',
        arguments=(
            Commands.Argument(
                '--commit', action='store_true', help='Reset is a no-op unless committed.'
            ),
        ),
    )
    def do_reset_command(self, args):
        """
        Delete everything - be careful!
        """
        self._fail_if_headless(args)
        self._fail_if_not_local(args)
        if not args.commit:
            raise UsageError('If you really want to delete EVERYTHING, use --commit')
        print('Deleting entire database...', file=self.stdout)
        self.manager.model()._reset()

    @Commands.command(
        'bs-add-partition',
        help='Add another partition for storage (MultiDiskBundleStore only)',
        arguments=(
            Commands.Argument(
                'name', help='The name you\'d like to give this partition for CodaLab.'
            ),
            Commands.Argument(
                'path',
                help=' '.join(
                    [
                        'The target location you would like to use for storing bundles.',
                        'This directory should be underneath a mountpoint for the partition',
                        'you would like to use. You are responsible for configuring the',
                        'mountpoint yourself.',
                    ]
                ),
            ),
        ),
    )
    def do_add_partition_command(self, args):
        """
        Add the specified target location as a new partition available for use by the filesystem.
        """
        self._fail_if_headless(args)
        self._fail_if_not_local(args)
        # This operation only allowed if we're using MultiDiskBundleStore
        if not isinstance(self.manager.bundle_store(), MultiDiskBundleStore):
            print(
                "This command can only be run when MultiDiskBundleStore is in use.", file=sys.stderr
            )
            sys.exit(1)
        self.manager.bundle_store().add_partition(args.path, args.name)

    @Commands.command(
        'bs-rm-partition',
        help='Remove a partition by its number (MultiDiskBundleStore only)',
        arguments=(Commands.Argument('partition', help='The partition you want to remove.'),),
    )
    def do_rm_partition_command(self, args):
        self._fail_if_headless(args)
        self._fail_if_not_local(args)
        if not isinstance(self.manager.bundle_store(), MultiDiskBundleStore):
            print(
                "This command can only be run when MultiDiskBundleStore is in use.", file=sys.stderr
            )
            sys.exit(1)
        self.manager.bundle_store().rm_partition(args.partition)

    @Commands.command(
        'bs-ls-partitions',
        help='List available partitions (MultiDiskBundleStore only)',
        arguments=(),
    )
    def do_ls_partitions_command(self, args):
        self._fail_if_headless(args)
        self._fail_if_not_local(args)
        if not isinstance(self.manager.bundle_store(), MultiDiskBundleStore):
            print(
                "This command can only be run when MultiDiskBundleStore is in use.", file=sys.stderr
            )
            sys.exit(1)
        self.manager.bundle_store().ls_partitions()

    @Commands.command(
        'bs-health-check',
        help='Perform a health check on the bundle store, garbage collecting bad files in the store. Performs a dry run by default, use -f to force removal.',
        arguments=(
            Commands.Argument(
                '-f',
                '--force',
                help='Perform all garbage collection and database updates instead of just printing what would happen',
                action='store_true',
            ),
            Commands.Argument(
                '-d',
                '--data-hash',
                help='Compute the digest for every bundle and compare against data_hash for consistency',
                action='store_true',
            ),
            Commands.Argument(
                '-r',
                '--repair',
                help='When used with --force and --data-hash, repairs incorrect data_hash in existing bundles',
                action='store_true',
            ),
        ),
    )
    def do_bs_health_check(self, args):
        self._fail_if_headless(args)
        self._fail_if_not_local(args)
        print('Performing Health Check...', file=sys.stderr)
        self.manager.bundle_store().health_check(
            self.manager.model(), args.force, args.data_hash, args.repair
        )

    def _fail_if_headless(self, args):
        if self.headless:
            raise UsageError(
                'You are only allowed to execute command "%s" from the CLI.' % args.command
            )

    def _fail_if_not_local(self, args):
        if 'localhost' not in self.manager.current_client().address:
            raise UsageError(
                'Sanity check! Point your CLI at an instance on localhost before executing admin commands: %s'
                % args.command
            )
