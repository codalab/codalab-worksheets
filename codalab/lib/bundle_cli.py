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
import copy
import datetime
import inspect
import itertools
import os
import shlex
import shutil
import sys
import time
import textwrap
from contextlib import closing
from StringIO import StringIO

import argcomplete
from argcomplete.completers import FilesCompleter, ChoicesCompleter

from codalab.bundles import (
    get_bundle_subclass,
    UPLOADED_TYPES,
)
from codalab.bundles.make_bundle import MakeBundle
from codalab.bundles.uploaded_bundle import UploadedBundle
from codalab.bundles.run_bundle import RunBundle
from codalab.common import (
    CODALAB_VERSION,
    NotFoundError,
    PermissionError,
    precondition,
    State,
    UsageError,
)
from codalab.lib import (
    bundle_util,
    cli_util,
    file_util,
    formatting,
    metadata_util,
    path_util,
    spec_util,
    ui_actions,
    worksheet_util,
    zip_util,
    bundle_fuse,
)
from codalab.lib.cli_util import nested_dict_get
from codalab.objects.permission import (
    group_permissions_str,
    parse_permission,
    permission_str,
)
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
from codalab.lib.bundle_store import (
    MultiDiskBundleStore
)
from codalab.lib.print_util import FileTransferProgress
from codalabworker.file_util import un_tar_directory

from codalab.lib.spec_util import generate_uuid
from codalabworker.docker_client import DockerClient
from codalabworker.file_util import remove_path

# Formatting Constants
GLOBAL_SPEC_FORMAT = "[<alias>::|<address>::](<uuid>|<name>)"
ADDRESS_SPEC_FORMAT = "(<alias>|<address>)"
TARGET_SPEC_FORMAT = '(<uuid>|<name>)[%s<subpath within bundle>]' % (os.sep,)
ALIASED_TARGET_SPEC_FORMAT = '[<key>:]' + TARGET_SPEC_FORMAT
BUNDLE_SPEC_FORMAT = '(<uuid>|<name>|^<index>)'
GLOBAL_BUNDLE_SPEC_FORMAT = '((<uuid>|<name>|^<index>)|(<alias>|<address>)::(<uuid>|<name>))'
WORKSHEET_SPEC_FORMAT = GLOBAL_SPEC_FORMAT
GROUP_SPEC_FORMAT = '(<uuid>|<name>|public)'
PERMISSION_SPEC_FORMAT = '((n)one|(r)ead|(a)ll)'
UUID_POST_FUNC = '[0:8]'  # Only keep first 8 characters

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
)

DOCKER_IMAGE_COMMANDS = (
    'edit-image',
    'commit-image',
    'push-image',
)

WORKSHEET_COMMANDS = (
    'new',
    'add',
    'wadd',
    'work',
    'print',
    'wedit',
    'wrm',
    'wls',
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

USER_COMMANDS = (
    'uinfo',
    'uedit',
)

SERVER_COMMANDS = (
    'server',
    'bundle-manager',
    'bs-add-partition',
    'bs-rm-partition',
    'bs-ls-partitions',
    'bs-health-check',
)

OTHER_COMMANDS = (
    'help',
    'status',
    'alias',
    'config',
    'logout',
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
    commands = {}

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
    def help_text(cls, verbose):
        def command_name(command):
            name = command
            aliases = cls.commands[command].aliases
            if aliases:
                name += ' (%s)' % ', '.join(list(aliases))
            return name

        available_other_commands = filter(
            lambda command: command in cls.commands, OTHER_COMMANDS)

        indent = 2
        max_length = max(
          len(command_name(command)) for command in itertools.chain(
              BUNDLE_COMMANDS,
              DOCKER_IMAGE_COMMANDS,
              WORKSHEET_COMMANDS,
              GROUP_AND_PERMISSION_COMMANDS,
              USER_COMMANDS,
              SERVER_COMMANDS,
              available_other_commands)
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
                return [(' ' * (indent * 2)) + 'Arguments:'] + \
                       [(' ' * (indent * 3) + '%-' + str(width) + 's  %s') % (row[0], row[1]) for row in table] + \
                       ['']
            if verbose:
                return '%s%s:\n%s\n%s' % (
                  ' ' * indent,
                  name,
                  '\n'.join((' ' * (indent * 2)) + line for line in command_obj.help),
                  '\n'.join(render_args(command_obj.arguments))
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

        return textwrap.dedent("""
        Usage: cl <command> <arguments>

        Commands for bundles:
        {bundle_commands}

        Commands for worksheets:
        {worksheet_commands}

        Commands for groups and permissions:
        {group_and_permission_commands}

        Commands for users:
        {user_commands}

        Commands for building Docker images:
        {docker_image_commands}

        Commands for managing server:
        {server_commands}

        Other commands:
        {other_commands}
        """).format(
            bundle_commands=command_group_help_text(BUNDLE_COMMANDS),
            docker_image_commands=command_group_help_text(DOCKER_IMAGE_COMMANDS),
            worksheet_commands=command_group_help_text(WORKSHEET_COMMANDS),
            group_and_permission_commands=command_group_help_text(GROUP_AND_PERMISSION_COMMANDS),
            user_commands=command_group_help_text(USER_COMMANDS),
            server_commands=command_group_help_text(SERVER_COMMANDS),
            other_commands=command_group_help_text(available_other_commands),
        ).strip()

    @classmethod
    def build_parser(cls, cli):
        """
        Builds an `ArgumentParser` for the cl program, with all the subcommands registered
        through the `Commands.command` decorator.
        """
        parser = CodaLabArgumentParser(prog='cl', cli=cli, add_help=False, formatter_class=argparse.RawTextHelpFormatter)
        parser.register('action', 'parsers', AliasedSubParsersAction)
        subparsers = parser.add_subparsers(dest='command', metavar='command')

        # Build subparser for each subcommand
        for command in cls.commands.itervalues():
            help = '\n'.join(command.help)
            subparser = subparsers.add_parser(command.name, cli=cli, help=help, description=help, aliases=command.aliases, add_help=True, formatter_class=argparse.RawTextHelpFormatter)

            # Register arguments for the subcommand
            for argument in command.arguments:
                argument_kwargs = argument.kwargs.copy()
                completer = argument_kwargs.pop('completer', None)
                argument = subparser.add_argument(*argument.args, **argument_kwargs)

                if completer is not None:
                    # If the completer is subclass of CodaLabCompleter, give it the BundleCLI instance
                    completer_class = completer if inspect.isclass(completer) else completer.__class__
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
        arguments = []
        added_keys = set()
        for bundle_subclass in bundle_subclasses:
            help_suffix = ' (for %ss)' % (bundle_subclass.BUNDLE_TYPE,) if bundle_subclass.BUNDLE_TYPE else ''

            for spec in bundle_subclass.get_user_defined_metadata():
                if spec.key not in added_keys:
                    added_keys.add(spec.key)

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
                    elif issubclass(spec.type, basestring):
                        kwargs['type'] = str
                        kwargs['metavar'] = spec.metavar
                    elif spec.type is bool:
                        kwargs['action'] = 'store_true'
                    else:
                        kwargs['type'] = spec.type
                        kwargs['metavar'] = spec.metavar
                    arguments.append(Commands.Argument(*args, **kwargs))

        return tuple(arguments)


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
        print >>self.stderr, message
        sys.exit(error_code)

    @staticmethod
    def simple_bundle_str(info):
        return '%s(%s)' % (contents_str(nested_dict_get(info, 'metadata', 'name')), info['uuid'])

    @staticmethod
    def simple_worksheet_str(info):
        return '%s(%s)' % (contents_str(info.get('name')), info['uuid'])

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

    @staticmethod
    def parse_target(client, worksheet_uuid, target_spec):
        """
        Helper: A target_spec is a bundle_spec[/subpath].
        """
        if os.sep in target_spec:
            bundle_spec, subpath = tuple(target_spec.split(os.sep, 1))
        else:
            bundle_spec, subpath = target_spec, ''
        # Resolve the bundle_spec to a particular bundle_uuid.
        bundle_uuid = BundleCLI.resolve_bundle_uuid(client, worksheet_uuid, bundle_spec)
        return (bundle_uuid, subpath)

    def parse_target_specs(self, items):
        targets = []
        for item in items:
            if ':' in item:
                (key, target) = item.split(':', 1)
                if key == '':
                    key = target  # Set default key to be same as target
            else:
                # Provide syntactic sugar for a make bundle with a single anonymous target.
                (key, target) = ('', item)

            targets.append((key, target))
        return targets

    def parse_key_targets(self, client, worksheet_uuid, items):
        """
        Helper: items is a list of strings which are [<key>]:<target>
        """
        targets = []
        # Turn targets into a dict mapping key -> (uuid, subpath)) tuples.

        for key, target in self.parse_target_specs(items):
            if key in targets:
                if key:
                    raise UsageError('Duplicate key: %s' % (key,))
                else:
                    raise UsageError('Must specify keys when packaging multiple targets!')
            targets.append((key, self.parse_target(client, worksheet_uuid, target)))
        return targets

    @staticmethod
    def resolve_bundle_uuid(client, worksheet_uuid, bundle_spec):
        # Minor optimization: return immediately if already a UUID
        if spec_util.UUID_REGEX.match(bundle_spec):
            return bundle_spec
        return BundleCLI.resolve_bundle_uuids(client, worksheet_uuid, [bundle_spec])[0]

    @staticmethod
    def resolve_bundle_uuids(client, worksheet_uuid, bundle_specs):
        bundles = client.fetch('bundles', params={
            'worksheet': worksheet_uuid,
            'specs': bundle_specs,
        })
        return [b['id'] for b in bundles]

    @staticmethod
    def resolve_worksheet_uuid(client, base_worksheet_uuid, worksheet_spec):
        """
        Avoid making REST call if worksheet_spec is already a uuid.
        """
        if spec_util.UUID_REGEX.match(worksheet_spec):
            worksheet_uuid = worksheet_spec  # Already uuid, don't need to look up specification
        else:
            worksheet_uuid = client.fetch_one('worksheets', params={
                'base': base_worksheet_uuid,
                'specs': [worksheet_spec],
            })['uuid']
        return worksheet_uuid

    def print_table(self, columns, row_dicts, post_funcs={}, justify={}, show_header=True, indent=''):
        """
        Pretty-print a list of columns from each row in the given list of dicts.
        """
        # Get the contents of the table
        rows = [columns]
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
                length = lengths[j]
                padding = (length - len(str(value))) * ' '
                if justify.get(columns[j], -1) < 0:
                    row_strs.append(str(value) + padding)
                else:
                    row_strs.append(padding + str(value))
            if show_header or i > 0:
                print >>self.stdout, indent + '  '.join(row_strs)
            if i == 0:
                print >>self.stdout, indent + (sum(lengths) + 2*(len(columns) - 1)) * '-'

    def parse_spec(self, spec):
        """
        Parse a global spec, which includes the instance and either a bundle or worksheet spec.
        Example: https://worksheets.codalab.org/bundleservice::wine
        Return (client, spec)
        """
        tokens = spec.split('::')
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
            client, spec = self.parse_spec(spec)
            # If we're on the same client, then resolve spec with respect to
            # the current worksheet.
            if client_is_explicit:
                base_worksheet_uuid = None
            else:
                _, base_worksheet_uuid = self.manager.get_current_worksheet_uuid()
            worksheet_uuid = self.resolve_worksheet_uuid(client, base_worksheet_uuid, spec)
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

    #############################################################################
    # CLI methods
    #############################################################################

    EDIT_ARGUMENTS = (
        Commands.Argument('-e', '--edit', action='store_true', help='Show an editor to allow editing of the bundle metadata.'),
    )

    # After running a bundle, we can wait for it, possibly observing it's output.
    # These functions are shared across run and mimic.
    WAIT_ARGUMENTS = (
        Commands.Argument('-W', '--wait', action='store_true', help='Wait until run finishes.'),
        Commands.Argument('-t', '--tail', action='store_true', help='Wait until run finishes, displaying stdout/stderr.'),
        Commands.Argument('-v', '--verbose', action='store_true', help='Display verbose output.'),
    )

    MIMIC_ARGUMENTS = (
        Commands.Argument('-n', '--name', help='Name of the output bundle.'),
        Commands.Argument('-d', '--depth', type=int, default=10, help='Number of parents to look back from the old output in search of the old input.'),
        Commands.Argument('-s', '--shadow', action='store_true', help='Add the newly created bundles right after the old bundles that are being mimicked.'),
        Commands.Argument('-i', '--dry-run', help='Perform a dry run (just show what will be done without doing it)', action='store_true'),
        Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
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
            argv = argv[0:i] + [' '.join(argv[i+1:])]  # TODO: quote command properly
        except:
            pass

        return argv

    def complete_command(self, command):
        """
        Given a command string, return a list of suggestions to complete the last token.
        """
        parser = Commands.build_parser(self)
        cf = argcomplete.CompletionFinder(parser)
        cword_prequote, cword_prefix, _, comp_words, first_colon_pos = argcomplete.split_line(command, len(command))

        # Strip whitespace and parse according to shell escaping rules
        try:
            clean = lambda s: shlex.split(s.strip())[0] if s else ''
        except ValueError as e:
            raise UsageError(e.message)
        return map(clean, cf._get_completions(comp_words, cword_prefix, cword_prequote, first_colon_pos))

    def do_command(self, argv, stdout=None, stderr=None):
        parser = Commands.build_parser(self)

        # Call autocompleter (no side effect if os.environ['_ARGCOMPLETE'] is not set)
        argcomplete.autocomplete(parser)

        # Parse arguments
        argv = self.collapse_bare_command(argv)
        args = parser.parse_args(argv)

        # Bind self (BundleCLI instance) and args to command function
        command_fn = lambda: args.function(self, args)

        if self.verbose >= 2:
            structured_result = command_fn()
        else:
            try:
                # Profiling (off by default)
                if False:
                    import hotshot
                    import hotshot.stats
                    prof_path = 'codalab.prof'
                    prof = hotshot.Profile(prof_path)
                    prof.runcall(command_fn)
                    prof.close()
                    stats = hotshot.stats.load(prof_path)
                    stats.sort_stats('time', 'calls')
                    stats.print_stats(20)
                else:
                    structured_result = command_fn()
            except PermissionError, e:
                if self.headless:
                    raise e
                self.exit(e.message)
            except UsageError, e:
                if self.headless:
                    raise e
                self.exit('%s: %s' % (e.__class__.__name__, e))
        return structured_result

    @Commands.command(
        'help',
        help=[
            'Show usage information for commands.',
            '  help           : Show brief description for all commands.',
            '  help -v        : Show full usage information for all commands.',
            '  help <command> : Show full usage information for <command>.',
        ],
        arguments=(
            Commands.Argument('command', help='name of command to look up', nargs='?'),
            Commands.Argument('-v', '--verbose', action='store_true', help='Display all options of all commands.'),
        ),
    )
    def do_help_command(self, args):
        print >>self.stdout, 'CodaLab CLI version %s' % CODALAB_VERSION
        if args.command:
            self.do_command([args.command, '--help'])
            return
        print >>self.stdout, Commands.help_text(args.verbose)

    @Commands.command(
        'status',
        aliases=('st',),
        help='Show current client status.'
    )
    def do_status_command(self, args):
        if not self.headless:
            print >>self.stdout, "codalab_home: %s" % self.manager.codalab_home
            print >>self.stdout, "session: %s" % self.manager.session_name()
            address = self.manager.session()['address']
            print >>self.stdout, "address: %s" % address
            state = self.manager.state['auth'].get(address, {})
            if 'username' in state:
                print >>self.stdout, "username: %s" % state['username']

        client, worksheet_uuid = self.manager.get_current_worksheet_uuid()
        worksheet_info = client.fetch('worksheets', worksheet_uuid)
        print >>self.stdout, "current_worksheet: %s" % self.simple_worksheet_str(worksheet_info)
        print >>self.stdout, "user: %s" % self.simple_user_str(client.fetch('user'))

    @Commands.command(
        'logout',
        help='Logout of the current session, or a specific instance.',
        arguments=(
            Commands.Argument('alias', help='Alias or URL of instance from which to logout. Default is the current session.', nargs='?'),
        )
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
            Commands.Argument('instance', help='Instance to bind the alias to (e.g., https://codalab.org/bundleservice).', nargs='?'),
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
                print >>self.stdout, args.name + ': ' + formatting.verbose_contents_str(instance)
        else:
            for name, instance in aliases.items():
                print >>self.stdout, name + ': ' + instance

    @Commands.command(
        'config',
        help=[
            'Set CodaLab configuration.',
            '  config <key>         : Shows the value of <key>.',
            '  config <key> <value> : Sets <key> to <value>.',
        ],
        arguments=(
            Commands.Argument('key', help='key to set (e.g., cli/verbose).'),
            Commands.Argument('value', help='Instance to bind the alias to (e.g., https://codalab.org/bundleservice).', nargs='?'),
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
            except:
                pass
            try:
                return float(value)
            except:
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
            print config[key]

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
            Commands.Argument('path', help='Paths (or URLs) of the files/directories to upload.', nargs='*', completer=require_not_headless(FilesCompleter())),
            Commands.Argument('-c', '--contents', help='Specify the string contents of the bundle.'),
            Commands.Argument('-L', '--follow-symlinks', help='Always dereference (follow) symlinks.', action='store_true'),
            Commands.Argument('-x', '--exclude-patterns', help='Exclude these file patterns.', nargs='*'),
            Commands.Argument('-g', '--git', help='Path is a git repository, git clone it.', action='store_true'),
            Commands.Argument('-p', '--pack', help='If path is an archive file (e.g., zip, tar.gz), keep it packed.', action='store_true', default=False),
            Commands.Argument('-z', '--force-compression', help='Always use compression (this may speed up single-file uploads over a slow network).', action='store_true', default=False),
            Commands.Argument('-w', '--worksheet-spec', help='Upload to this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ) + Commands.metadata_arguments([UploadedBundle] + [get_bundle_subclass(bundle_type) for bundle_type in UPLOADED_TYPES])
        + EDIT_ARGUMENTS,
    )
    def do_upload_command(self, args):
        # Uploading from local filesystem not allowed for headless CLI (i.e. web terminal)
        if self.headless and args.path:
            raise UsageError("Upload from local filesystem not supported in headless CLI.")

        if args.contents is None and not args.path:
            raise UsageError("Nothing to upload.")

        if args.contents is not None and args.path:
            raise UsageError("Upload does not support mixing content strings and local files.")

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        # Build bundle info
        metadata = self.get_missing_metadata(UploadedBundle, args, initial_metadata={})
        # name = 'test.zip' => name = 'test'
        if args.contents is not None:
            metadata['name'] = 'contents'
        if not args.pack and zip_util.path_is_archive(metadata['name']):
            metadata['name'] = zip_util.strip_archive_ext(metadata['name'])
        bundle_info = {
            'bundle_type': 'dataset',  # TODO: deprecate Dataset and ProgramBundles
            'metadata': metadata,
        }

        # Option 1: Upload contents string
        if args.contents is not None:
            contents_buffer = StringIO(args.contents)
            new_bundle = client.create('bundles', bundle_info, params={
                'worksheet': worksheet_uuid
            })
            client.upload_contents_blob(
                new_bundle['id'],
                fileobj=contents_buffer,
                params={'filename': 'contents', 'unpack': False})

        # Option 2: Upload URL(s)
        elif any(map(path_util.path_is_url, args.path)):
            if self.headless:
                raise UsageError("Local file paths not allowed without a filesystem.")
            if not all(map(path_util.path_is_url, args.path)):
                raise UsageError("URLs and local files cannot be uploaded in the same bundle.")

            new_bundle = client.create('bundles', bundle_info, params={
                'worksheet': worksheet_uuid
            })
            client.upload_contents_blob(new_bundle['id'], params={
                'urls': args.path,
                'git': args.git,
            })

        # Option 3: Upload file(s) from the local filesystem
        else:
            # Check that the upload paths exist
            for path in args.path:
                path_util.check_isvalid(path_util.normalize(path), 'upload')

            # Canonicalize paths (e.g., removing trailing /)
            sources = [path_util.normalize(path) for path in args.path]

            print >>self.stderr, "Preparing upload archive..."
            packed = zip_util.pack_files_for_upload(
                    sources, should_unpack=(not args.pack),
                    follow_symlinks=args.follow_symlinks,
                    exclude_patterns=args.exclude_patterns,
                    force_compression=args.force_compression)

            # Create bundle.
            # We must create the bundle right before we upload it because we
            # perform some input validation in functions such as
            # zip_util.pack_files_for_upload that we want to fail fast before
            # we try to create or upload the bundle, otherwise you will be left
            # with empty shells of failed uploading bundles on your worksheet.
            new_bundle = client.create('bundles', bundle_info, params={
                'worksheet': worksheet_uuid,
                'wait_for_upload': True,
            })
            print >>self.stderr, 'Uploading %s (%s) to %s' %\
                                 (packed['filename'], new_bundle['id'], client.address)
            progress = FileTransferProgress('Sent ', packed['filesize'], f=self.stderr)
            with closing(packed['fileobj']), progress:
                client.upload_contents_blob(
                    new_bundle['id'],
                    fileobj=packed['fileobj'],
                    params={
                        'filename': packed['filename'],
                        'unpack': packed['should_unpack'],
                        'simplify': packed['should_simplify'],
                    },
                    progress_callback=progress.update)

        print >>self.stdout, new_bundle['id']

    @Commands.command(
        'download',
        aliases=('down',),
        help='Download bundle from a CodaLab instance.',
        arguments=(
            Commands.Argument('target_spec', help=TARGET_SPEC_FORMAT, completer=BundlesCompleter),
            Commands.Argument('-o', '--output-path', help='Path to download bundle to.  By default, the bundle or subpath name in the current directory is used.'),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ),
    )
    def do_download_command(self, args):
        self._fail_if_headless(args)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        target = self.parse_target(client, worksheet_uuid, args.target_spec)
        bundle_uuid, subpath = target

        # Figure out where to download.
        info = client.fetch('bundles', bundle_uuid)
        if args.output_path:
            local_path = args.output_path
        else:
            local_path = nested_dict_get(info, 'metadata', 'name', default='untitled') if subpath == '' else os.path.basename(subpath)
        final_path = os.path.join(os.getcwd(), local_path)
        if os.path.exists(final_path):
            print >>self.stdout, 'Local file/directory \'%s\' already exists.' % local_path
            return

        # Do the download.
        target_info = client.fetch_contents_info(target[0], target[1], 0)
        if target_info['type'] == 'link':
            raise UsageError('Downloading symlinks is not allowed.')

        print >>self.stdout, 'Downloading %s/%s => %s' % (self.simple_bundle_str(info), subpath, final_path)

        progress = FileTransferProgress('Received ', f=self.stderr)
        contents = file_util.tracked(
            client.fetch_contents_blob(target[0], target[1]), progress.update)
        with progress, closing(contents):
            if target_info['type'] == 'directory':
                un_tar_directory(contents, final_path, 'gz')
            elif target_info['type'] == 'file':
                with open(final_path, 'wb') as out:
                    shutil.copyfileobj(contents, out)

    def copy_bundle(self, source_client, source_bundle_uuid, dest_client, dest_worksheet_uuid, copy_dependencies, add_to_worksheet):
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
                self.copy_bundle(source_client, dep['parent_uuid'], dest_client, dest_worksheet_uuid, False, add_to_worksheet)
            self.copy_bundle(source_client, source_bundle_uuid, dest_client, dest_worksheet_uuid, False, add_to_worksheet)
            return

        # Check if the bundle already exists on the destination, then don't copy it
        # (although metadata could be different on source and destination).
        # TODO: sync the metadata.
        try:
            dest_client.fetch('bundles', source_bundle_uuid)
        except NotFoundError as e:
            bundle_exists = False
        else:
            bundle_exists = True

        # Bundle already exists, just need to add to worksheet if desired.
        if bundle_exists:
            if add_to_worksheet:
                dest_client.create('worksheet-items', data={
                    'type': worksheet_util.TYPE_BUNDLE,
                    'worksheet': JsonApiRelationship('worksheets', dest_worksheet_uuid),
                    'bundle': JsonApiRelationship('bundles', source_bundle_uuid)
                })
            return

        source_info = source_client.fetch('bundles', source_bundle_uuid)
        if source_info is None:
            print >>self.stdout, 'Unable to read bundle %s' % source_bundle_uuid
            return

        source_desc = self.simple_bundle_str(source_info)
        if source_info['state'] not in [State.READY, State.FAILED]:
            print >>self.stdout, 'Not copying %s because it has non-final state %s' % (source_desc, source_info['state'])
            return

        print >>self.stdout, "Copying %s..." % source_desc

        # Create the bundle, copying over metadata from the source bundle
        dest_bundle = dest_client.create('bundles', source_info, params={
            'worksheet': dest_worksheet_uuid,
            'detached': not add_to_worksheet,
            'wait_for_upload': True,
        })

        # If bundle contents don't exist, finish after just copying metadata
        try:
            target_info = source_client.fetch_contents_info(source_bundle_uuid)
        except NotFoundError:
            return

        # Collect information about how server should unpack
        filename = nested_dict_get(source_info, 'metadata', 'name')
        if target_info['type'] == 'directory':
            filename += '.tar.gz'
            unpack = True
        else:
            unpack = False

        # Bundles stuck in non-final states such as 'running' should not keep
        # that state at the destination server, and should instead just fallback
        # to 'failed'
        if source_info['state'] == State.READY:
            source_state = State.READY
        else:
            source_state = State.FAILED

        # Send file over
        progress = FileTransferProgress('Copied ', f=self.stderr)
        source = source_client.fetch_contents_blob(source_bundle_uuid)
        with closing(source), progress:
            dest_client.upload_contents_blob(
                dest_bundle['id'],
                fileobj=source,
                params={
                    'filename': filename,
                    'unpack': unpack,
                    'simplify': False,  # retain original bundle verbatim
                    'state_on_success': source_state,  # copy bundle state
                },
                progress_callback=progress.update)

    @Commands.command(
        'make',
        help=['Create a bundle by combining parts of existing bundles.',
            '  make <bundle>/<subpath>                : New bundle\'s contents are copied from <subpath> in <bundle>.',
            '  make <key>:<bundle> ... <key>:<bundle> : New bundle contains file/directories <key> ... <key>, whose contents are given.',
        ],
        arguments=(
            Commands.Argument('target_spec', help=ALIASED_TARGET_SPEC_FORMAT, nargs='+', completer=BundlesCompleter),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ) + Commands.metadata_arguments([MakeBundle]) + EDIT_ARGUMENTS,
    )
    def do_make_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        targets = self.parse_key_targets(client, worksheet_uuid, args.target_spec)
        metadata = self.get_missing_metadata(MakeBundle, args)
        new_bundle = client.create(
            'bundles',
            self.derive_bundle(MakeBundle.BUNDLE_TYPE, None, targets, metadata),
            params={'worksheet': worksheet_uuid},
        )

        print >>self.stdout, new_bundle['uuid']

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
        for (child_path, (parent_uuid, parent_path)) in targets:
            dependencies.append({
                'child_path': child_path,
                'parent_uuid': parent_uuid,
                'parent_path': parent_path,
            })
        return {
            'bundle_type': bundle_type,
            'command': command,
            'metadata': metadata,
            'dependencies': dependencies,
        }

    @Commands.command(
        'run',
        help='Create a bundle by running a program bundle on an input bundle. If local mode is specified, simulate a run bundle locally, producing bundle contents in the local environment and mounting local dependencies.',
        arguments=(
            Commands.Argument('target_spec', help=ALIASED_TARGET_SPEC_FORMAT, nargs='*', completer=TargetsCompleter),
            Commands.Argument('command', metavar='[---] command', help='Arbitrary Linux command to execute.', completer=NullCompleter),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
            Commands.Argument('--local', action='store_true', help='Beta feature: this command may change in a future release. Simulate a run bundle locally. This means any dependencies provided are local files/directories mounted to a temporary container (read-only).'),
        ) + Commands.metadata_arguments([RunBundle]) + EDIT_ARGUMENTS + WAIT_ARGUMENTS,
    )
    def do_run_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        args.target_spec, args.command = cli_util.desugar_command(args.target_spec, args.command)
        metadata = self.get_missing_metadata(RunBundle, args)

        if args.local:
            self._fail_if_headless(args)  # Disable on headless systems
            docker_image = metadata.get('request_docker_image', None)
            if not docker_image:
                raise UsageError('--request-docker-image [docker-image] must be specified when running in local mode')

            uuid = generate_uuid()
            bundle_path = os.path.join(self.manager.codalab_home, 'local_bundles', uuid)
            command = args.command
            request_network = None
            dependencies = [
                (u'{}'.format(key), u'/{}_dependencies/{}'.format(uuid, key)) for key, target in self.parse_target_specs(args.target_spec)
            ]

            # Set up a directory to store the bundle.
            remove_path(bundle_path)
            os.makedirs(bundle_path)

            for dependency_path, docker_dependency_path in dependencies:
                child_path = os.path.join(bundle_path, dependency_path)
                os.symlink(docker_dependency_path, child_path)

            dc = DockerClient()
            container_id = dc.start_container(bundle_path, uuid, command, docker_image, request_network, dependencies)
            print >>self.stdout, '===='
            print >>self.stdout, 'ContainerID: ', container_id
            print >>self.stdout, 'Local Bundle ID: ', uuid
            print >>self.stdout, 'You can find local bundle contents in: ', bundle_path
            print >>self.stdout, '===='
        else:
            targets = self.parse_key_targets(client, worksheet_uuid, args.target_spec)
            new_bundle = client.create(
                'bundles',
                self.derive_bundle(RunBundle.BUNDLE_TYPE, args.command, targets, metadata),
                params={'worksheet': worksheet_uuid},
            )

            print >>self.stdout, new_bundle['uuid']
            self.wait(client, args, new_bundle['uuid'])

    @Commands.command(
        'edit-image',
        help='Beta feature: this command may change in a future release. Start an interactive shell with an image to allow edits to that image locally. This means any dependencies provided are also local files/directories mounted to a temporary container (read-only).',
        arguments=(
            Commands.Argument('target_spec', help=ALIASED_TARGET_SPEC_FORMAT, nargs='*', completer=TargetsCompleter),
            Commands.Argument('--request-docker-image', help='The docker image to edit', required=True),
        )
    )
    def do_edit_image_command(self, args):
        self._fail_if_headless(args)  # Disable on headless systems
        docker_image = args.request_docker_image

        uuid = generate_uuid()
        bundle_path = os.path.join(self.manager.codalab_home, 'local_bundles', uuid)
        command = '/bin/bash'
        request_network = None
        dependencies = [
            (u'{}'.format(key), u'/{}_dependencies/{}'.format(uuid, key)) for key, target in self.parse_target_specs(args.target_spec)
        ]

        # Set up a directory to store the bundle.
        remove_path(bundle_path)
        os.makedirs(bundle_path)

        for dependency_path, docker_dependency_path in dependencies:
            child_path = os.path.join(bundle_path, dependency_path)
            os.symlink(docker_dependency_path, child_path)

        dc = DockerClient()
        container_id = dc.create_container(bundle_path, uuid, command, docker_image, request_network, dependencies, ['-it'])

        print >>self.stdout, '===='
        print >>self.stdout, 'Entering container {}'.format(container_id[:8])
        print >>self.stdout, 'Once you are happy with the changes, please exit the container (ctrl-D)'
        print >>self.stdout, 'and commit your changes to a new image by running:'
        print >>self.stdout, ''
        print >>self.stdout, '\tcl commit-image {} [image-tag]'.format(container_id[:8])
        print >>self.stdout, ''
        print >>self.stdout, '===='
        os.system('docker start -ai {}'.format(container_id))
        print >>self.stdout, '===='
        print >>self.stdout, 'Exited from container {}'.format(container_id[:8])
        print >>self.stdout, 'If you are happy with the changes, please commit your changes to a new'
        print >>self.stdout, 'image by running:'
        print >>self.stdout, ''
        print >>self.stdout, '\tcl commit-image {} [image-tag]'.format(container_id[:8])
        print >>self.stdout, ''
        print >>self.stdout, '===='

    @Commands.command(
        'commit-image',
        help='Create an image from a container.',
        arguments=(
            Commands.Argument('container', help='Container to commit.'),
            Commands.Argument('image_tag', help='Image tag to commit to. E.g: codalabtest-on.azurecr.io/ubuntu'),
        )
    )
    def do_commit_image_command(self, args):
        self._fail_if_headless(args)  # Disable on headless systems
        cli_command = 'docker commit {} {}'.format(args.container, args.image_tag)
        os.system(cli_command)

    @Commands.command(
        'push-image',
        help='Beta feature: this command may change in a future release. Push a (committed) image to a docker registry. Deprecated and disabled. Please use docker push instead.',
        arguments=(
            Commands.Argument('image_tag', help='Image tag for which to perform a push. E.g: codalabtest-on.azurecr.io/ubuntu'),
        )
    )
    def do_push_image_command(self, args):
        self._fail_if_headless(args)  # Disable on headless systems
        print >>self.stdout, '===='
        print >>self.stdout, 'cl push-image has been deprecated and disabled. Please use docker push instead:'
        print >>self.stdout, ''
        print >>self.stdout, '\tdocker push [image-tag]'
        print >>self.stdout, ''
        print >>self.stdout, '===='


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
            Commands.Argument('-n', '--name', help='Change the bundle name (format: %s).' % spec_util.NAME_REGEX.pattern),
            Commands.Argument('-T', '--tags', help='Change tags (must appear after worksheet_spec).', nargs='*'),
            Commands.Argument('-d', '--description', help='New bundle description.'),
            Commands.Argument('--anonymous', help='Set bundle to be anonymous (identity of the owner will NOT \n'
                              'be visible to users without \'all\' permission on the bundle).',
                              dest='anonymous', action='store_true', default=None),
            Commands.Argument('--not-anonymous', help='Set bundle to be NOT anonymous.', dest='anonymous', action='store_false'),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ),
    )
    def do_edit_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        info = client.fetch_one('bundles', params={
            'specs': args.bundle_spec,
            'worksheet': worksheet_uuid,
        })

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

        # Prompt user for edits via an editor when no edits provided by command line options
        if not self.headless and not metadata_update and not bundle_update:
            metadata_update = metadata_util.request_missing_metadata(bundle_subclass, info['metadata'])

        if bundle_update or metadata_update:
            bundle_update.update({
                'id': info['id'],
                'bundle_type': info['bundle_type'],
            })
            if metadata_update:
                bundle_update['metadata'] = metadata_update

            client.update('bundles', bundle_update)
            print >>self.stdout, "Saved metadata for bundle %s." % (info['id'])

    @Commands.command(
        'detach',
        aliases=('de',),
        help='Detach a bundle from this worksheet, but doesn\'t remove the bundle.',
        arguments=(
            Commands.Argument('bundle_spec', help=BUNDLE_SPEC_FORMAT, nargs='+', completer=BundlesCompleter),
            Commands.Argument('-n', '--index', help='Specifies which occurrence (1, 2, ...) of the bundle to detach, counting from the end.', type=int),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
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
        bundle_uuids = self.resolve_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        worksheet_info = client.fetch('worksheets', worksheet_uuid, params={'include': ['items', 'items.bundle']})

        # Number the bundles: c c a b c => 3 2 1 1 1
        items = worksheet_info['items']
        indices = [None] * len(items)  # Parallel array to items that stores the index associated with that bundle uuid
        uuid2index = {}  # bundle uuid => index of the bundle (at the end, number of times it occurs on the worksheet)
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
                            raise UsageError('bundle %s shows up more than once, need to specify index' % uuid)
                        detach = True
                    else:
                        if args.index > uuid2index[uuid]:
                            raise UsageError('bundle %s shows up %d times, can\'t get index %d' % (uuid, uuid2index[uuid], args.index))
                        if args.index == indices[i]:
                            detach = True
            if not detach:
                new_items.append(item)

        client.create('worksheet-items', data=new_items, params={'replace': True})

    @Commands.command(
        'rm',
        help='Remove a bundle (permanent!).',
        arguments=(
            Commands.Argument('bundle_spec', help=BUNDLE_SPEC_FORMAT, nargs='+', completer=BundlesCompleter),
            Commands.Argument('--force', action='store_true', help='Delete bundle (DANGEROUS - breaking dependencies!)'),
            Commands.Argument('-r', '--recursive', action='store_true', help='Delete all bundles downstream that depend on this bundle (DANGEROUS - could be a lot!).'),
            Commands.Argument('-d', '--data-only', action='store_true', help='Keep the bundle metadata, but remove the bundle contents on disk.'),
            Commands.Argument('-i', '--dry-run', help='Perform a dry run (just show what will be done without doing it).', action='store_true'),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ),
    )
    def do_rm_command(self, args):
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        # Resolve all the bundles first, then delete.
        # This is important since some of the bundle specs (^1 ^2) are relative.
        bundle_uuids = self.resolve_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        deleted_uuids = client.delete('bundles', bundle_uuids, params={
            'force': args.force,
            'recursive': args.recursive,
            'data-only': args.data_only,
            'dry-run': args.dry_run
        })['meta']['ids']

        if args.dry_run:
            bundles = client.fetch('bundles', params={
                'specs': deleted_uuids,
                'include': ['owner']
            })
            print >>self.stdout, 'This command would permanently remove the following bundles (not doing so yet):'
            self.print_bundle_info_list(bundles, uuid_only=False, print_ref=False)
        else:
            for uuid in deleted_uuids:
                print >>self.stdout, uuid

    @Commands.command(
        'search',
        aliases=('s',),
        help=[
            'Search for bundles on a CodaLab instance (returns 10 results by default).',
            '  search <keyword> ... <keyword> : Match name and description.',
            '  search name=<name>             : More targeted search of using metadata fields.',
            '  search size=.sort              : Sort by a particular field.',
            '  search size=.sort-             : Sort by a particular field in reverse.',
            '  search size=.sum               : Compute total of a particular field.',
            '  search .mine                   : Match only bundles I own.',
            '  search .floating               : Match bundles that aren\'t on any worksheet.',
            '  search .count                  : Count the number of bundles.',
            '  search .limit=10               : Limit the number of results to the top 10.',
        ],
        arguments=(
            Commands.Argument('keywords', help='Keywords to search for.', nargs='+'),
            Commands.Argument('-a', '--append', help='Append these bundles to the current worksheet.', action='store_true'),
            Commands.Argument('-u', '--uuid-only', help='Print only uuids.', action='store_true'),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ),
    )
    def do_search_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        bundles = client.fetch('bundles', params={
            'worksheet': worksheet_uuid,
            'keywords': args.keywords,
            'include': ['owner'],
        })

        # Print direct numeric result
        if 'meta' in bundles:
            print >>self.stdout, bundles['meta']['result']
            return

        # Print table
        if len(bundles) > 0:
            self.print_bundle_info_list(bundles, uuid_only=args.uuid_only, print_ref=False)

        # Add the bundles to the current worksheet
        if args.append:
            client.create('worksheet-items', data=[
                {
                    'worksheet': JsonApiRelationship('worksheets', worksheet_uuid),
                    'bundle': JsonApiRelationship('bundles', bundle['uuid']),
                    'type': worksheet_util.TYPE_BUNDLE,
                }
                for bundle in bundles
            ])
            worksheet_info = client.fetch('worksheets', worksheet_uuid)
            print >>self.stdout, 'Added %d bundles to %s' % (len(bundles), self.worksheet_str(worksheet_info))

        return {
            'refs': self.create_reference_map('bundle', bundles)
        }

    def create_reference_map(self, info_type, info_list):
        """
        Return dict of dicts containing name, uuid and type for each
        bundle/worksheet in the info_list. This information is needed to recover
        URL on the web client.
        """
        return {
            worksheet_util.apply_func(UUID_POST_FUNC, info['uuid']) : {
                'type': info_type,
                'uuid': info['uuid'],
                'name': info.get('metadata', info).get('name', None)
            } for info in info_list if 'uuid' in info
        }

    @Commands.command(
        name='ls',
        help='List bundles in a worksheet.',
        arguments=(
            Commands.Argument('-u', '--uuid-only', help='Print only uuids.', action='store_true'),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ),
    )
    def do_ls_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        worksheet_info = client.fetch('worksheets', worksheet_uuid, params={
            'include': ['owner', 'group_permissions', 'items', 'items.bundle', 'items.bundle.owner']
        })
        if not args.uuid_only:
            print >>self.stdout, self._worksheet_description(worksheet_info)
        bundle_info_list = [item['bundle'] for item in worksheet_info['items'] if item['type'] == 'bundle']
        self.print_bundle_info_list(bundle_info_list, args.uuid_only, print_ref=True)
        return {
            'refs': self.create_reference_map('bundle', bundle_info_list)
        }

    def _worksheet_description(self, worksheet_info):
        fields = [
            ('Worksheet', self.worksheet_str(worksheet_info)),
            ('Title', formatting.verbose_contents_str(worksheet_info['title'])),
            ('Tags', ' '.join(worksheet_info['tags'])),
            ('Owner', self.simple_user_str(worksheet_info['owner']) +
                (' [anonymous]' if worksheet_info['is_anonymous'] else '')),
            ('Permissions', group_permissions_str(worksheet_info['group_permissions']) +
                (' [frozen]' if worksheet_info['frozen'] else '')),
        ]
        return '\n'.join('### %s: %s' % (k, v) for k, v in fields)

    def print_bundle_info_list(self, bundle_info_list, uuid_only, print_ref):
        """
        Helper function: print >>self.stdout, a nice table showing all provided bundles.
        """
        if uuid_only:
            for bundle_info in bundle_info_list:
                print >>self.stdout, bundle_info['uuid']
        else:
            def get(i, info, col):
                if col == 'ref':
                    return '^' + str(len(bundle_info_list) - i)
                else:
                    return info.get(col, nested_dict_get(info, 'metadata', col))

            for bundle_info in bundle_info_list:
                bundle_info['owner'] = nested_dict_get(bundle_info, 'owner', 'user_name')

            columns = (('ref',) if print_ref else ()) + ('uuid', 'name', 'summary', 'owner', 'created', 'data_size', 'state')
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
            Commands.Argument('bundle_spec', help=BUNDLE_SPEC_FORMAT, nargs='+', completer=BundlesCompleter),
            Commands.Argument('-f', '--field', help='Print out these comma-separated fields.'),
            Commands.Argument('-r', '--raw', action='store_true', help='Print out raw information (no rendering of numbers/times).'),
            Commands.Argument('-v', '--verbose', action='store_true', help='Print top-level contents of bundle, children bundles, and host worksheets.'),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ),
    )
    def do_info_command(self, args):
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        bundles = client.fetch('bundles', params={
            'specs': args.bundle_spec,
            'worksheet': worksheet_uuid,
            'include': ['owner'] + (['children', 'group_permissions', 'host_worksheets'] if args.verbose else []),
        })

        for i, info in enumerate(bundles):
            if args.field:
                # Display individual fields (arbitrary genpath)
                values = []
                for genpath in args.field.split(','):
                    if worksheet_util.is_file_genpath(genpath):
                        value = contents_str(client.interpret_file_genpaths([(info['id'], genpath, None)])[0])
                    else:
                        value = worksheet_util.interpret_genpath(info, genpath)
                    values.append(value)
                print >>self.stdout, '\t'.join(map(str, values))
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

        # Headless client should fire OpenBundle UI action if no special flags used
        if self.headless and not (args.field or args.raw or args.verbose):
            return ui_actions.serialize([ui_actions.OpenBundle(bundle['id']) for bundle in bundles])

    @staticmethod
    def key_value_str(key, value):
        return '%-26s: %s' % (key, formatting.verbose_contents_str(unicode(value) if value is not None else None))

    def print_basic_info(self, client, info, raw):
        """
        print >>self.stdout, the basic information for a bundle (key/value pairs).
        """

        metadata = info['metadata']
        lines = []  # The output that we're accumulating

        # Bundle fields
        for key in ('bundle_type', 'uuid', 'data_hash', 'state', 'command', 'is_anonymous'):
            if not raw:
                if key not in info: continue
            lines.append(self.key_value_str(key, info.get(key)))

        # Owner info
        lines.append(self.key_value_str('owner', self.simple_user_str(info['owner'])))

        # Metadata fields (standard)
        cls = get_bundle_subclass(info['bundle_type'])
        for key, value in worksheet_util.get_formatted_metadata(cls, metadata, raw):
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
                parent = path_util.safe_join(contents_str(dep['parent_name']) + '(' + dep['parent_uuid'] + ')', dep['parent_path'])
                lines.append('  %s: %s' % (child, parent))
        if info['dependencies']:
            deps = info['dependencies']
            display_dependencies('dependencies', deps)

        print >>self.stdout, '\n'.join(lines)

    def print_children(self, info):
        print >>self.stdout, 'children:'
        for child in info['children']:
            print >>self.stdout, "  %s" % self.simple_bundle_str(child)

    def print_host_worksheets(self, info):
        print >>self.stdout, 'host_worksheets:'
        for host_worksheet_info in info['host_worksheets']:
            print >>self.stdout, "  %s" % self.simple_worksheet_str(host_worksheet_info)

    def print_permissions(self, info):
        print >>self.stdout, 'permission: %s' % permission_str(info['permission'])
        print >>self.stdout, 'group_permissions:'
        print >>self.stdout, '  %s' % group_permissions_str(info['group_permissions'])

    def print_contents(self, client, info):
        def wrap(string):
            return '=== ' + string + ' preview ==='

        print >>self.stdout, wrap('contents')
        bundle_uuid = info['uuid']
        info = self.print_target_info(client, (bundle_uuid, ''), decorate=True)
        if info is not None and info['type'] == 'directory':
            for item in info['contents']:
                if item['name'] not in ['stdout', 'stderr']:
                    continue
                print >>self.stdout, wrap(item['name'])
                self.print_target_info(client, (bundle_uuid, item['name']), decorate=True)

    @Commands.command(
        'mount',
        help=[
            'Beta feature: this command may change in a future release. Mount the contents of a bundle at a read-only mountpoint.',
        ],
        arguments=(
            Commands.Argument('target_spec', help=TARGET_SPEC_FORMAT, completer=TargetsCompleter),
            Commands.Argument('--mountpoint', help='Empty directory path to set up as the mountpoint for FUSE.'),
            Commands.Argument('--verbose', help='Verbose mode for BundleFUSE.', action='store_true', default=False),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ),
    )
    def do_mount_command(self, args):
        if bundle_fuse.fuse_is_available:
            self._fail_if_headless(args)  # Disable on headless systems

            client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
            target = self.parse_target(client, worksheet_uuid, args.target_spec)
            uuid, path = target

            mountpoint = path_util.normalize(args.mountpoint)
            path_util.check_isvalid(mountpoint, 'mount')
            print >>self.stdout, 'BundleFUSE mounting bundle {} on {}'.format(uuid, mountpoint)
            print >>self.stdout, 'BundleFUSE will run and maintain the mounted filesystem in the foreground. CTRL-C to cancel.'
            bundle_fuse.bundle_mount(client, mountpoint, target, args.verbose)
            print >>self.stdout, 'BundleFUSE shutting down.'
        else:
            print >>self.stdout, 'fuse is not installed'

    @Commands.command(
        'cat',
        help=[
            'Print the contents of a file/directory in a bundle.',
            'Note that cat on a directory will list its files.',
        ],
        arguments=(
            Commands.Argument('target_spec', help=TARGET_SPEC_FORMAT, completer=TargetsCompleter),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ),
    )
    def do_cat_command(self, args):
        self._fail_if_headless(args)  # Files might be too big

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        target = self.parse_target(client, worksheet_uuid, args.target_spec)
        self.print_target_info(client, target, decorate=False, fail_if_not_exist=True)

    # Helper: shared between info and cat
    def print_target_info(self, client, target, decorate, maxlines=10, fail_if_not_exist=False):
        info = client.fetch_contents_info(target[0], target[1], 1)
        info_type = info.get('type')

        if info_type is None:
            print >>self.stdout, formatting.verbose_contents_str(None)

        if info_type == 'file':
            if decorate:
                contents = client.fetch_contents_blob(target[0], target[1], head=maxlines)
            else:
                contents = client.fetch_contents_blob(target[0], target[1])
            with closing(contents):
                shutil.copyfileobj(contents, self.stdout)

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
                    'perm': oct(x['perm']) if 'perm' in x else ''
                }
                for x in info['contents']
            ]
            contents = sorted(contents, key=lambda r: r['name'])
            self.print_table(('name', 'perm', 'size'), contents, justify={'size': 1}, indent='')

        if info_type == 'link':
            print >>self.stdout, ' -> ' + info['link']

        return info

    @Commands.command(
        'wait',
        help='Wait until a run bundle finishes.',
        arguments=(
            Commands.Argument('target_spec', help=TARGET_SPEC_FORMAT, completer=BundlesCompleter),
            Commands.Argument('-t', '--tail', action='store_true', help='Print out the tail of the file or bundle and block until the run bundle has finished running.'),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ),
    )
    def do_wait_command(self, args):
        self._fail_if_headless(args)

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
        print >>self.stdout, bundle_uuid

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

        SLEEP_PERIOD = 1.0

        # Wait for all files to become ready
        for subpath in subpaths:
            while True:
                try:
                    client.fetch_contents_info(bundle_uuid, subpath, 0)
                    break
                except NotFoundError:
                    time.sleep(SLEEP_PERIOD)

        info = None
        run_finished = False
        while True:
            if not run_finished:
                info = client.fetch('bundles', bundle_uuid)
                run_finished = info['state'] in State.FINAL_STATES

            # Read data.
            for i in xrange(0, len(subpaths)):
                # If the subpath we're interested in appears, check if it's a
                # file and if so, initialize the offset.
                if subpath_is_file[i] is None:
                    target_info = client.fetch_contents_info(bundle_uuid, subpaths[i], 0)
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
                    with closing(client.fetch_contents_blob(bundle_uuid, subpaths[i], byte_range)) as contents:
                        result = contents.read()
                    if not result:
                        break
                    subpath_offset[i] += len(result)
                    self.stdout.write(result)
                    if len(result) < READ_LENGTH:
                        # No more to read.
                        break

            self.stdout.flush()

            # The run finished and we read all the data.
            if run_finished:
                break

            # Sleep, since we've finished reading all the data available.
            time.sleep(SLEEP_PERIOD)

        return info['state']

    @Commands.command(
        'mimic',
        help=[
            'Creates a set of bundles based on analogy with another set.',
            '  mimic <run>      : Rerun the <run> bundle.',
            '  mimic A B        : For all run bundles downstream of A, rerun with B instead.',
            '  mimic A X B -n Y : For all run bundles used to produce X depending on A, rerun with B instead to produce Y.',
        ],
        arguments=(
            Commands.Argument('bundles', help='Bundles: old_input_1 ... old_input_n old_output new_input_1 ... new_input_n (%s).' % BUNDLE_SPEC_FORMAT, nargs='+', completer=BundlesCompleter),
        ) + MIMIC_ARGUMENTS,
    )
    def do_mimic_command(self, args):
        self.mimic(args)

    @Commands.command(
        'macro',
        help=[
            'Use mimicry to simulate macros.',
            '  macro M A B   <=>   mimic M-in1 M-in2 M-out A B'
        ],
        arguments=(
            Commands.Argument('macro_name', help='Name of the macro (look for <macro_name>-in1, ..., and <macro_name>-out bundles).'),
            Commands.Argument('bundles', help='Bundles: new_input_1 ... new_input_n (%s)' % BUNDLE_SPEC_FORMAT, nargs='+', completer=BundlesCompleter),
        ) + MIMIC_ARGUMENTS,
    )
    def do_macro_command(self, args):
        """
        Just like do_mimic_command.
        """
        # For a macro, it's important that the name be not-null, so that we
        # don't create bundles called '<macro_name>-out', which would clash
        # next time we try to use the macro.
        if not args.name: args.name = 'new'
        # Reduce to the mimic case
        args.bundles = [args.macro_name + '-in' + str(i+1) for i in range(len(args.bundles))] + \
                       [args.macro_name + '-out'] + args.bundles
        self.mimic(args)

    def add_mimic_args(self, parser):
        self.add_wait_args(parser)

    def mimic(self, args):
        """
        Use args.bundles to generate a call to bundle_util.mimic_bundles()
        """
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        bundle_uuids = self.resolve_bundle_uuids(client, worksheet_uuid, args.bundles)

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

        plan = bundle_util.mimic_bundles(
            client,
            old_inputs, old_output, new_inputs, args.name,
            worksheet_uuid, args.depth, args.shadow, args.dry_run)
        for (old, new) in plan:
            print >>self.stderr, '%s => %s' % (self.simple_bundle_str(old), self.simple_bundle_str(new))
        if len(plan) > 0:
            new_uuid = plan[-1][1]['uuid']  # Last new uuid to be created
            self.wait(client, args, new_uuid)
            print >>self.stdout, new_uuid
        else:
            print >>self.stdout, 'Nothing to be done.'

    @Commands.command(
        'kill',
        help='Instruct the appropriate worker to terminate the running bundle(s).',
        arguments=(
            Commands.Argument('bundle_spec', help=BUNDLE_SPEC_FORMAT, nargs='+', completer=BundlesCompleter),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ),
    )
    def do_kill_command(self, args):
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        bundle_uuids = self.resolve_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        for bundle_uuid in bundle_uuids:
            print >>self.stdout, bundle_uuid
        client.create('bundle-actions', [{
            'type': 'kill',
            'uuid': uuid
        } for uuid in bundle_uuids])

    @Commands.command(
        'write',
        help='Instruct the appropriate worker to write a small file into the running bundle(s).',
        arguments=(
            Commands.Argument('target_spec', help=TARGET_SPEC_FORMAT, completer=BundlesCompleter),
            Commands.Argument('string', help='Write this string to the target file.'),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ),
    )
    def do_write_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        target = self.parse_target(client, worksheet_uuid, args.target_spec)
        client.create('bundle-actions', {
            'type': 'write',
            'uuid': target[0],
            'subpath': target[1],
            'string': args.string,
        })
        print >>self.stdout, target[0]

    #############################################################################
    # CLI methods for worksheet-related commands follow!
    #############################################################################

    def worksheet_str(self, worksheet_info):
        return '%s::%s(%s)' % (self.manager.session()['address'], worksheet_info['name'], worksheet_info['uuid'])

    @Commands.command(
        'new',
        help='Create a new worksheet.',
        arguments=(
            Commands.Argument('name', help='Name of worksheet (%s).' % spec_util.NAME_REGEX.pattern),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ),
    )
    def do_new_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        new_worksheet = client.create('worksheets', data={'name': args.name})
        print >>self.stdout, new_worksheet['uuid']
        if self.headless:
            return ui_actions.serialize([
                ui_actions.OpenWorksheet(new_worksheet['uuid'])
            ])

    ITEM_DESCRIPTION = textwrap.dedent("""
    Item specifications, with the format depending on the specified item_type.
        text:      (<text>|%%<directive>)
        bundle:    {0}
        worksheet: {1}""").format(GLOBAL_BUNDLE_SPEC_FORMAT, WORKSHEET_SPEC_FORMAT).strip()

    @Commands.command(
        'add',
        help=[
            'Append text items, bundles, or subworksheets to a worksheet (possibly on a different instance).',
            'Bundles that do not yet exist on the destination instance will be copied over.',
        ],
        arguments=(
            Commands.Argument('item_type', help='Type of item(s) to add {text, bundle, worksheet}.', choices=('text', 'bundle', 'worksheet'), metavar='item_type'),
            Commands.Argument('item_spec', help=ITEM_DESCRIPTION, nargs='+', completer=UnionCompleter(WorksheetsCompleter, BundlesCompleter)),
            Commands.Argument('dest_worksheet', help='Worksheet to which to add items (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
            Commands.Argument('-d', '--copy-dependencies', help='If adding bundles, also add dependencies of the bundles.', action='store_true'),
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
                    dest_client.create('worksheet-items', data={
                        'type': worksheet_util.TYPE_DIRECTIVE,
                        'worksheet': JsonApiRelationship('worksheets', dest_worksheet_uuid),
                        'value': item_spec[1:].strip(),
                    })
                else:
                    dest_client.create('worksheet-items', data={
                        'type': worksheet_util.TYPE_MARKUP,
                        'worksheet': JsonApiRelationship('worksheets', dest_worksheet_uuid),
                        'value': item_spec,
                    })

        elif args.item_type == 'bundle':
            for bundle_spec in args.item_spec:
                source_client, source_spec = self.parse_spec(bundle_spec)

                # a base_worksheet_uuid is only applicable if we're on the source client
                base_worksheet_uuid = curr_worksheet_uuid if source_client is curr_client else None
                source_bundle_uuid = self.resolve_bundle_uuid(source_client, base_worksheet_uuid, source_spec)

                # copy (or add only if bundle already exists on destination)
                self.copy_bundle(source_client, source_bundle_uuid,
                                 dest_client, dest_worksheet_uuid,
                                 copy_dependencies=args.copy_dependencies,
                                 add_to_worksheet=True)

        elif args.item_type == 'worksheet':
            for worksheet_spec in args.item_spec:
                source_client, worksheet_spec = self.parse_spec(worksheet_spec)
                if source_client.address != dest_client.address:
                    raise UsageError("You cannot add worksheet links across instances.")

                # a base_worksheet_uuid is only applicable if we're on the source client
                base_worksheet_uuid = curr_worksheet_uuid if source_client is curr_client else None
                subworksheet_uuid = self.resolve_worksheet_uuid(source_client, base_worksheet_uuid, worksheet_spec)

                # add worksheet
                dest_client.create('worksheet-items', data={
                    'type': worksheet_util.TYPE_WORKSHEET,
                    'worksheet': JsonApiRelationship('worksheets', dest_worksheet_uuid),
                    'subworksheet': JsonApiRelationship('worksheets', subworksheet_uuid),
                })

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
            Commands.Argument('-u', '--uuid-only', help='Print only the worksheet uuid.', action='store_true'),
            Commands.Argument('worksheet_spec', help=WORKSHEET_SPEC_FORMAT, nargs='?', completer=UnionCompleter(AddressesCompleter, WorksheetsCompleter)),
        ),
    )
    def do_work_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        worksheet_info = client.fetch('worksheets', worksheet_uuid)
        if args.worksheet_spec:
            if args.uuid_only:
                print >>self.stdout, worksheet_info['uuid']
            return self.change_current_worksheet(client, worksheet_uuid, verbose=(not args.uuid_only))
        else:
            if worksheet_info:
                if args.uuid_only:
                    print >>self.stdout, worksheet_info['uuid']
                else:
                    print >>self.stdout, 'Currently on worksheet %s.' % (self.worksheet_str(worksheet_info))
            else:
                print >>self.stdout, 'Not on any worksheet. Use `cl new` or `cl work` to switch to one.'

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
            print >>self.stdout, 'Switched to worksheet %s.' % (self.worksheet_str(worksheet_info))

    @Commands.command(
        'wedit',
        aliases=('we',),
        help=[
            'Edit the contents of a worksheet.',
            'See https://github.com/codalab/codalab-worksheets/wiki/User_Worksheet-Markdown for the markdown syntax.',
            '  wedit -n <name>          : Change the name of the worksheet.',
            '  wedit -T <tag> ... <tag> : Set the tags of the worksheet (e.g., paper).',
            '  wedit -o <username>      : Set the owner of the worksheet to <username>.',
        ],
        arguments=(
            Commands.Argument('worksheet_spec', help=WORKSHEET_SPEC_FORMAT, nargs='?', completer=WorksheetsCompleter),
            Commands.Argument('-n', '--name', help='Changes the name of the worksheet (%s).' % spec_util.NAME_REGEX.pattern),
            Commands.Argument('-t', '--title', help='Change title of worksheet.'),
            Commands.Argument('-T', '--tags', help='Change tags (must appear after worksheet_spec).', nargs='*'),
            Commands.Argument('-o', '--owner-spec', help='Change owner of worksheet.'),
            Commands.Argument('--freeze', help='Freeze worksheet to prevent future modification (PERMANENT!).', action='store_true'),
            Commands.Argument('--anonymous', help='Set worksheet to be anonymous (identity of the owner will NOT \n'
                                                  'be visible to users without \'all\' permission on the worksheet).',
                              dest='anonymous', action='store_true', default=None),
            Commands.Argument('--not-anonymous', help='Set bundle to be NOT anonymous.', dest='anonymous', action='store_false'),
            Commands.Argument('-f', '--file', help='Replace the contents of the current worksheet with this file.', completer=require_not_headless(FilesCompleter(directories=False))),
        ),
    )
    def do_wedit_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        worksheet_info = client.fetch('worksheets', worksheet_uuid, params={
            'include': ['items', 'items.bundle', 'items.subworksheet']
        })
        if args.freeze or any(arg is not None for arg in (args.name, args.title, args.tags, args.owner_spec, args.anonymous)):
            # Update the worksheet metadata.
            info = {
                'id': worksheet_info['id']
            }
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
            if args.anonymous is not None:
                info['is_anonymous'] = args.anonymous

            client.update('worksheets', info)
            print >>self.stdout, 'Saved worksheet metadata for %s(%s).' % (worksheet_info['name'], worksheet_info['uuid'])
        else:
            if self.headless:
                return ui_actions.serialize([ui_actions.SetEditMode(True)])

            # Either get a list of lines from the given file or request it from the user in an editor.
            if args.file:
                if args.file == '-':
                    infile = sys.stdin
                else:
                    infile = open(args.file)
                lines = [line.rstrip() for line in infile.readlines()]
            else:
                worksheet_info['items'] = map(self.unpack_item, worksheet_info['items'])
                lines = worksheet_util.request_lines(worksheet_info)

            # Update worksheet
            client.update_worksheet_raw(worksheet_info['id'], lines)
            print >>self.stdout, 'Saved worksheet items for %s(%s).' % (worksheet_info['name'], worksheet_info['uuid'])

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
        value_obj = formatting.string_to_tokens(value) if item_type == worksheet_util.TYPE_DIRECTIVE else value
        return bundle_info, subworksheet_info, value_obj, item_type

    @Commands.command(
        'print',
        aliases=('p',),
        help='Print the rendered contents of a worksheet.',
        arguments=(
            Commands.Argument('worksheet_spec', help=WORKSHEET_SPEC_FORMAT, nargs='?', completer=WorksheetsCompleter),
            Commands.Argument('-r', '--raw', action='store_true', help='Print out the raw contents (for editing).'),
        ),
    )
    def do_print_command(self, args):
        self._fail_if_headless(args)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        worksheet_info = client.fetch('worksheets', worksheet_uuid, params={
            'include': ['owner', 'group_permissions', 'items', 'items.bundle', 'items.bundle.owner', 'items.subworksheet']
        })
        worksheet_info['items'] = map(self.unpack_item, worksheet_info['items'])

        if args.raw:
            lines = worksheet_util.get_worksheet_lines(worksheet_info)
            for line in lines:
                print >>self.stdout, line
        else:
            print >>self.stdout, self._worksheet_description(worksheet_info)
            interpreted = worksheet_util.interpret_items(worksheet_util.get_default_schemas(), worksheet_info['items'])
            self.display_interpreted(client, worksheet_info, interpreted)

    def display_interpreted(self, client, worksheet_info, interpreted):
        for item in interpreted['items']:
            mode = item['mode']
            data = item['interpreted']
            properties = item['properties']
            print >>self.stdout, ''  # Separate interpreted items
            if mode == 'markup' or mode == 'contents':
                if mode == 'contents':
                    maxlines = properties.get('maxlines')
                    if maxlines:
                        maxlines = int(maxlines)
                    try:
                        self.print_target_info(client, data, decorate=True, maxlines=maxlines)
                    except UsageError, e:
                        print >>self.stdout, 'ERROR:', e
                else:
                    print >>self.stdout, data
            elif mode == 'record' or mode == 'table':
                # header_name_posts is a list of (name, post-processing) pairs.
                header, contents = data
                contents = client.interpret_genpath_table_contents(contents)
                # print >>self.stdout, the table
                self.print_table(header, contents, show_header=(mode == 'table'), indent='  ')
            elif mode == 'html' or mode == 'image' or mode == 'graph':
                # Placeholder
                print >>self.stdout, '[' + mode + ']'
            elif mode == 'search':
                search_interpreted = client.interpret_search(data)
                self.display_interpreted(client, worksheet_info, search_interpreted)
            elif mode == 'wsearch':
                wsearch_interpreted = client.interpret_wsearch(data)
                self.display_interpreted(client, worksheet_info, wsearch_interpreted)
            elif mode == 'worksheet':
                print >>self.stdout, '[Worksheet ' + self.simple_worksheet_str(data) + ']'
            else:
                raise UsageError('Invalid display mode: %s' % mode)

    @Commands.command(
        'wls',
        aliases=('wsearch', 'ws'),
        help=[
            'List worksheets on the current instance matching the given keywords.',
            '  wls tag=paper : List worksheets tagged as "paper".',
            '  wls .mine     : List my worksheets.',
        ],
        arguments=(
            Commands.Argument('keywords', help='Keywords to search for.', nargs='*'),
            Commands.Argument('-a', '--address', help=ADDRESS_SPEC_FORMAT, completer=AddressesCompleter),
            Commands.Argument('-u', '--uuid-only', help='Print only uuids.', action='store_true'),
        ),
    )
    def do_wls_command(self, args):
        if args.address:
            address = self.manager.apply_alias(args.address)
            client = self.manager.client(address)
        else:
            client = self.manager.current_client()

        worksheet_dicts = client.fetch('worksheets', params={
            'keywords': args.keywords,
            'include': ['owner', 'group_permissions'],
        })
        if args.uuid_only:
            for row in worksheet_dicts:
                print >>self.stdout, row['uuid']
        else:
            if worksheet_dicts:
                for row in worksheet_dicts:
                    row['owner'] = self.simple_user_str(row['owner'])
                    row['permissions'] = group_permissions_str(row['group_permissions'])
                post_funcs = {'uuid': UUID_POST_FUNC}
                self.print_table(('uuid', 'name', 'owner', 'permissions'), worksheet_dicts, post_funcs)
        return {
            'refs': self.create_reference_map('worksheet', worksheet_dicts)
        }

    @Commands.command(
        'wrm',
        help=[
            'Delete a worksheet.',
            'To be safe, you can only delete a worksheet if it has no items and is not frozen.',
        ],
        arguments=(
            Commands.Argument('worksheet_spec', help=WORKSHEET_SPEC_FORMAT, nargs='+', completer=WorksheetsCompleter),
            Commands.Argument('--force', action='store_true', help='Delete worksheet even if it is non-empty and frozen.'),
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
            'The existing items on the destination worksheet are not affected unless the -r/--replace flag is set.',
        ],
        arguments=(
            Commands.Argument('source_worksheet_spec', help=WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
            Commands.Argument('dest_worksheet_spec', help=WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
            Commands.Argument('-r', '--replace', help='Replace everything on the destination worksheet with the items from the source worksheet, instead of appending (does not delete old bundles, just detaches).', action='store_true'),
        ),
    )
    def do_wadd_command(self, args):
        # Source worksheet
        (source_client, source_worksheet_uuid) = self.parse_client_worksheet_uuid(args.source_worksheet_spec)
        source_items = source_client.fetch('worksheets', source_worksheet_uuid, params={
            'include': ['items', 'items.bundle']
        })['items']

        # Destination worksheet
        (dest_client, dest_worksheet_uuid) = self.parse_client_worksheet_uuid(args.dest_worksheet_spec)

        # Save all items to the destination worksheet
        for item in source_items:
            item['worksheet'] = JsonApiRelationship('worksheets', dest_worksheet_uuid)
        dest_client.create('worksheet-items', source_items, params={'replace': args.replace})

        # Copy over the bundles
        for item in source_items:
            if item['type'] == worksheet_util.TYPE_BUNDLE:
                self.copy_bundle(source_client, item['bundle']['id'],
                                 dest_client, dest_worksheet_uuid,
                                 copy_dependencies=False, add_to_worksheet=False)

        print >>self.stdout, 'Copied %s worksheet items to %s.' % (len(source_items), dest_worksheet_uuid)


    #############################################################################
    # CLI methods for commands related to groups and permissions follow!
    #############################################################################

    @Commands.command(
        'gls',
        help='Show groups to which you belong.',
        arguments=(
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
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
            print >>self.stdout, 'No groups found.'

    @Commands.command(
        'gnew',
        help='Create a new group.',
        arguments=(
            Commands.Argument('name', help='Name of new group (%s).' % spec_util.NAME_REGEX.pattern),
        ),
    )
    def do_gnew_command(self, args):
        client = self.manager.current_client()
        group = client.create('groups', {'name': args.name})
        print >>self.stdout, 'Created new group %s(%s).' % (group['name'], group['id'])

    @Commands.command(
        'grm',
        help='Delete a group.',
        arguments=(
            Commands.Argument('group_spec', help='Group to delete (%s).' % GROUP_SPEC_FORMAT, completer=GroupsCompleter),
        ),
    )
    def do_grm_command(self, args):
        client = self.manager.current_client()
        group = client.fetch('groups', args.group_spec)
        client.delete('groups', group['id'])
        print >>self.stdout, 'Deleted group %s(%s).' % (group['name'], group['id'])

    @Commands.command(
        'ginfo',
        help='Show detailed information for a group.',
        arguments=(
            Commands.Argument('group_spec', help='Group to show information about (%s).' % GROUP_SPEC_FORMAT, completer=GroupsCompleter),
        ),
    )
    def do_ginfo_command(self, args):
        client = self.manager.current_client()
        group = client.fetch('groups', args.group_spec)

        members = []
        # group['owner'] may be a falsey null-relationship (i.e. for the public group)
        if group['owner']:
            members.append({
                'role': 'owner',
                'user': '%s(%s)' % (group['owner']['user_name'], group['owner']['id']),
            })
        for member in group['admins']:
            members.append({
                'role': 'admin',
                'user': '%s(%s)' % (member['user_name'], member['id']),
            })
        for member in group['members']:
            members.append({
                'role': 'member',
                'user': '%s(%s)' % (member['user_name'], member['id']),
            })

        print >>self.stdout, 'Members of group %s(%s):' % (group['name'], group['id'])
        self.print_table(('user', 'role'), members)

    @Commands.command(
        'uadd',
        help='Add a user to a group.',
        arguments=(
            Commands.Argument('user_spec', help='Username to add.'),
            Commands.Argument('group_spec', help='Group to add user to (%s).' % GROUP_SPEC_FORMAT, completer=GroupsCompleter),
            Commands.Argument('-a', '--admin', action='store_true', help='Give admin privileges to the user for the group.'),
        ),
    )
    def do_uadd_command(self, args):
        client = self.manager.current_client()

        user = client.fetch('users', args.user_spec)
        group = client.fetch('groups', args.group_spec)
        client.create_relationship('groups', group['id'],
                                   'admins' if args.admin else 'members',
                                   JsonApiRelationship('users', user['id']))

        print >>self.stdout, '%s in group %s as %s' % (
            user['user_name'],
            group['name'],
            'admin' if args.admin else 'member'
        )

    @Commands.command(
        'urm',
        help='Remove a user from a group.',
        arguments=(
            Commands.Argument('user_spec', help='Username to remove.'),
            Commands.Argument('group_spec', help='Group to remove user from (%s).' % GROUP_SPEC_FORMAT, completer=GroupsCompleter),
        ),
    )
    def do_urm_command(self, args):
        client = self.manager.current_client()
        user = client.fetch('users', args.user_spec)
        group = client.fetch('groups', args.group_spec)

        # Get the first member that matches the target user ID
        member = next(
            itertools.ifilter(
                lambda m: m['id'] == user['id'],
                group['members'] + group['admins']),
            None)

        if member is None:
            print >>self.stdout, '%s is not a member of group %s.' % (user['user_name'], group['name'])
        else:
            client.delete_relationship('groups', group['id'], 'members',
                                       JsonApiRelationship('users', user['id']))
            print >>self.stdout, 'Removed %s from group %s.' % (user['user_name'], group['name'])

    @Commands.command(
        'perm',
        help='Set a group\'s permissions for a bundle.',
        arguments=(
            Commands.Argument('bundle_spec', help=BUNDLE_SPEC_FORMAT, nargs='+', completer=BundlesCompleter),
            Commands.Argument('group_spec', help=GROUP_SPEC_FORMAT, completer=GroupsCompleter),
            Commands.Argument('permission_spec', help=PERMISSION_SPEC_FORMAT, completer=ChoicesCompleter(['none', 'read', 'all'])),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ),
    )
    def do_perm_command(self, args):
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)

        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)
        group = client.fetch('groups', args.group_spec)

        bundle_uuids = self.resolve_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        new_permission = parse_permission(args.permission_spec)

        client.create('bundle-permissions', [{
            'group': JsonApiRelationship('groups', group['id']),
            'bundle': JsonApiRelationship('bundles', uuid),
            'permission': new_permission,
        } for uuid in bundle_uuids])

        print >>self.stdout, "Group %s(%s) has %s permission on %d bundles." % \
            (group['name'], group['id'], permission_str(new_permission), len(bundle_uuids))

    @Commands.command(
        'wperm',
        help='Set a group\'s permissions for a worksheet.',
        arguments=(
            Commands.Argument('worksheet_spec', help=WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
            Commands.Argument('group_spec', help=GROUP_SPEC_FORMAT, completer=GroupsCompleter),
            Commands.Argument('permission_spec', help=PERMISSION_SPEC_FORMAT),
        ),
    )
    def do_wperm_command(self, args):
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        worksheet = client.fetch('worksheets', worksheet_uuid)
        group = client.fetch('groups', args.group_spec)
        new_permission = parse_permission(args.permission_spec)

        client.create('worksheet-permissions', {
            'group': JsonApiRelationship('groups', group['id']),
            'worksheet': JsonApiRelationship('worksheets', worksheet_uuid),
            'permission': new_permission,
        })

        print >>self.stdout, "Group %s has %s permission on worksheet %s." % \
            (self.simple_group_str(group), permission_str(new_permission),
             self.simple_worksheet_str(worksheet))

    @Commands.command(
        'chown',
        help='Set the owner of bundles.',
        arguments=(
            Commands.Argument('user_spec', help='Username to set as the owner.'),
            Commands.Argument('bundle_spec', help=BUNDLE_SPEC_FORMAT, nargs='+', completer=BundlesCompleter),
            Commands.Argument('-w', '--worksheet-spec', help='Operate on this worksheet (%s).' % WORKSHEET_SPEC_FORMAT, completer=WorksheetsCompleter),
        ),
    )
    def do_chown_command(self, args):
        """
        Change the owner of bundles.
        """
        args.bundle_spec = spec_util.expand_specs(args.bundle_spec)
        client, worksheet_uuid = self.parse_client_worksheet_uuid(args.worksheet_spec)

        bundle_uuids = self.resolve_bundle_uuids(client, worksheet_uuid, args.bundle_spec)
        owner_id = client.fetch('users', args.user_spec)['id']

        client.update('bundles', [{
            'id': id_,
            'owner': JsonApiRelationship('users', owner_id),
        } for id_ in bundle_uuids])
        for uuid in bundle_uuids:
            print >>self.stdout, uuid

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
            Commands.Argument('user_spec', nargs='?', help='Username or id of user to update [default: the authenticated user]'),
            Commands.Argument('--first-name', help='First name'),
            Commands.Argument('--last-name', help='Last name'),
            Commands.Argument('--affiliation', help='Affiliation'),
            Commands.Argument('--url', help='Website URL'),
            Commands.Argument('-t', '--time-quota', help='Total amount of time allowed (e.g., 3, 3m, 3h, 3d)'),
            Commands.Argument('-d', '--disk-quota', help='Total amount of disk allowed (e.g., 3, 3k, 3m, 3g, 3t)'),
        ),
    )
    def do_uedit_command(self, args):
        """
        Edit properties of users.
        """
        client = self.manager.current_client()

        # Build user info
        user_info = {
            key: getattr(args, key)
            for key in (
                'first_name',
                'last_name',
                'affiliation',
                'url',
            )
            if getattr(args, key) is not None
        }
        if args.time_quota is not None:
            user_info['time_quota'] = formatting.parse_duration(args.time_quota)
        if args.disk_quota is not None:
            user_info['disk_quota'] = formatting.parse_size(args.disk_quota)
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
        'uinfo',
        help=[
            'Show user information.',
        ],
        arguments=(
            Commands.Argument('user_spec', nargs='?', help='Username or id of user to show [default: the authenticated user]'),
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
        self.print_user_info(user)

    def print_user_info(self, user):
        def print_attribute(key, value):
            print >>self.stdout, u'{:<15}: {}'.format(key, value).encode('utf-8')

        for key in ('id', 'user_name', 'first_name', 'last_name',
                    'affiliation', 'url', 'date_joined'):
            print_attribute(key, user.get(key, None))

        # These fields will not be returned by the server if the
        # authenticated user is not root, so stop early on first KeyError
        try:
            for key in ('last_login', 'email'):
                print_attribute(key, user[key])

            print_attribute(
                'time', formatting.ratio_str(
                    formatting.duration_str,
                    user['time_used'],
                    user['time_quota']))

            print_attribute(
                'disk', formatting.ratio_str(formatting.size_str,
                                             user['disk_used'],
                                             user['disk_quota']))
        except KeyError:
            pass

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
            Commands.Argument('-o', '--offset', help='Offset in the result list.', type=int, default=0),
            Commands.Argument('-l', '--limit', help='Limit in the result list.', type=int, default=20),
            Commands.Argument('-n', '--count', help='Just count.', action='store_true'),
            Commands.Argument('-g', '--group-by', help='Group by this field (e.g., date).'),
        ),
    )
    def do_events_command(self, args):
        self._fail_if_headless(args)
        self._fail_if_not_local(args)

        # Build query
        query_info = {
            'user': args.user, 'command': args.match_command, 'args': args.args, 'uuid': args.uuid,
            'count': args.count, 'group_by': args.group_by
        }
        info = self.manager.model().get_events_log_info(query_info, args.offset, args.limit)
        if 'counts' in info:
            for row in info['counts']:
                print >>self.stdout, '\t'.join(map(str, list(row)))
        if 'events' in info:
            for event in info['events']:
                row = [
                    event.end_time.strftime('%Y-%m-%d %X') if event.end_time != None else '',
                    '%.3f' % event.duration if event.duration != None else '',
                    '%s(%s)' % (event.user_name, event.user_id),
                    event.command, event.args]
                print >>self.stdout, '\t'.join(row)

    @Commands.command(
        'reset',
        help='Delete the CodaLab bundle store and reset the database (local only).',
        arguments=(
            Commands.Argument('--commit', action='store_true', help='Reset is a no-op unless committed.'),
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
        print >>self.stdout, 'Deleting entire database...'
        self.manager.model()._reset()

    @Commands.command(
        'bs-add-partition',
        help='Add another partition for storage (MultiDiskBundleStore only)',
        arguments=(
            Commands.Argument('name',
                              help='The name you\'d like to give this partition for CodaLab.',),
            Commands.Argument('path',
                              help=' '.join(['The target location you would like to use for storing bundles.',
                                             'This directory should be underneath a mountpoint for the partition',
                                             'you would like to use. You are responsible for configuring the',
                                             'mountpoint yourself.']),),
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
            print >> sys.stderr, "This command can only be run when MultiDiskBundleStore is in use."
            sys.exit(1)
        self.manager.bundle_store().add_partition(args.path, args.name)

    @Commands.command(
        'bs-rm-partition',
        help='Remove a partition by its number (MultiDiskBundleStore only)',
        arguments=(
            Commands.Argument('partition', help='The partition you want to remove.'),
        ),
    )
    def do_rm_partition_command(self, args):
        self._fail_if_headless(args)
        self._fail_if_not_local(args)
        if not isinstance(self.manager.bundle_store(), MultiDiskBundleStore):
            print >> sys.stderr, "This command can only be run when MultiDiskBundleStore is in use."
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
            print >> sys.stderr, "This command can only be run when MultiDiskBundleStore is in use."
            sys.exit(1)
        self.manager.bundle_store().ls_partitions()

    @Commands.command(
        'bs-health-check',
        help='Perform a health check on the bundle store, garbage collecting bad files in the store. Performs a dry run by default, use -f to force removal.',
        arguments=(
            Commands.Argument('-f', '--force', help='Perform all garbage collection and database updates instead of just printing what would happen', action='store_true'),
            Commands.Argument('-d', '--data-hash', help='Compute the digest for every bundle and compare against data_hash for consistency', action='store_true'),
            Commands.Argument('-r', '--repair', help='When used with --force and --data-hash, repairs incorrect data_hash in existing bundles', action='store_true'),
        ),
    )
    def do_bs_health_check(self, args):
        self._fail_if_headless(args)
        self._fail_if_not_local(args)
        print >> sys.stderr, 'Performing Health Check...'
        self.manager.bundle_store().health_check(self.manager.model(), args.force, args.data_hash, args.repair)

    def _fail_if_headless(self, args):
        if self.headless:
            raise UsageError('Cannot execute CLI command: %s' % args.command)

    def _fail_if_not_local(self, args):
        if 'localhost' not in self.manager.current_client().address:
            raise UsageError('Sanity check! Point your CLI at an instance on localhost before executing admin commands: %s' % args.command)
