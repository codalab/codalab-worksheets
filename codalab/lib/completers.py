"""
completers.py

Classes and functions for CLI autocomplete functionality, built within the argcomplete framework.
"""
import inspect
import itertools
import os
import re

from argcomplete import warn

from codalab.common import NotFoundError
from codalab.lib import spec_util, cli_util
from codalab.worker.download_util import BundleTarget


class CodaLabCompleter(object):
    """
    A CodaLabCompleter is just a class that needs to be initialized with a BundleCLI instance.
    """

    def __init__(self, cli):
        self.cli = cli


class WorksheetsCompleter(CodaLabCompleter):
    """
    Complete worksheet specs with suggestions pulled from the current client.
    """

    def __call__(self, prefix, action=None, parser=None, parsed_args=None):
        client, worksheet_spec = self.cli.parse_spec(prefix)
        worksheets = client.fetch('worksheets', params={'keywords': worksheet_spec})

        tokens = prefix.split('::')
        if len(tokens) == 1:
            format_string = "{}"
        else:
            format_string = tokens[0] + '::{}'

        if spec_util.UUID_PREFIX_REGEX.match(worksheet_spec):
            return (
                format_string.format(w['uuid'])
                for w in worksheets
                if w['uuid'].startswith(worksheet_spec)
            )
        else:
            return (format_string.format(w['name']) for w in worksheets)


class BundlesCompleter(CodaLabCompleter):
    """
    Complete bundle specs with suggestions from the current worksheet, or from the
    worksheet specified in the current arguments if one exists.
    """

    def __call__(self, prefix, action=None, parser=None, parsed_args=None, worksheet_uuid=None):
        worksheet_spec = getattr(parsed_args, 'worksheet_spec', None)
        client, worksheet_uuid = self.cli.parse_client_worksheet_uuid(worksheet_spec)

        if spec_util.UUID_PREFIX_REGEX.match(prefix):
            # uuids are matched globally
            return client.fetch('bundles', params={'keywords': 'uuid=' + prefix + '%'})
        else:
            # Names are matched locally on worksheet
            worksheet_info = client.fetch(
                'worksheets', worksheet_uuid, params={'include': ['items', 'items.bundle']}
            )
            bundle_infos = [
                item['bundle'] for item in worksheet_info['items'] if item['type'] == 'bundle'
            ]
            return (
                b['metadata']['name']
                for b in bundle_infos
                if b['metadata']['name'].startswith(prefix)
            )


class AddressesCompleter(CodaLabCompleter):
    """
    Complete address with suggestions from the current worksheet.
    """

    def __call__(self, prefix, action=None, parser=None, parsed_args=None):
        return (a for a in self.cli.manager.config.get('aliases', {}) if a.startswith(prefix))


class GroupsCompleter(CodaLabCompleter):
    """
    Complete group specs with suggestions pulled from the current client.
    """

    def __call__(self, prefix, action=None, parser=None, parsed_args=None):
        client = self.cli.manager.current_client()
        # TODO: allow fetch slice of attributes in API to optimize this
        group_dicts = client.fetch('groups')

        if spec_util.UUID_PREFIX_REGEX.match(prefix):
            return (short_uuid(g['uuid']) for g in group_dicts if g['uuid'].startswith(prefix))
        else:
            return (g['name'] for g in group_dicts if g['name'].startswith(prefix))


def NullCompleter(*args, **kwargs):
    """
    Completer that always returns nothing.
    """
    return ()


def UnionCompleter(*completers):
    """
    Return a CodaLabCompleter that suggests the union of the suggestions provided
    by the given completers.
    """

    class _UnionCompleter(CodaLabCompleter):
        def __call__(self, *args, **kwargs):
            initialized_completers = [
                initialize_completer(completer, self.cli) for completer in completers
            ]
            return set(
                itertools.chain(
                    *[completer(*args, **kwargs) for completer in initialized_completers]
                )
            )

    return _UnionCompleter


# TODO: argcomplete does not recognize that an argument following a target_spec might be another target_spec, or
# TODO: even a command. The argcomplete code is a mess, so this may require some monkey patching/pull requests to
# TODO: fix, or building our own argument parsing/completion framework (e.g. like aws-cli).
# TODO: https://github.com/codalab/codalab-worksheets/issues/223
class TargetsCompleter(CodaLabCompleter):
    def __call__(self, prefix, action=None, parser=None, parsed_args=None):
        key, target = cli_util.parse_key_target(prefix)
        if target is None:
            return ()

        instance, worksheet_spec, bundle_spec, subpath = cli_util.parse_target_spec(target)

        if worksheet_spec is None:
            worksheet_spec = getattr(parsed_args, 'worksheet_spec', None)

        client, worksheet_uuid = self.cli.parse_client_worksheet_uuid(worksheet_spec)

        # Build parameterizable format string for suggestions
        suggestion_format = ''.join(
            [
                (key + ':') if key is not None else '',
                (worksheet_spec + '//') if worksheet_spec is not None else '',
                (bundle_spec + '/') if subpath is not None else '',
                '{}',
            ]
        )

        if subpath is None:
            # then suggest completions for bundle_spec
            return (
                suggestion_format.format(b)
                for b in BundlesCompleter(self.cli)(
                    bundle_spec, action, parsed_args, worksheet_uuid
                )
            )
        else:
            # then suggest completions for subpath
            client, worksheet_uuid, resolved_target = self.cli.resolve_target(
                client, worksheet_uuid, target
            )
            dir_target = BundleTarget(resolved_target.bundle_uuid, os.path.dirname(subpath))
            try:
                info = client.fetch_contents_info(dir_target, depth=1)
            except NotFoundError:
                return ()
            if info['type'] == 'directory':
                matching_child_names = []
                basename = os.path.basename(subpath)
                for child in info['contents']:
                    if child['name'].startswith(basename):
                        matching_child_names.append(child['name'])
                return (
                    suggestion_format.format(os.path.join(dir_target.subpath, child_name))
                    for child_name in matching_child_names
                )
            else:
                return ()


class DockerImagesCompleter(CodaLabCompleter):
    """
    Matches the first column image tag output in 'docker search'
    """

    IMAGE_TAG_REGEX = re.compile(r'^(?P<tag>\S+)\s+')

    """
    Completes names of Docker images available on DockerHub.
    """

    def __call__(self, prefix, action=None, parser=None, parsed_args=None):
        import docker
        from codalab.worker.docker_utils import DEFAULT_DOCKER_TIMEOUT

        if prefix == "":
            prefix = "codalab"
        first_slash = prefix.find('/')
        trimmed_prefix = prefix[0:first_slash] if first_slash >= 0 else prefix
        try:
            client = docker.from_env(timeout=DEFAULT_DOCKER_TIMEOUT)
            return (img['name'] for img in client.images.search(trimmed_prefix))
        except docker.errors.APIError as ex:
            warn('Error: {}'.format(ex))


def short_uuid(full_uuid):
    from codalab.lib import worksheet_util

    return worksheet_util.apply_func('[0:8]', full_uuid)


def require_not_headless(completer):
    """
    Given a completer (that is not a CodaLabCompleter), return a CodaLabCompleter that will only call the
    given completer if the client is not headless.
    """

    class SafeCompleter(CodaLabCompleter):
        def __call__(self, *args, **kwargs):
            if self.cli.headless:
                return ()
            else:
                return completer(*args, **kwargs)

    return SafeCompleter


def initialize_completer(completer, cli):
    completer_class = completer if inspect.isclass(completer) else completer.__class__
    if issubclass(completer_class, CodaLabCompleter):
        return completer(cli)
    else:
        return completer
