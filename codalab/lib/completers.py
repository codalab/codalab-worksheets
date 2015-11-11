'''
completers.py
'''
import inspect
import itertools

from argcomplete import warn
from argcomplete.completers import FilesCompleter

from codalab.lib import spec_util, worksheet_util

def short_uuid(full_uuid):
    return worksheet_util.apply_func('[0:8]', full_uuid)

class CodaLabCompleter(object):
    def __init__(self, cli):
        self.cli = cli

class WorksheetsCompleter(CodaLabCompleter):
    '''
    Complete worksheet specs with suggestions pulled from the current client.
    '''
    def __call__(self, prefix, **kwargs):
        client = self.cli.manager.current_client()
        worksheets = client.search_worksheets([prefix])

        if spec_util.UUID_PREFIX_REGEX.match(prefix):
            return (w['uuid'] for w in worksheets if w['uuid'].startswith(prefix))
        else:
            return (w['name'] for w in worksheets if w['name'].startswith(prefix))

class BundlesCompleter(CodaLabCompleter):
    '''
    Complete bundle specs with suggestions from the current worksheet, or from the
    worksheet specified in the current arguments if one exists.
    '''
    def __call__(self, prefix, action=None, parsed_args=None):
        worksheet_spec = getattr(parsed_args, 'worksheet_spec', None)
        client, worksheet_uuid = self.cli.parse_client_worksheet_uuid(worksheet_spec)

        if spec_util.UUID_PREFIX_REGEX.match(prefix):
            # uuids are matched globally
            return client.search_bundle_uuids(worksheet_uuid, ['uuid=' + prefix + '%'])
        else:
            # Names are matched locally on worksheet
            worksheet_info = client.get_worksheet_info(worksheet_uuid, True, True)
            bundle_infos = self.cli.get_worksheet_bundles(worksheet_info)
            return (b['metadata']['name'] for b in bundle_infos if b['metadata']['name'].startswith(prefix))

class AddressesCompleter(CodaLabCompleter):
    '''
    Complete address with suggestions from the current worksheet.
    '''
    def __call__(self, prefix, action=None, parsed_args=None):
        return (a for a in self.cli.manager.config.get('aliases', {}) if a.startswith(prefix))

class GroupsCompleter(CodaLabCompleter):
    '''
    Complete group specs with suggestions pulled from the current client.
    '''
    def __call__(self, prefix, action=None, parsed_args=None):
        client = self.cli.manager.current_client()
        group_dicts = client.list_groups()

        if spec_util.UUID_PREFIX_REGEX.match(prefix):
            return (short_uuid(g['uuid']) for g in group_dicts if g['uuid'].startswith(prefix))
        else:
            return (g['name'] for g in group_dicts if g['name'].startswith(prefix))

def require_not_headless(completer):
    '''
    Given a completer, return a CodaLabCompleter that will only call the
    given completer if the client is not headless.
    '''
    class SafeCompleter(CodaLabCompleter):
        def __call__(self, *args, **kwargs):
            if self.cli.headless:
                return ()
            elif inspect.isclass(completer):
                return completer()(*args, **kwargs)
            else:
                return completer(*args, **kwargs)

    return SafeCompleter

def NullCompleter(*args, **kwargs):
    '''
    Completer that always returns nothing.
    '''
    return ()

def UnionCompleter(*completers):
    '''
    Return a CodaLabCompleter that suggests the union of the suggestions provided
    by the given completers.
    '''
    class _UnionCompleter(CodaLabCompleter):
        def __call__(self, *args, **kwargs):
            ready_completers = []
            for completer in completers:
                completer_class = completer if inspect.isclass(completer) else completer.__class__
                if issubclass(completer_class, CodaLabCompleter):
                    completer = completer(self.cli)

                ready_completers.append(completer)

            return set(itertools.chain(*[completer(*args, **kwargs) for completer in ready_completers]))

    return _UnionCompleter

