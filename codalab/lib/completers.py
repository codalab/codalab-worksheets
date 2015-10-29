'''
completers.py
'''
import inspect

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
        print "pinpon!!!!"
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
        worksheet_info = client.get_worksheet_info(worksheet_uuid, True, True)
        bundle_infos = self.cli.get_worksheet_bundles(worksheet_info)

        if spec_util.UUID_PREFIX_REGEX.match(prefix):
            return (short_uuid(b['uuid']) for b in bundle_infos if b['uuid'].startswith(prefix))
        else:
            return (b['metadata']['name'] for b in bundle_infos if b['metadata']['name'].startswith(prefix))

class AddressesCompleter(CodaLabCompleter):
    '''
    Complete address with suggestions from the current worksheet.
    '''
    def __call__(self, prefix, action=None, parsed_args=None):
        return (a for a in self.cli.manager.config['aliases'] if a.startswith(prefix))

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
