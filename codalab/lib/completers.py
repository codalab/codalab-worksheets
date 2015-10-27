'''
completers.py
'''
from codalab.lib import spec_util

class CodaLabCompleter(object):
    def __init__(self, manager):
        self.manager = manager

class WorksheetsCompleter(CodaLabCompleter):
    def __call__(self, prefix, **kwargs):
        client = self.manager.current_client()
        worksheets = client.search_worksheets([prefix])

        if spec_util.UUID_PREFIX_REGEX.match(prefix):
            return (w['uuid'] for w in worksheets if w['uuid'].startswith(prefix))
        else:
            return (w['name'] for w in worksheets if w['name'].startswith(prefix))

