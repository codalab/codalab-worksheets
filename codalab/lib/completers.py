'''
completers.py
'''
import itertools

class WorksheetsCompleter(object):
    def __init__(self, manager):
        self.manager = manager

    def __call__(self, prefix, **kwargs):
        client = self.manager.current_client()
        worksheets = client.search_worksheets([prefix])

        return itertools.chain(
            (w['uuid'] for w in worksheets if w['uuid'].startswith(prefix)),
            (w['name'] for w in worksheets if w['name'].startswith(prefix)),
        )

