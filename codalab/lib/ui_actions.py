"""
Mappings for UI action representation on the frontend side.
"""

class UIAction(object):
    def __init__(self, parameter=None):
        if getattr(self, 'KEY', None) is None:
            raise NotImplementedError
        self.parameter = parameter


class OpenWorksheet(UIAction):
    KEY = 'openWorksheet'


class SetEditMode(UIAction):
    KEY = 'setEditMode'


class OpenBundle(UIAction):
    KEY = 'openBundle'


def serialize(actions):
    return {
        'ui_actions': [[a.KEY, a.parameter] for a in actions]
    }
