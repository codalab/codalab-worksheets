'''
MetadataSpec is the specification of the name and type of a metadata key.

The METADATA_SPECS class attribute of each Bundle subclass should be a list of
these objects. For example, if some bundle type should have a string name and
a list of string tags, then its METADATA_SPECS would be:
  [
    MetadataSpec('name', basestring, 'The bundle name.'),
    MetadataSpec('tags', list, 'A list of searchable tags.'),
  ]
The description, short_key, and default of a metadata spec are used to produce
nicely-formatted help strings for bundle creation commands.
'''


def bool_constructor(x=False):
    # Need this method because metadata values are stored as strings in the database,
    # and bool('False') returns True.
    return x == 'True'


def unicode_constructor(s=""):
    # Need this method so that any uninterpretable characters
    # in the database will be replaced with '?'.
    return s.encode(encoding='ascii', errors='replace').decode()


class MetadataSpec(object):
    def __init__(
        self,
        key,
        type,
        description,
        short_key=None,
        metavar=None,
        default=None,
        generated=False,
        formatting=None,
        completer=None,
        hide_when_anonymous=False,
        optional=False,
        hidden=False,  # if hidden=True, field is hidden by default and can be displayed with -f
        lock_after_start=False,  # no longer editable once a bundle has reached 'starting' state
    ):
        self.key = key
        self.type = type
        self.description = description
        self.short_key = short_key
        self.metavar = metavar or (self.short_key or self.key).upper()
        self.default = default
        self.generated = generated
        self.formatting = formatting
        self.completer = completer
        self.hide_when_anonymous = hide_when_anonymous
        self.optional = optional
        self.hidden = hidden
        self.lock_after_start = lock_after_start

    def get_constructor(self):
        # Convert from string to type
        if self.type == str:
            return unicode_constructor
        if self.type == bool:
            return bool_constructor
        return self.type
