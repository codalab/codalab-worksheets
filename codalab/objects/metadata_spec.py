'''
MetadataSpec is the specification of the name and type of a metadata key.

The METADATA_SPECS class attribute of each Bundle subclass should be a list of
these objects. For example, if some bundle type should have a string name and
a list of string tags, then its METADATA_SPECS would be:
  [
    MetadataSpec('name', basestring, 'The bundle name.'),
    MetadataSpec('tags', set, 'A list of searchable tags.'),
  ]
The description, short_key, and default of a metadata spec are used to produce
nicely-formatted help strings for bundle creation commands.
'''
class MetadataSpec(object):
  def __init__(
      self,
      key,
      type,
      description,
      short_key=None,
      metavar=None,
      default=None,
  ):
    self.key = key
    self.type = type
    self.description = description
    self.default = default
    self.short_key = short_key or self.key
    self.metavar = metavar or self.short_key.upper()
    self.default = default

  def get_default(self, args):
    if self.default:
      return self.default(args)
    return self.get_constructor()()

  def get_constructor(self):
    if self.type == basestring:
      return unicode
    return self.type
