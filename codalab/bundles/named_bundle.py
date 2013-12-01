import re

from codalab.objects.bundle import Bundle
from codalab.objects.metadata_spec import MetadataSpec


class NamedBundle(Bundle):
  NAME_REGEX = '^[a-zA-Z_][a-zA-Z0-9_-]*\Z'
  METADATA_SPECS = (
    MetadataSpec('name', basestring, 'name: %s' % (NAME_REGEX,)),
    MetadataSpec('description', basestring, 'human-readable description'),
    MetadataSpec('tags', set, 'list of searchable tags'),
  )

  def validate(self):
    super(NamedBundle, self).validate()
    bundle_type = self.bundle_type.title()
    if not self.metadata.name:
      raise ValueError('%ss must have non-empty names' % (bundle_type,))
    if not re.match(self.NAME_REGEX, self.metadata.name):
      raise ValueError(
        '%s names must match %s, was %s' %
        (bundle_type, self.NAME_REGEX, self.metadata.name)
      )
    if not self.metadata.description:
      raise ValueError('%ss must have non-empty descriptions' % (bundle_type,))
