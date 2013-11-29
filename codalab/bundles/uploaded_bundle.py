import re

from codalab.common import State
from codalab.objects.bundle import Bundle


class UploadedBundle(Bundle):
  NAME_REGEX = '^[a-zA-Z_][a-zA-Z0-9_-]*$'
  METADATA_SPEC = [
    ('name', 'name', 'name: %s' % (NAME_REGEX,)),
    ('description', 'desc', 'human-readable description'),
    ('tags', 'tags', 'list of searchable tags'),
  ]
  METADATA_TYPES = {
    'name': basestring,
    'description': basestring,
    'tags': set,
  }

  @classmethod
  def construct(cls, data_hash, metadata):
    return cls({
      'bundle_type': cls.BUNDLE_TYPE,
      'data_hash': data_hash,
      'state': State.READY,
      'is_current': True,
      'metadata': metadata,
    })

  def validate(self):
    super(UploadedBundle, self).validate()
    bundle_type = self.bundle_type.title()
    if not self.metadata.name:
      raise ValueError('%ss must have non-empty names' % (bundle_type,))
    if not re.match(self.NAME_REGEX, self.metadata.name):
      raise ValueError(
        "%s names must match '%s', was '%s'" %
        (bundle_type, self.NAME_REGEX, self.metadata.name)
      )
    if not self.metadata.description:
      raise ValueError('%ss must have non-empty descriptions' % (bundle_type,))
