'''
NameBundle is an abstract Bundle supertype that all other bundle types subclass.
It requires name, description, and tags metadata for all of its subclasses.
'''
import os
import re

from codalab.common import UsageError
from codalab.lib import path_util
from codalab.objects.bundle import Bundle
from codalab.objects.metadata_spec import MetadataSpec


def get_default_name(args):
  if hasattr(args, 'path'):
    absolute_path = path_util.normalize(args.path)
    return os.path.basename(absolute_path)
  return ''


class NamedBundle(Bundle):
  NAME_REGEX = re.compile('^[a-zA-Z_][a-zA-Z0-9_\.\-]*\Z')
  METADATA_SPECS = (
    MetadataSpec(
      'name',
      basestring,
      'name: %s' % (NAME_REGEX.pattern,),
      default=get_default_name,
    ),
    MetadataSpec('description', basestring, 'human-readable description'),
    MetadataSpec('tags', set, 'list of searchable tags', metavar='TAG'),
  )

  def validate(self):
    super(NamedBundle, self).validate()
    bundle_type = self.bundle_type.title()
    if not self.metadata.name:
      raise UsageError('%ss must have non-empty names' % (bundle_type,))
    if not self.NAME_REGEX.match(self.metadata.name):
      raise UsageError(
        '%s names must match %s, was %s' %
        (bundle_type, self.NAME_REGEX.pattern, self.metadata.name)
      )
    if not self.metadata.description:
      raise UsageError('%ss must have non-empty descriptions' % (bundle_type,))
