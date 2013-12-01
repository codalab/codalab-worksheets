import os
import re

from codalab.bundles.named_bundle import NamedBundle
from codalab.common import State


class MakeBundle(NamedBundle):
  BUNDLE_TYPE = 'make'

  NAME_LENGTH = 8
  TARGET_KEY_REGEX = '^[a-zA-Z0-9_-]+\Z'

  @classmethod
  def construct(cls, targets):
    for key in targets:
      if not re.match(cls.TARGET_KEY_REGEX, key):
        raise ValueError(
          'Target key should match %s, was %s' %
          (cls.TARGET_KEY_REGEX, key)
        )
    uuid = cls.generate_uuid()
    description = 'Package containing %s' % (', '.join(
      '%s:%s' % (key, os.path.join(*[part for part in target if part]))
      for (key, target) in sorted(targets.iteritems())
    ),)
    metadata = {
      'name': 'make-%s' % (uuid[:cls.NAME_LENGTH],),
      'description': description,
      'tags': [],
    }
    return cls({
      'uuid': uuid,
      'bundle_type': cls.BUNDLE_TYPE,
      'data_hash': None,
      'state': State.CREATED,
      'metadata': metadata,
    })
