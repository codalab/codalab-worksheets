'''
MakeBundle is a Bundle type that symlinks a number of targets in from other
bundles to produce a new, packaged bundle.
'''
import os

from codalab.bundles.named_bundle import NamedBundle
from codalab.common import (
  precondition,
  State,
  UsageError,
)
from codalab.lib import spec_util, path_util
from codalab.objects.metadata_spec import MetadataSpec

class MakeBundle(NamedBundle):
    BUNDLE_TYPE = 'make'
    METADATA_SPECS = list(NamedBundle.METADATA_SPECS)

    @classmethod
    def construct(cls, targets, command, metadata, owner_id, uuid=None, data_hash=None, state=State.CREATED):
        if not uuid: uuid = spec_util.generate_uuid()
        # Check that targets does not include both keyed and anonymous targets.
        if len(targets) > 1 and '' in targets:
            raise UsageError('Must specify keys when packaging multiple targets!')

        # List the dependencies of this bundle on its targets.
        dependencies = []
        for (child_path, (parent_uuid, parent_path)) in targets.iteritems():
            dependencies.append({
              'child_uuid': uuid,
              'child_path': child_path,
              'parent_uuid': parent_uuid,
              'parent_path': parent_path,
            })
        return super(MakeBundle, cls).construct({
          'uuid': uuid,
          'bundle_type': cls.BUNDLE_TYPE,
          'command': command,
          'data_hash': data_hash,
          'state': state,
          'metadata': metadata,
          'dependencies': dependencies,
          'owner_id': owner_id,
        })
