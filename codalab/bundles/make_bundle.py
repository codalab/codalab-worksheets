import os
import tempfile

from codalab.bundles.named_bundle import NamedBundle
from codalab.common import (
  State,
  UsageError,
)


class MakeBundle(NamedBundle):
  BUNDLE_TYPE = 'make'
  NAME_LENGTH = 8

  @classmethod
  def construct(cls, uuid_targets, targets=None):
    uuid = cls.generate_uuid()
    # Compute metadata with default values for name and description.
    targets = targets or uuid_targets
    description = 'Package containing %s' % (
      ', '.join(
        '%s:%s' % (key, os.path.join(*[part for part in target if part]))
        for (key, target) in sorted(targets.iteritems())
      ),
    )
    metadata = {
      'name': 'make-%s' % (uuid[:cls.NAME_LENGTH],),
      'description': description,
      'tags': [],
    }
    # List the dependencies of this bundle on its targets.
    dependencies = []
    for (child_path, (parent_uuid, parent_path)) in uuid_targets.iteritems():
      dependencies.append({
        'child_uuid': uuid,
        'child_path': child_path,
        'parent_uuid': parent_uuid,
        'parent_path': parent_path,
      })
    return cls({
      'uuid': uuid,
      'bundle_type': cls.BUNDLE_TYPE,
      'data_hash': None,
      'state': State.CREATED,
      'metadata': metadata,
      'dependencies': dependencies,
    })

  def run(self, bundle_store, parent_dict):
    temp_dir = tempfile.mkdtemp()
    for dep in self.dependencies:
      parent = parent_dict[dep.parent_uuid]
      # Compute an absolute target and check that the dependency exists.
      full_target = os.path.join(
        bundle_store.get_location(parent.data_hash),
        dep.parent_path,
      )
      if not os.path.exists(full_target):
        raise UsageError('Target %s not found!' % (full_target,))
      # Create a symlink that points to the dependency's relative target.
      relative_target = os.path.join(
        os.pardir,
        bundle_store.get_location(parent.data_hash, relative=True),
        dep.parent_path,
      )
      link_path = os.path.join(temp_dir, dep.child_path)
      os.symlink(relative_target, link_path)
    return bundle_store.upload(temp_dir, allow_symlinks=True)
