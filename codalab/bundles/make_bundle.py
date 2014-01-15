'''
MakeBundle is a Bundle type that symlinks a number of targets in from other
bundles to produce a new, packaged bundle.
'''
from codalab.bundles.named_bundle import NamedBundle
from codalab.common import State
from codalab.lib import path_util


class MakeBundle(NamedBundle):
  BUNDLE_TYPE = 'make'
  NAME_LENGTH = 8

  @classmethod
  def construct(cls, targets):
    uuid = cls.generate_uuid()
    # Compute metadata with default values for name and description.
    description = 'Package containing %s' % (
      ', '.join(
        '%s:%s' % (key, path_util.safe_join(parent.metadata.name, parent_path))
        for (key, (parent, parent_path)) in sorted(targets.iteritems())
      ),
    )
    metadata = {
      'name': 'make-%s' % (uuid[:cls.NAME_LENGTH],),
      'description': description,
      'tags': [],
    }
    # List the dependencies of this bundle on its targets.
    dependencies = []
    for (child_path, (parent, parent_path)) in targets.iteritems():
      dependencies.append({
        'child_uuid': uuid,
        'child_path': child_path,
        'parent_uuid': parent.uuid,
        'parent_path': parent_path,
      })
    return cls({
      'uuid': uuid,
      'bundle_type': cls.BUNDLE_TYPE,
      'command': None,
      'data_hash': None,
      'state': State.CREATED,
      'metadata': metadata,
      'dependencies': dependencies,
    })

  def run(self, bundle_store, parent_dict, temp_dir):
    self.install_dependencies(bundle_store, parent_dict, temp_dir, rel=True)
    return bundle_store.upload(temp_dir, allow_symlinks=True)
