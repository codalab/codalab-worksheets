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


class MakeBundle(NamedBundle):
  BUNDLE_TYPE = 'make'
  NAME_LENGTH = 8

  @classmethod
  def construct(cls, targets, metadata):
    uuid = cls.generate_uuid()
    # Check that targets does not include both keyed and anonymous targets.
    if len(targets) > 1 and '' in targets:
      raise UsageError('Must specify keys when packaging multiple targets!')
    # Support anonymous make bundles with names based on their uuid.
    if not metadata['name']:
      metadata['name'] = 'make-%s' % (uuid[:cls.NAME_LENGTH],)
    # List the dependencies of this bundle on its targets.
    dependencies = []
    for (child_path, (parent, parent_path)) in targets.iteritems():
      dependencies.append({
        'child_uuid': uuid,
        'child_path': child_path,
        'parent_uuid': parent.uuid,
        'parent_path': parent_path,
      })
    return super(MakeBundle, cls).construct({
      'uuid': uuid,
      'bundle_type': cls.BUNDLE_TYPE,
      'command': None,
      'data_hash': None,
      'state': State.CREATED,
      'metadata': metadata,
      'dependencies': dependencies,
    })

  def get_hard_dependencies(self):
    return self.dependencies

  def run(self, bundle_store, parent_dict, temp_dir):
    if any(not dep.child_path for dep in self.dependencies):
      message = '%s has keyed and anonymous targets!' % (self,),
      precondition(len(self.dependencies) == 1, message)
      temp_dir = os.path.join(temp_dir, 'anonymous_link')
    self.install_dependencies(bundle_store, parent_dict, temp_dir, rel=True)
    return bundle_store.upload(temp_dir, allow_symlinks=True)
