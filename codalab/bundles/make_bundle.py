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

class MakeBundle(NamedBundle):
    BUNDLE_TYPE = 'make'

    @classmethod
    def construct(cls, targets, command, metadata, uuid=None, data_hash=None, state=State.CREATED):
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
        })

    def get_hard_dependencies(self):
        return self.dependencies

    def complete(self, bundle_store, parent_dict, temp_dir):
        path_util.make_directory(temp_dir)

        # If the make bundle's targets are [('', target)], then treat this
        # bundle as directly pointing to target rather than having a field that
        # points to target.
        if any(not dep.child_path for dep in self.dependencies):
            message = '%s has keyed and anonymous targets!' % (self,),
            precondition(len(self.dependencies) == 1, message)
            temp_dir = os.path.join(temp_dir, 'anonymous_link')

        self.install_dependencies(bundle_store, parent_dict, temp_dir, relative_symlinks=True)
        return bundle_store.upload(temp_dir, allow_symlinks=True)
