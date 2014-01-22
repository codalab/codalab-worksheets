'''
LocalBundleClient is BundleClient implementation that interacts directly with a
BundleStore and a BundleModel. All filesystem operations are handled locally.
'''
from codalab.bundles import (
  get_bundle_subclass,
  UPLOADED_TYPES,
)
from codalab.common import (
  precondition,
  UsageError,
)
from codalab.client.bundle_client import BundleClient
from codalab.lib import (
  canonicalize,
  path_util,
)


class LocalBundleClient(BundleClient):
  def __init__(self, bundle_store, model):
    self.bundle_store = bundle_store
    self.model = model

  def get_bundle_info(self, bundle, parents=None, children=None):
    hard_dependencies = bundle.get_hard_dependencies()
    result = {
      'bundle_type': bundle.bundle_type,
      'data_hash': bundle.data_hash,
      'metadata': bundle.metadata.to_dict(),
      'state': bundle.state,
      'uuid': bundle.uuid,
      'hard_dependencies': [dep.to_dict() for dep in hard_dependencies]
    }
    if parents is not None:
      result['parents'] = [str(parent) for parent in parents]
    if children is not None:
      result['children'] = [str(child) for child in children]
    return result

  def get_spec_uuid(self, bundle_spec):
    return canonicalize.get_spec_uuid(self.model, bundle_spec)

  def get_target_path(self, target):
    return canonicalize.get_target_path(self.bundle_store, self.model, target)

  def get_bundle_target(self, target):
    (bundle_spec, subpath) = target
    return (self.model.get_bundle(self.get_spec_uuid(bundle_spec)), subpath)

  def upload(self, bundle_type, path, metadata):
    message = 'Invalid upload bundle_type: %s' % (bundle_type,)
    precondition(bundle_type in UPLOADED_TYPES, message)
    bundle_subclass = get_bundle_subclass(bundle_type)
    # Type-check the bundle metadata BEFORE uploading the bundle data.
    # This optimization will avoid file operations on failed bundle creations.
    bundle_subclass.construct(data_hash='', metadata=metadata).validate()
    data_hash = self.bundle_store.upload(path)
    bundle = bundle_subclass.construct(data_hash=data_hash, metadata=metadata)
    self.model.save_bundle(bundle)
    return bundle.uuid

  def make(self, targets, metadata):
    bundle_subclass = get_bundle_subclass('make')
    targets = {
      key: self.get_bundle_target(target)
      for (key, target) in targets.iteritems()
    }
    bundle = bundle_subclass.construct(targets, metadata)
    self.model.save_bundle(bundle)
    return bundle.uuid

  def run(self, program_target, input_target, command, metadata):
    program_target = self.get_bundle_target(program_target)
    input_target = self.get_bundle_target(input_target)
    bundle_subclass = get_bundle_subclass('run')
    bundle = bundle_subclass.construct(
      program_target, input_target, command, metadata)
    self.model.save_bundle(bundle)
    return bundle.uuid

  def edit(self, uuid, metadata):
    bundle = self.model.get_bundle(uuid)
    legal_keys = set(spec.key for spec in bundle.get_user_defined_metadata())
    illegal_keys = [key for key in metadata if key not in legal_keys]
    if illegal_keys:
      raise UsageError('Illegal metadata keys: %s' % (', '.join(illegal_keys),))
    self.model.update_bundle(bundle, {'metadata': metadata})

  def delete(self, bundle_spec, force=False):
    uuid = self.get_spec_uuid(bundle_spec)
    children = self.model.get_children(uuid)
    if children and not force:
      raise UsageError('Bundles depend on %s:\n  %s' % (
        bundle_spec,
        '\n  '.join(str(child) for child in children),
      ))
    self.model.delete_bundle_tree([uuid], force=force)

  def info(self, bundle_spec, parents=False, children=False):
    uuid = self.get_spec_uuid(bundle_spec)
    bundle = self.model.get_bundle(uuid)
    parents = self.model.get_parents(uuid) if parents else None
    children = self.model.get_children(uuid) if children else None
    return self.get_bundle_info(bundle, parents=parents, children=children)

  def ls(self, target):
    path = self.get_target_path(target)
    return path_util.ls(path)

  def cat(self, target):
    path = self.get_target_path(target)
    path_util.cat(path)

  def search(self, query=None):
    if query:
      bundles = self.model.search_bundles(**query)
    else:
      bundles = self.model.batch_get_bundles()
    return [self.get_bundle_info(bundle) for bundle in bundles]
