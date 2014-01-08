'''
LocalBundleClient is BundleClient implementation that interacts directly with a
BundleStore and a BundleModel. All filesystem operations are handled locally.
'''
from codalab.bundles import (
  get_bundle_subclass,
  UPLOADABLE_TYPES,
)
from codalab.common import (
  precondition,
  State,
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

  def get_bundle_info(self, bundle):
    '''
    Convert a bundle to a data dict. This method should NOT hit the filesystem.
    '''
    location = None
    if bundle.state == State.READY:
      # get_location is in-memory and fast for existing bundle stores.
      location = self.bundle_store.get_location(bundle.data_hash)
    return {
      'bundle_type': bundle.bundle_type,
      'location': location,
      'metadata': bundle.metadata.to_dict(),
      'state': bundle.state,
      'uuid': bundle.uuid,
    }

  def get_spec_uuid(self, bundle_spec):
    return canonicalize.get_spec_uuid(self.model, bundle_spec)

  def get_target_path(self, target):
    return canonicalize.get_target_path(self.bundle_store, self.model, target)

  def get_bundle_target(self, target):
    (bundle_spec, subpath) = target
    return (self.model.get_bundle(self.get_spec_uuid(bundle_spec)), subpath)

  def upload(self, bundle_type, path, metadata):
    message = 'Invalid upload bundle_type: %s' % (bundle_type,)
    precondition(bundle_type in UPLOADABLE_TYPES, message)
    bundle_subclass = get_bundle_subclass(bundle_type)
    # Type-check the bundle metadata BEFORE uploading the bundle data.
    # This optimization will avoid file operations on failed bundle creations.
    bundle_subclass.construct(data_hash='', metadata=metadata).validate()
    data_hash = self.bundle_store.upload(path)
    bundle = bundle_subclass.construct(data_hash=data_hash, metadata=metadata)
    self.model.save_bundle(bundle)
    return bundle.uuid

  def make(self, targets):
    bundle_subclass = get_bundle_subclass('make')
    targets = {
      key: self.get_bundle_target(target)
      for (key, target) in targets.iteritems()
    }
    bundle = bundle_subclass.construct(targets)
    self.model.save_bundle(bundle)
    return bundle.uuid

  def run(self, program_target, input_target, command):
    program_target = self.get_bundle_target(program_target)
    input_target = self.get_bundle_target(input_target)
    bundle_subclass = get_bundle_subclass('run')
    bundle = bundle_subclass.construct(program_target, input_target, command)
    self.model.save_bundle(bundle)
    return bundle.uuid

  def update(self, uuid, metadata):
    bundle = self.model.get_bundle(uuid)
    self.model.update_bundle_metadata(bundle, metadata)

  def info(self, bundle_spec):
    uuid = self.get_spec_uuid(bundle_spec)
    bundle = self.model.get_bundle(uuid)
    return self.get_bundle_info(bundle)

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
