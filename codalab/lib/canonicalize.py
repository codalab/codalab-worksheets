'''
canonicalize provides helpers that convert ambiguous inputs to canonical forms:
  get_spec_uuid: bundle_spec [<uuid>|<name>] -> uuid
  get_target_path: target (bundle_spec, subpath) -> filesystem path

These methods are only available if we have direct access to the bundle system.
Converting a bundle spec to a uuid requires access to the bundle databases,
while getting the on-disk location of a target requires access to both the
database and the bundle store.
'''
from codalab.common import (
  precondition,
  State,
  UsageError,
)
from codalab.objects.bundle import Bundle
from codalab.bundles.uploaded_bundle import UploadedBundle
from codalab.lib import path_util
from codalab.model.util import LikeQuery


def get_spec_uuid(model, bundle_spec):
  '''
  Resolve a string bundle_spec to a unique bundle uuid.
  '''
  if not bundle_spec:
    raise UsageError('Tried to expand empty bundle_spec!')
  if Bundle.UUID_REGEX.match(bundle_spec):
    return bundle_spec
  elif Bundle.UUID_PREFIX_REGEX.match(bundle_spec):
    bundles = model.batch_get_bundles(uuid=LikeQuery(bundle_spec + '%'))
    message = "uuid starting with '%s'" % (bundle_spec,)
  elif UploadedBundle.NAME_REGEX.match(bundle_spec):
    bundles = model.search_bundles(name=bundle_spec)
    message = "name '%s'" % (bundle_spec,)
  else:
    raise UsageError(
      'Bundle names must match %s, was %s' %
      (UploadedBundle.NAME_REGEX.pattern, bundle_spec)
    )
  if not bundles:
    raise UsageError('No bundle found with %s' % (message,))
  elif len(bundles) > 1:
    raise UsageError(
      'Found multiple bundles with %s:%s' %
      (message, ''.join('\n  %s' % (bundle,) for bundle in bundles))
    )
  return bundles[0].uuid


def get_target_path(bundle_store, model, target):
  '''
  Return the on-disk location of the target (bundle_spec, path) pair.
  '''
  (bundle_spec, path) = target
  uuid = get_spec_uuid(model, bundle_spec)
  bundle = model.get_bundle(uuid)
  if not bundle.data_hash:
    message = 'Unexpected: %s is ready but it has no data hash!' % (bundle,)
    precondition(bundle.state != State.READY, message)
    if bundle.state == State.FAILED:
      raise UsageError('%s failed unrecoverably' % (bundle,))
    else:
      raise UsageError('%s has not yet been executed' % (bundle,))
  bundle_root = bundle_store.get_location(bundle.data_hash)
  final_path = path_util.safe_join(bundle_root, path)
  result = path_util.TargetPath(final_path)
  result.target = target
  return result
