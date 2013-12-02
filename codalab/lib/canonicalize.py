'''
Helper functions that convert ambiguous inputs into canonical forms:
  bundle_spec [<uuid>|<name>] -> uuid
  target (bundle_spec, subpath) -> filesystem path
'''
import os
import re

from codalab.common import (
  State,
  UsageError,
)
from codalab.objects.bundle import Bundle
from codalab.bundles.uploaded_bundle import UploadedBundle


def get_spec_uuid(model, bundle_spec):
  '''
  Resolve a string bundle_spec to a unique bundle uuid.
  '''
  if not bundle_spec:
    raise UsageError('Tried to expand empty bundle_spec!')
  if re.match(Bundle.UUID_REGEX, bundle_spec):
    return bundle_spec
  elif not re.match(UploadedBundle.NAME_REGEX, bundle_spec):
    raise UsageError(
      "Bundle names should match '%s', was '%s'" %
      (UploadedBundle.NAME_REGEX, bundle_spec)
    )
  bundles = model.search_bundles(name=bundle_spec)
  if not bundles:
    raise UsageError("No bundle found with name '%s'" % (bundle_spec,))
  elif len(bundles) > 1:
    raise UsageError(
      "Found multiple bundles with name '%s':%s" %
      (bundle_spec, ''.join('\n  %s' % (bundle,) for bundle in bundles))
    )
  return bundles[0].uuid


def get_target_path(bundle_store, model, target):
  '''
  Return the on-disk location of the target (bundle_spec, path) pair.
  '''
  (bundle_spec, path) = target
  uuid = get_spec_uuid(model, bundle_spec)
  bundle = model.get_bundle(uuid)
  if bundle.state != State.READY:
    raise UsageError('%s is not ready' % (bundle,))
  bundle_root = bundle_store.get_location(bundle.data_hash)
  final_path = os.path.join(bundle_root, path)
  return final_path
