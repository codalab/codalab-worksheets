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
from codalab.lib import (
  path_util,
  spec_util,
)
from codalab.model.util import LikeQuery


def get_spec_uuid(model, bundle_spec):
    '''
    Resolve a string bundle_spec to a unique bundle uuid.
    '''
    if not bundle_spec:
        raise UsageError('Tried to expand empty bundle_spec!')
    if spec_util.UUID_REGEX.match(bundle_spec):
        return bundle_spec
    elif spec_util.UUID_PREFIX_REGEX.match(bundle_spec):
        bundles = model.batch_get_bundles(uuid=LikeQuery(bundle_spec + '%'))
        message = "uuid starting with '%s'" % (bundle_spec,)
    else:
        spec_util.check_name(bundle_spec)
        bundles = model.search_bundles(name=bundle_spec)
        message = "name '%s'" % (bundle_spec,)
    if not bundles:
        raise UsageError('No bundle found with %s' % (message,))
    elif len(bundles) > 1:
        raise UsageError(
          'Found multiple bundles with %s:%s' %
          (message, ''.join('\n  %s' % (bundle,) for bundle in bundles))
        )
    return bundles[0].uuid

def get_current_location(bundle_store, uuid):
    '''
    Return the on-disk location of currently running target.
    '''
    return bundle_store.get_temp_location(uuid)

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
        elif bundle.state == State.RUNNING:
            bundle_root = get_current_location(bundle_store, uuid)
        else:
            raise UsageError('%s isn\'t running yet!' % (bundle,))
    else:
        bundle_root = bundle_store.get_location(bundle.data_hash)

    final_path = path_util.safe_join(bundle_root, path)
    result = path_util.TargetPath(final_path)
    result.target = target
    return result


def get_worksheet_uuid(model, worksheet_spec):
    '''
    Resolve a string worksheet_spec to a unique worksheet uuid.
    '''
    if not worksheet_spec:
        raise UsageError('Tried to expand empty worksheet_spec!')
    if spec_util.UUID_REGEX.match(worksheet_spec):
        return worksheet_spec
    elif spec_util.UUID_PREFIX_REGEX.match(worksheet_spec):
        worksheets = model.batch_get_worksheets(uuid=LikeQuery(worksheet_spec + '%'))
        message = "uuid starting with '%s'" % (worksheet_spec,)
    else:
        spec_util.check_name(worksheet_spec)
        worksheets = model.batch_get_worksheets(name=worksheet_spec)
        message = "name '%s'" % (worksheet_spec,)
    if not worksheets:
        raise UsageError('No worksheet found with %s' % (message,))
    elif len(worksheets) > 1:
        raise UsageError(
          'Found multiple worksheets with %s:%s' %
          (message, ''.join('\n  %s' % (worksheet,) for worksheet in worksheets))
        )
    return worksheets[0].uuid
