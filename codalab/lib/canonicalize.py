'''
canonicalize provides helpers that convert ambiguous inputs to canonical forms:
  get_bundle_uuid: bundle_spec (which is <uuid>|<name>) -> uuid
  get_worksheet_uuid: worksheet_spec -> uuid
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

def get_bundle_uuid(model, worksheet_uuid, bundle_spec):
    '''
    Resolve a string bundle_spec to a bundle uuid.
    Types of specifications:
    - uuid: should be unique.
    - name[^[<index>]: there might be many uuids with this name.
    - ^[<index>], where index is the i-th (1-based) most recent element on the current worksheet.
    '''
    last_index = 1  # By default, take the last one
    if not bundle_spec:
        raise UsageError('Tried to expand empty bundle_spec!')
    if spec_util.UUID_REGEX.match(bundle_spec):
        return bundle_spec
    elif spec_util.UUID_PREFIX_REGEX.match(bundle_spec):
        bundle_uuids = model.get_bundle_uuids({'uuid': LikeQuery(bundle_spec + '%')}, max_results=1)
        message = "uuid starting with '%s'" % (bundle_spec,)
    else:
        m = spec_util.NAME_HISTORY_REGEX.match(bundle_spec)  # run^3: 3rd to last run
        if m:
            bundle_spec = m.group(1)
            last_index = int(m.group(2)) if m.group(2) != '' else 1
        else:
            m = spec_util.HISTORY_REGEX.match(bundle_spec)  # ^3: 3rd to last run in this worksheet
            if m:
                bundle_spec = None
                last_index = int(m.group(1)) if m.group(1) != '' else 1

        if bundle_spec:
            spec_util.check_name(bundle_spec)
        bundle_uuids = model.get_bundle_uuids({
            'name': LikeQuery(bundle_spec + '%') if bundle_spec else None,
            'worksheet_uuid': worksheet_uuid
        }, max_results=last_index)
        message = "name '%s'" % (bundle_spec,)
    if not bundle_uuids:
        # If fail to find something in the worksheet, then backoff to global
        if worksheet_uuid: return get_bundle_uuid(model, None, bundle_spec)
        raise UsageError('No bundle found with %s' % (message,))
    # Take the last bundle
    if last_index <= 0 or last_index > len(bundle_uuids):
        raise UsageError('Index %d out of range, only %d bundles matched' % (last_index, len(bundle_uuids)))
    return bundle_uuids[last_index - 1]

def get_current_location(bundle_store, uuid):
    '''
    Return the on-disk location of currently running target.
    '''
    return bundle_store.get_temp_location(uuid)

def get_target_path(bundle_store, model, target):
    '''
    Return the on-disk location of the target (bundle_uuid, subpath) pair.
    '''
    (uuid, path) = target
    bundle = model.get_bundle(uuid)
    if not bundle.data_hash:
        # Note that the bundle might not be done, but return the location anyway to the temporary directory
        bundle_root = get_current_location(bundle_store, uuid)
    else:
        bundle_root = bundle_store.get_location(bundle.data_hash)
    final_path = path_util.safe_join(bundle_root, path)

    # This is a bit restrictive because it means we can't follow symlinks to
    # other bundles arbitrarily, but it's safer.
    path_util.check_under_path(final_path, bundle_root)

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
