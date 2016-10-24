"""
canonicalize provides helpers that convert ambiguous inputs to canonical forms:
  get_bundle_uuid: bundle_spec (which is <uuid>|<name>) -> uuid
  get_worksheet_uuid: worksheet_spec -> uuid

These methods are only available if we have direct access to the bundle system.
Converting a bundle spec to a uuid requires access to the bundle databases,
while getting the on-disk location of a target requires access to both the
database and the bundle store.
"""
from codalab.common import (
  NotFoundError,
  UsageError,
)
from codalab.lib import (
  spec_util,
)
from codalab.model.util import LikeQuery


HOME_WORKSHEET = '/'


def _parse_relative_bundle_spec(bundle_spec):
    """
    Parse bundle spec "BUNDLESPEC^I" into ("BUNDLE_SPEC", I).

    :param bundle_spec: string bundle spec
    :return: (base bundle spec, reverse history index)
    """
    # run: bundle whose name starts with foo
    m = spec_util.NAME_PATTERN_REGEX.match(bundle_spec)
    if m:
        bundle_spec = m.group(1)
        reverse_index = 1
        return (bundle_spec, reverse_index)

    # foo^3: 3rd to last bundle whose name starts with foo
    m = spec_util.NAME_PATTERN_HISTORY_REGEX.match(bundle_spec)
    if m:
        bundle_spec = m.group(1)
        reverse_index = int(m.group(2)) if m.group(2) != '' else 1
        return (bundle_spec, reverse_index)

    # ^3: 3rd to last bundle whose name starts with foo in this worksheet
    m = spec_util.HISTORY_REGEX.match(bundle_spec)
    if m:
        bundle_spec = None
        reverse_index = int(m.group(1)) if m.group(1) != '' else 1
        return (bundle_spec, reverse_index)

    raise UsageError('Invalid bundle_spec: %s' % bundle_spec)


def get_bundle_uuid(model, user, worksheet_uuid, bundle_spec):
    """
    Resolve a string bundle_spec to a bundle uuid.
    Types of specifications:
    - uuid: should be unique.
    - name[^[<index>]: there might be many uuids with this name.
    - ^[<index>], where index is the i-th (1-based) most recent element on the current worksheet.
    Specification can also be prefixed with a base worksheet spec and a slash:
    - <worksheet_spec>/<bundle_spec>
    """
    bundle_spec = bundle_spec.strip()
    user_id = user and user.user_id
    if not bundle_spec:
        raise UsageError('Tried to expand empty bundle_spec!')

    if '/' in bundle_spec:  # <worksheet_spec>/<bundle_spec>
        # Shift to new worksheet
        worksheet_spec, bundle_spec = bundle_spec.split('/', 1)
        worksheet_uuid = get_worksheet_uuid(model, user, worksheet_uuid, worksheet_spec)

    if spec_util.UUID_REGEX.match(bundle_spec):
        return bundle_spec
    elif spec_util.UUID_PREFIX_REGEX.match(bundle_spec):
        bundle_uuids = model.get_bundle_uuids({
            'uuid': LikeQuery(bundle_spec + '%'),
            'user_id': user_id,
        }, max_results=2)
        if len(bundle_uuids) == 0:
            raise NotFoundError('uuid prefix %s doesn\'t match any bundles' % bundle_spec)
        elif len(bundle_uuids) == 1:
            return bundle_uuids[0]
        else:
            raise UsageError('uuid prefix %s more than one bundle' % bundle_spec)
    else:
        bundle_spec, reverse_index = _parse_relative_bundle_spec(bundle_spec)

        if bundle_spec:
            bundle_spec = bundle_spec.replace('.*', '%')  # Convert regular expression syntax to SQL syntax
            if '%' in bundle_spec:
                bundle_spec_query = LikeQuery(bundle_spec)
            else:
                bundle_spec_query = bundle_spec
        else:
            bundle_spec_query = None

        # query results are ordered from newest to old
        bundle_uuids = model.get_bundle_uuids({
            'name': bundle_spec_query,
            'worksheet_uuid': worksheet_uuid,
            'user_id': user_id,
        }, max_results=reverse_index)

    # Take the last bundle
    if reverse_index <= 0 or reverse_index > len(bundle_uuids):
        if bundle_spec is None:
            raise UsageError('%d bundles, index %d out of bounds' %
                             (len(bundle_uuids), reverse_index))
        elif len(bundle_uuids) == 0:
            raise NotFoundError('bundle spec %s doesn\'t match any bundles' % bundle_spec)
        else:
            raise UsageError('bundle spec %s matches %d bundles, index %d out of bounds' %
                             (bundle_spec, len(bundle_uuids), reverse_index))

    return bundle_uuids[reverse_index - 1]


def get_bundle_uuids(model, user, worksheet_uuid, bundle_specs):
    """
    Convenience function for resolving more than one bundle spec in one call.
    """
    return [get_bundle_uuid(model, user, worksheet_uuid, spec) for spec in bundle_specs]


def get_worksheet_uuid(model, user, base_worksheet_uuid, worksheet_spec):
    """
    Resolve a string worksheet_spec to a unique worksheet uuid.
    If base_worksheet_uuid specified, then try to resolve worksheet_spec in the
    context of base_worksheet_uuid.
    """
    worksheet_spec = worksheet_spec.strip()
    if (worksheet_spec == '' or worksheet_spec == HOME_WORKSHEET) and user:
        worksheet_spec = spec_util.home_worksheet(user.user_name)
    if not worksheet_spec:
        raise UsageError('Tried to expand empty worksheet_spec!')
    if spec_util.UUID_REGEX.match(worksheet_spec):
        return worksheet_spec

    if spec_util.UUID_PREFIX_REGEX.match(worksheet_spec):
        worksheets = model.batch_get_worksheets(fetch_items=False, uuid=LikeQuery(worksheet_spec + '%'),
                                                base_worksheet_uuid=base_worksheet_uuid)
        message = "uuid starting with '%s'" % (worksheet_spec,)
    else:
        spec_util.check_name(worksheet_spec)
        worksheets = model.batch_get_worksheets(fetch_items=False, name=worksheet_spec,
                                                base_worksheet_uuid=base_worksheet_uuid)
        message = "name '%s'" % (worksheet_spec,)

    if not worksheets:
        raise NotFoundError('No worksheet found with %s' % (message,))
    if len(worksheets) > 1:
        raise UsageError(
          'Found multiple worksheets with %s:%s' %
          (message, ''.join('\n  %s' % (worksheet,) for worksheet in worksheets))
        )

    return worksheets[0].uuid
