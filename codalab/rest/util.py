"""
Helper functions for working with the BundleModel.
Most of these are adapted from the LocalBundleClient methods,
Placed in this central location to prevent circular imports.
"""
import httplib
import os
import re

from bottle import request, local
from codalab.objects.worksheet import Worksheet
from bottle import abort, get, post, delete, local, request, response
from marshmallow import (
    ValidationError,
    Schema as PlainSchema,
    validate,
    validates_schema,
    validates,
)
from marshmallow_jsonapi import Schema, fields

from codalab.bundles import PrivateBundle
from codalab.common import UsageError, PermissionError, NotFoundError
from codalab.lib import worksheet_util, spec_util, canonicalize, formatting
from codalab.model.tables import GROUP_OBJECT_PERMISSION_READ
from codalab.model.util import LikeQuery
from codalab.lib.server_util import (
    bottle_patch as patch,
    json_api_include,
    json_api_meta,
    query_get_bool,
    query_get_list,
    query_get_type,
)
from codalab.lib.spec_util import validate_uuid, validate_name
from codalab.lib.worksheet_util import WORKSHEET_ITEM_TYPES
from codalab.objects.permission import (
    check_worksheet_has_all_permission,
    check_worksheet_has_read_permission,
    parse_permission,
    PermissionSpec,
)
from codalab.objects.worksheet import Worksheet


#############################################################
# WORKSHEETS
#############################################################

def convert_items_from_db(items):
    """
    Helper function.
    (bundle_uuid, subworksheet_uuid, value, type) -> (bundle_info, subworksheet_info, value_obj, type)
    """
    # Database only contains the uuid; need to expand to info.
    # We need to do to convert the bundle_uuids into bundle_info dicts.
    # However, we still make O(1) database calls because we use the
    # optimized batch_get_bundles multiget method.
    bundle_uuids = set(
        bundle_uuid for (bundle_uuid, subworksheet_uuid, value, type) in items
        if bundle_uuid is not None
    )
    bundle_dict = get_bundle_infos(bundle_uuids)

    # Go through the items and substitute the components
    new_items = []
    for (bundle_uuid, subworksheet_uuid, value, type) in items:
        bundle_info = bundle_dict.get(bundle_uuid, {'uuid': bundle_uuid}) if bundle_uuid else None
        if subworksheet_uuid:
            try:
                subworksheet_info = local.model.get_worksheet(subworksheet_uuid, fetch_items=False).to_dict()
            except UsageError, e:
                # If can't get the subworksheet, it's probably invalid, so just replace it with an error
                # type = worksheet_util.TYPE_MARKUP
                subworksheet_info = {'uuid': subworksheet_uuid}
                # value = 'ERROR: non-existent worksheet %s' % subworksheet_uuid
        else:
            subworksheet_info = None
        value_obj = formatting.string_to_tokens(value) if type == worksheet_util.TYPE_DIRECTIVE else value
        new_items.append((bundle_info, subworksheet_info, value_obj, type))
    return new_items


def get_worksheet_info(uuid, fetch_items=False, fetch_permission=True):
    """
    The returned info object contains items which are (bundle_info, subworksheet_info, value_obj, type).
    """
    worksheet = local.model.get_worksheet_rest(uuid, fetch_items=fetch_items)
    check_worksheet_has_read_permission(local.model, request.user, worksheet)

    # Create the info by starting out with the metadata.
    result = worksheet.to_dict()

    if fetch_items:
        result['items'] = convert_items_from_db(result['items'])

    # Note that these group_permissions is universal and permissions are relative to the current user.
    # Need to make another database query.
    if fetch_permission:
        result['group_permissions'] = local.model.get_group_worksheet_permissions(
            request.user.user_id, worksheet.uuid)
        result['permission'] = local.model.get_user_worksheet_permissions(
            request.user.user_id, [worksheet.uuid], {worksheet.uuid: worksheet.owner_id}
        )[worksheet.uuid]

    return result


def check_worksheet_not_frozen(worksheet):
    if worksheet.frozen:
        raise PermissionError('Cannot mutate frozen worksheet %s(%s).' % (worksheet.uuid, worksheet.name))


# TODO(sckoo): fix
def populate_dashboard(worksheet):
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../objects/dashboard.ws')
    lines = [line.rstrip() for line in open(file_path, 'r').readlines()]
    items, commands = worksheet_util.parse_worksheet_form(lines, self, worksheet.uuid)
    info = self.get_worksheet_info(worksheet.uuid, True)
    self.update_worksheet_items(info, items)
    self.update_worksheet_metadata(worksheet.uuid, {'title': 'Codalab Dashboard'})


def get_worksheet_uuid_or_none(base_worksheet_uuid, worksheet_spec):
    """
    Helper: Return the uuid of the specified worksheet if it exists. Otherwise, return None.
    """
    try:
        return canonicalize.get_worksheet_uuid(local.model, base_worksheet_uuid, worksheet_spec)
    except UsageError:
        return None


def ensure_unused_worksheet_name(name):
    # Ensure worksheet names are unique.  Note: for simplicity, we are
    # ensuring uniqueness across the system, even on worksheet names that
    # the user may not have access to.

    # If trying to set the name to a home worksheet, then it better be
    # user's home worksheet.
    if spec_util.is_home_worksheet(name) and spec_util.home_worksheet(request.user.user_name) != name:
        raise UsageError('Cannot create %s because this is potentially the home worksheet of another user' % name)
    if get_worksheet_uuid_or_none(None, name) is not None:
        raise UsageError('Worksheet with name %s already exists' % name)


def set_worksheet_perm(worksheet_uuid, group_spec, permission_spec):
    """
    Give the given |group_spec| the desired |permission_spec| on |worksheet_uuid|.
    """
    worksheet = local.model.get_worksheet(worksheet_uuid, fetch_items=False)
    check_worksheet_has_all_permission(local.model, request.user.user_id, worksheet)
    group_info = get_group_info(group_spec, need_admin=False)
    old_permission = local.model.get_group_worksheet_permission(group_info['uuid'], worksheet.uuid)
    new_permission = parse_permission(permission_spec)

    if new_permission > 0:
        if old_permission > 0:
            local.model.update_worksheet_permission(group_info['uuid'], worksheet.uuid, new_permission)
        else:
            local.model.add_worksheet_permission(group_info['uuid'], worksheet.uuid, new_permission)
    else:
        if old_permission > 0:
            local.model.delete_worksheet_permission(group_info['uuid'], worksheet.uuid)
    return {'worksheet': {'uuid': worksheet.uuid, 'name': worksheet.name},
            'group_info': group_info,
            'permission': new_permission}


def new_worksheet(name):
    """
    Create a new worksheet with the given |name|.
    """
    ensure_unused_worksheet_name(name)

    # Don't need any permissions to do this.
    worksheet = Worksheet({
        'name': name,
        'title': None,
        'frozen': None,
        'items': [],
        'owner_id': request.user.user_id
    })
    local.model.new_worksheet(worksheet)

    # Make worksheet publicly readable by default
    set_worksheet_perm(worksheet.uuid, local.model.public_group_uuid, 'read')
    if spec_util.is_dashboard(name):
        # FIXME
        pass
        # self.populate_dashboard(worksheet)
    return worksheet.uuid


def get_worksheet_uuid(base_worksheet_uuid, worksheet_spec):
    """
    Return the uuid of the specified worksheet if it exists.
    If not, create a new worksheet if the specified worksheet is home_worksheet or dashboard. Otherwise, throw an error.
    """
    if worksheet_spec == '' or worksheet_spec == worksheet_util.HOME_WORKSHEET:
        worksheet_spec = spec_util.home_worksheet(request.user.user_id)
    worksheet_uuid = get_worksheet_uuid_or_none(base_worksheet_uuid, worksheet_spec)
    if worksheet_uuid is not None:
        return worksheet_uuid
    else:
        if spec_util.is_home_worksheet(worksheet_spec) or spec_util.is_dashboard(worksheet_spec):
            return new_worksheet(worksheet_spec)
        else:
            # let it throw the correct error message
            return canonicalize.get_worksheet_uuid(local.model, base_worksheet_uuid, worksheet_spec)


#############################################################
#  BUNDLES
#############################################################


def resolve_bundle_specs(worksheet_uuid, bundle_specs):
    return [resolve_bundle_spec(worksheet_uuid, bundle_spec)
            for bundle_spec in bundle_specs]


def resolve_bundle_spec(worksheet_uuid, bundle_spec):
    if '/' in bundle_spec:  # <worksheet_spec>/<bundle_spec>
        # Shift to new worksheet
        worksheet_spec, bundle_spec = bundle_spec.split('/', 1)
        worksheet_uuid = get_worksheet_uuid(worksheet_uuid, worksheet_spec)

    return canonicalize.get_bundle_uuid(local.model, request.user.user_id,
                                        worksheet_uuid, bundle_spec)


def mask_bundle(bundle_info):
    """
    Return a copy of the bundle_info dict that hides all fields except 'uuid'.
    """
    return {
        'uuid': bundle_info['uuid'],
        'bundle_type': PrivateBundle.BUNDLE_TYPE,
        'owner_id': None,
        'command': None,
        'data_hash': None,
        'state': None,
        'metadata': [{
            'metadata_key': 'name',
            'metadata_value': '<private>',
        }],
        'dependencies': [],
    }


def get_bundle_infos(uuids, get_children=False, get_host_worksheets=False, get_permissions=False):
    """
    get_children, get_host_worksheets, get_permissions:
        whether we want to return more detailed information.
    Return map from bundle uuid to info.
    """
    if len(uuids) == 0:
        return {}
    bundles = local.model.batch_get_bundles_rest(uuid=uuids)
    bundle_dict = {bundle['uuid']: bundle for bundle in bundles}

    # Filter out bundles that we don't have read permission on
    def select_unreadable_bundles(uuids):
        permissions = local.model.get_user_bundle_permissions(request.user.user_id, uuids, local.model.get_bundle_owner_ids(uuids))
        return [uuid for uuid, permission in permissions.items() if permission < GROUP_OBJECT_PERMISSION_READ]

    def select_unreadable_worksheets(uuids):
        permissions = local.model.get_user_worksheet_permissions(request.user.user_id, uuids, local.model.get_worksheet_owner_ids(uuids))
        return [uuid for uuid, permission in permissions.items() if permission < GROUP_OBJECT_PERMISSION_READ]

    # Mask bundles that we can't access
    for uuid in select_unreadable_bundles(uuids):
        if uuid in bundle_dict:
            bundle_dict[uuid] = mask_bundle(bundle_dict[uuid])

    if get_children:
        result = local.model.get_children_uuids(uuids)
        # Gather all children bundle uuids
        children_uuids = [uuid for l in result.values() for uuid in l]
        unreadable = set(select_unreadable_bundles(children_uuids))
        for uuid, info in bundle_dict.items():
            info['children'] = [child_uuid for child_uuid in result[uuid]
                                if child_uuid not in unreadable]

    if get_host_worksheets:
        # bundle_uuids -> list of worksheet_uuids
        result = local.model.get_host_worksheet_uuids(uuids)
        # Gather all worksheet uuids
        worksheet_uuids = [uuid for l in result.values() for uuid in l]
        unreadable = set(select_unreadable_worksheets(worksheet_uuids))
        worksheet_uuids = [uuid for uuid in worksheet_uuids if uuid not in unreadable]
        # Lookup names
        worksheets = dict(
            (worksheet.uuid, worksheet)
            for worksheet in local.model.batch_get_worksheets(
                fetch_items=False,
                uuid=worksheet_uuids))
        # Fill the info
        for uuid, info in bundle_dict.items():
            info['host_worksheets'] = [
                {
                    'uuid': worksheet_uuid,
                    'name': worksheets[worksheet_uuid].name
                }
                for worksheet_uuid in result[uuid]
                if worksheet_uuid not in unreadable]

    if get_permissions:
        # Fill the info
        group_result = local.model.batch_get_group_bundle_permissions(request.user.user_id, uuids)
        result = local.model.get_user_bundle_permissions(request.user.user_id, uuids, local.model.get_bundle_owner_ids(uuids))
        for uuid, info in bundle_dict.items():
            info['group_permissions'] = group_result[uuid]
            info['permission'] = result[uuid]

    return bundle_dict


def resolve_owner_in_keywords(keywords):
    # Resolve references to owner ids
    def resolve(keyword):
        # Example: owner=codalab => owner_id=0
        m = re.match('owner=(.+)', keyword)
        if not m:
            return keyword
        return 'owner_id=%s' % getattr(local.model.get_user(username=m.group(1)), 'user_id', 'x')
    return map(resolve, keywords)


#############################################################
# GROUPS
#############################################################


def ensure_unused_group_name(name):
    """
    Ensure group names are unique.  Note: for simplicity, we are
    ensuring uniqueness across the system, even on group names that
    the user may not have access to.
    """
    groups = local.model.batch_get_groups(name=name)
    if len(groups) != 0:
        abort(httplib.CONFLICT, 'Group with name %s already exists' % name)


def unique_group(model, group_spec, user_id):
    """
    Return a group_info corresponding to |group_spec|.
    If |user_id| is given, only search only group that the user is involved in
    (either as an owner or just as a regular member).
    Otherwise, search all groups (this happens when we're root).
    """
    def search_all(model, **spec_filters):
        return model.batch_get_groups(**spec_filters)
    def search_user(model, **spec_filters):
        return model.batch_get_all_groups(
            spec_filters,
            {'owner_id': user_id, 'user_defined': True},
            {'user_id': user_id})
    if user_id == None:
        search = search_all
    else:
        search = search_user
    return get_single_group(model, group_spec, search)


def get_single_group(model, group_spec, search_fn):
    """
    Helper function.
    Resolve a string group_spec to a unique group for the given |search_fn|.
    Throw an error if zero or more than one group matches.
    """
    if not group_spec:
        raise UsageError('Tried to expand empty group_spec!')
    if spec_util.UUID_REGEX.match(group_spec):
        groups = search_fn(model, uuid=group_spec)
        message = "uuid starting with '%s'" % (group_spec,)
    elif spec_util.UUID_PREFIX_REGEX.match(group_spec):
        groups = search_fn(model, uuid=LikeQuery(group_spec + '%'))
        message = "uuid starting with '%s'" % (group_spec,)
    else:
        groups = search_fn(model, name=group_spec)
        message = "name '%s'" % (group_spec,)
    if not groups:
        raise NotFoundError('Found no group with %s' % (message,))
    elif len(groups) > 1:
        raise UsageError(
            'Found multiple groups with %s:%s' %
            (message, ''.join('\n  uuid=%s' % (group['uuid'],) for group in groups))
        )
    return groups[0]


def get_group_info(group_spec, need_admin):
    """
    Resolve |group_spec| and return the associated group_info.
    """
    user_id = request.user.user_id

    # If we're root, then we can access any group.
    if user_id == local.model.root_user_id:
        user_id = None

    group_info = unique_group(local.model, group_spec, user_id)

    # If not root and need admin access, but don't have it, raise error.
    if user_id and need_admin and not group_info['is_admin'] and user_id != group_info['owner_id']:
        abort(httplib.FORBIDDEN, 'You are not the admin of group %s.' % group_spec)

    # No one can admin the public group (not even root), because it's a special group.
    if need_admin and group_info['uuid'] == local.model.public_group_uuid:
        abort(httplib.FORBIDDEN, 'Cannot modify the public group %s.' % group_spec)

    group_info['memberships'] = local.model.batch_get_user_in_group(group_uuid=group_info['uuid'])

    return group_info

