"""
Helper functions for working with the BundleModel.
Some functions placed in this central location to prevent circular imports.
"""
import httplib
import re

from bottle import abort, local, request

from codalab.bundles import PrivateBundle
from codalab.lib import bundle_util
from codalab.model.tables import GROUP_OBJECT_PERMISSION_READ
from codalab.objects.permission import (
    check_bundles_have_read_permission,
    unique_group,
)


def get_resource_ids(document, type_):
    links = document['data']
    if not isinstance(links, list):
        links = [links]
    if any(link['type'] != type_ for link in links):
        raise abort(httplib.BAD_REQUEST, 'type must be %r' % type_)
    return [link['id'] for link in links]


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
# BUNDLES
#############################################################


def get_bundle_info(uuid, get_children=False, get_host_worksheets=False, get_permissions=False):
    return get_bundle_infos([uuid], get_children, get_host_worksheets, get_permissions).get(uuid)


# Placed here to prevent cyclic imports between rest.bundles and rest.worksheets
def get_bundle_infos(uuids, get_children=False, get_host_worksheets=False, get_permissions=False):
    """
    get_children, get_host_worksheets, get_permissions:
        whether we want to return more detailed information.
    Return map from bundle uuid to info.
    """
    if len(uuids) == 0:
        return {}
    bundles = local.model.batch_get_bundles(uuid=uuids)
    bundle_dict = {bundle.uuid: bundle_util.bundle_to_bundle_info(local.model, bundle) for bundle in bundles}

    # Filter out bundles that we don't have read permission on
    def select_unreadable_bundles(uuids):
        permissions = local.model.get_user_bundle_permissions(request.user.user_id, uuids, local.model.get_bundle_owner_ids(uuids))
        return [uuid for uuid, permission in permissions.items() if permission < GROUP_OBJECT_PERMISSION_READ]

    def select_unreadable_worksheets(uuids):
        permissions = local.model.get_user_worksheet_permissions(request.user.user_id, uuids, local.model.get_worksheet_owner_ids(uuids))
        return [uuid for uuid, permission in permissions.items() if permission < GROUP_OBJECT_PERMISSION_READ]

    # Lookup the user names of all the owners
    user_ids = [info['owner_id'] for info in bundle_dict.values()]
    users = local.model.get_users(user_ids=user_ids) if len(user_ids) > 0 else []
    users = {u.user_id: u for u in users}
    if users:
        for info in bundle_dict.values():
            user = users[info['owner_id']]
            info['owner_name'] = user.user_name if user else None
            info['owner'] = '%s(%s)' % (info['owner_name'], info['owner_id'])

    # Mask bundles that we can't access
    for uuid in select_unreadable_bundles(uuids):
        if uuid in bundle_dict:
            bundle_dict[uuid] = bundle_util.bundle_to_bundle_info(local.model, PrivateBundle.construct(uuid))

    if get_children:
        result = local.model.get_children_uuids(uuids)
        # Gather all children bundle uuids
        children_uuids = [uuid for l in result.values() for uuid in l]
        unreadable = set(select_unreadable_bundles(children_uuids))
        # Lookup bundle names
        names = local.model.get_bundle_names(children_uuids)
        # Fill in info
        for uuid, info in bundle_dict.items():
            info['children'] = [
                {
                    'uuid': child_uuid,
                    'metadata': {'name': names[child_uuid]}
                }
                for child_uuid in result[uuid] if child_uuid not in unreadable]

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
        # Fill the permissions info
        group_perms = local.model.batch_get_group_bundle_permissions(
                request.user.user_id, uuids)
        user_perm = local.model.get_user_bundle_permissions(
                request.user.user_id, uuids, local.model.get_bundle_owner_ids(uuids))
        for uuid, info in bundle_dict.items():
            info['permission'] = user_perm[uuid]
            # Only show group permissions to the user is they have
            # at least read permission on this bundle.
            if user_perm[uuid] >= GROUP_OBJECT_PERMISSION_READ:
                info['group_permissions'] = group_perms[uuid]
            else:
                info['group_permissions'] = []

    return bundle_dict


def check_target_has_read_permission(target):
    check_bundles_have_read_permission(local.model, request.user, [target[0]])


def get_target_info(target, depth):
    """
    Returns information about an individual target inside the bundle, or
    None if the target doesn't exist.
    """
    check_target_has_read_permission(target)
    return local.download_manager.get_target_info(target[0], target[1], depth)


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


def get_group_info(group_spec, need_admin, access_all_groups=False):
    """
    Resolve |group_spec| and return the associated group_info.
    """
    user_id = request.user.user_id

    # If we're root, then we can access any group.
    if user_id == local.model.root_user_id or access_all_groups:
        user_id = None

    group_info = unique_group(local.model, group_spec, user_id)

    # If not root and need admin access, but don't have it, raise error.
    if user_id and need_admin and not group_info['is_admin'] and user_id != group_info['owner_id']:
        abort(httplib.FORBIDDEN, 'You are not the admin of group %s.' % group_spec)

    # No one can admin the public group (not even root), because it's a special group.
    if need_admin and group_info['uuid'] == local.model.public_group_uuid:
        abort(httplib.FORBIDDEN, 'Cannot modify the public group %s.' % group_spec)

    return group_info

