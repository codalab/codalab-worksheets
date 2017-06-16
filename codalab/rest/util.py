"""
Helper functions for working with the BundleModel.
Some functions placed in this central location to prevent circular imports.
"""
import httplib
import re

from bottle import abort, local, request

from codalab.bundles import PrivateBundle
from codalab.lib import bundle_util
from codalab.model.tables import (
    GROUP_OBJECT_PERMISSION_ALL,
    GROUP_OBJECT_PERMISSION_NONE,
    GROUP_OBJECT_PERMISSION_READ,
)
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

# Placed in this module to prevent cyclic imports between rest.bundles and rest.worksheets
def get_bundle_infos(uuids, get_children=False, get_host_worksheets=False, get_permissions=False, ignore_not_found=True):
    """
    Return a map from bundle uuid to info.

    :param Collection[str] uuids: uuids of bundles to fetch
    :param bool get_children: include children
    :param bool get_host_worksheets: include host worksheets
    :param bool get_permissions: include group permissions
    :param bool ignore_not_found: abort with 404 NOT FOUND when False and bundle doesn't exist
    :rtype: dict[str, dict]
    """
    if len(uuids) == 0:
        return {}
    bundles = local.model.batch_get_bundles(uuid=uuids)
    bundle_infos = {bundle.uuid: bundle_util.bundle_to_bundle_info(local.model, bundle) for bundle in bundles}

    # Implement permissions policies
    perms = _get_user_bundle_permissions(uuids)
    readable = {u for u, perm in perms.iteritems() if perm >= GROUP_OBJECT_PERMISSION_READ}
    anonymous = {u for u, perm in perms.iteritems() if u in bundle_infos and (perm < GROUP_OBJECT_PERMISSION_READ or bundle_infos[u]['is_anonymous'])}
    for uuid in uuids:
        bundle = bundle_infos.get(uuid)
        # Bundle doesn't exist; abort or skip
        if bundle is None:
            if ignore_not_found:
                continue
            else:
                abort(httplib.NOT_FOUND, "Bundle %s not found" % uuid)
        # Replace bundles that the user does not have read access to
        elif uuid not in readable:
            bundle_infos[uuid] = bundle_util.bundle_to_bundle_info(local.model, PrivateBundle.construct(uuid))
        # Mask owners of anonymous bundles that user does not have all acccess to
        elif uuid in anonymous:
            bundle['owner_id'] = None

        # Set permission
        bundle['permission'] = perms[uuid]

    if get_children:
        parent2children = local.model.get_children_uuids(readable)

        # Gather all children bundle uuids and fetch permissions
        child_uuids = [uuid for l in parent2children.values() for uuid in l]
        child_perms = _get_user_bundle_permissions(child_uuids)

        # Lookup bundle names
        child_names = local.model.get_bundle_names(child_uuids)

        # Set children infos
        for parent_uuid, children in parent2children.iteritems():
            bundle_infos[parent_uuid]['children'] = [
                {
                    'uuid': child_uuid,
                    'metadata': {'name': child_names[child_uuid]}
                }
                for child_uuid in children
                if child_perms[child_uuid] >= GROUP_OBJECT_PERMISSION_READ]

    if get_host_worksheets:
        # bundle_uuids -> list of worksheet_uuids
        host_worksheets = local.model.get_host_worksheet_uuids(readable)
        # Gather all worksheet uuids
        worksheet_uuids = [uuid for l in host_worksheets.itervalues() for uuid in l]
        wpermissions = local.model.get_user_worksheet_permissions(
            request.user.user_id, worksheet_uuids, local.model.get_worksheet_owner_ids(worksheet_uuids))
        readable_worksheet_uuids = set(uuid for uuid, permission in wpermissions.iteritems()
                                       if permission >= GROUP_OBJECT_PERMISSION_READ)
        # Lookup names
        worksheets = dict(
            (worksheet.uuid, worksheet)
            for worksheet in local.model.batch_get_worksheets(
                fetch_items=False,
                uuid=readable_worksheet_uuids))
        # Fill the info
        for bundle_uuid, host_uuids in host_worksheets.iteritems():
            bundle_infos[bundle_uuid]['host_worksheets'] = [
                {
                    'uuid': host_uuid,
                    'name': worksheets[host_uuid].name
                }
                for host_uuid in host_uuids
                if host_uuid in readable_worksheet_uuids]

    if get_permissions:
        # Fill the permissions info
        bundle2group_perms = local.model.batch_get_group_bundle_permissions(
                request.user.user_id, readable)
        for uuid, group_perms in bundle2group_perms.items():
            # Only show group permissions to the user is they have
            # at least read permission on this bundle.
            if uuid in anonymous:
                bundle_infos[uuid]['group_permissions'] = []
            else:
                bundle_infos[uuid]['group_permissions'] = group_perms

    return bundle_infos


def _get_user_bundle_permissions(uuids):
    return local.model.get_user_bundle_permissions(
        request.user.user_id, uuids, local.model.get_bundle_owner_ids(uuids))


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

