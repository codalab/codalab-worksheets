"""
Helper functions for working with the BundleModel.
Some functions placed in this central location to prevent circular imports.
"""
import http.client
import re

from bottle import abort, local, request

from codalab.bundles import PrivateBundle
from codalab.lib import bundle_util
from codalab.model.tables import GROUP_OBJECT_PERMISSION_READ
from codalab.objects.permission import check_bundles_have_read_permission, unique_group


def get_resource_ids(document, type_):
    links = document['data']
    if not isinstance(links, list):
        links = [links]
    if any(link['type'] != type_ for link in links):
        raise abort(http.client.BAD_REQUEST, 'type must be %r' % type_)
    return [link['id'] for link in links]


def resolve_owner_in_keywords(keywords):
    # Resolve references to owner ids
    def resolve(keyword):
        # Example: owner=codalab => owner_id=0
        m = re.match('owner=(.+)', keyword)
        if not m:
            return keyword
        return 'owner_id=%s' % getattr(local.model.get_user(username=m.group(1)), 'user_id', 'x')

    return list(map(resolve, keywords))


#############################################################
# BUNDLES
#############################################################

# Placed in this module to prevent cyclic imports between rest.bundles and rest.worksheets
def get_bundle_infos(
    uuids,
    get_children=False,
    get_single_host_worksheet=False,
    get_host_worksheets=False,
    get_permissions=False,
    ignore_not_found=True,
    model=None,
):
    """
    Return a map from bundle uuid to info.

    :param Collection[str] uuids: uuids of bundles to fetch
    :param bool get_children: include children
    :param bool get_single_host_worksheet: include one host_worksheet per bundle uuid
    :param bool get_host_worksheets: include all host worksheets
    :param bool get_permissions: include group permissions
    :param bool ignore_not_found: abort with 404 NOT FOUND when False and bundle doesn't exist
    :param BundleModel model: model used to make database queries
    :rtype: dict[str, dict]
    """
    if model is None:
        model = local.model
    if len(uuids) == 0:
        return {}
    bundles = model.batch_get_bundles(uuid=uuids)
    bundle_infos = {
        bundle.uuid: bundle_util.bundle_to_bundle_info(model, bundle) for bundle in bundles
    }

    # Implement permissions policies
    perms = _get_user_bundle_permissions(model, uuids)
    readable = {u for u, perm in perms.items() if perm >= GROUP_OBJECT_PERMISSION_READ}
    anonymous = {
        u
        for u, perm in perms.items()
        if u in bundle_infos
        and (perm < GROUP_OBJECT_PERMISSION_READ or bundle_infos[u]['is_anonymous'])
    }
    for uuid in uuids:
        bundle = bundle_infos.get(uuid)
        # Bundle doesn't exist; abort or skip
        if bundle is None:
            if ignore_not_found:
                continue
            else:
                abort(http.client.NOT_FOUND, "Bundle %s not found" % uuid)
        # Replace bundles that the user does not have read access to
        elif uuid not in readable:
            bundle_infos[uuid] = bundle_util.bundle_to_bundle_info(
                model, PrivateBundle.construct(uuid)
            )
        # Mask owners of anonymous bundles that user does not have all access to
        elif uuid in anonymous:
            bundle['owner_id'] = None

        # Set permission
        bundle['permission'] = perms[uuid]

    if get_children:
        parent2children = model.get_children_uuids(readable)

        # Gather all children bundle uuids and fetch permissions
        child_uuids = [uuid for v in parent2children.values() for uuid in v]
        child_perms = _get_user_bundle_permissions(model, child_uuids)

        # Lookup bundle names
        child_names = model.get_bundle_names(child_uuids)

        # Set children infos
        for parent_uuid, children in parent2children.items():
            bundle_infos[parent_uuid]['children'] = [
                {'uuid': child_uuid, 'metadata': {'name': child_names[child_uuid]}}
                for child_uuid in children
                if child_perms[child_uuid] >= GROUP_OBJECT_PERMISSION_READ
            ]

    if get_single_host_worksheet:
        # Query for 5 worksheet uuids per bundle to check the read permissions for, since we
        # just need a single host worksheet per bundle uuid. This is much faster than fetching all
        # worksheet uuid's per bundle.
        host_worksheets = model.get_host_worksheet_uuids(readable, 5)
        worksheet_uuids = [uuid for v in host_worksheets.values() for uuid in v]
        worksheet_names = _get_readable_worksheet_names(model, worksheet_uuids)

        for bundle_uuid, host_uuids in host_worksheets.items():
            if bundle_uuid not in bundle_infos:
                continue
            for host_uuid in host_uuids:
                if host_uuid in worksheet_names:
                    bundle_infos[bundle_uuid]['host_worksheet'] = {
                        'uuid': host_uuid,
                        'name': worksheet_names[host_uuid],
                    }
                    # Just set a single host worksheet per bundle uuid
                    break

    if get_host_worksheets:
        host_worksheets = model.get_all_host_worksheet_uuids(readable)
        # Gather all worksheet uuids
        worksheet_uuids = [uuid for v in host_worksheets.values() for uuid in v]
        worksheet_names = _get_readable_worksheet_names(model, worksheet_uuids)

        # Fill the info
        for bundle_uuid, host_uuids in host_worksheets.items():
            if bundle_uuid not in bundle_infos:
                continue
            bundle_infos[bundle_uuid]['host_worksheets'] = [
                {'uuid': host_uuid, 'name': worksheet_names[host_uuid]}
                for host_uuid in host_uuids
                if host_uuid in worksheet_names
            ]

    if get_permissions:
        # Fill the permissions info
        bundle2group_perms = model.batch_get_group_bundle_permissions(
            request.user.user_id, readable
        )
        for uuid, group_perms in bundle2group_perms.items():
            # Only show group permissions to the user is they have
            # at least read permission on this bundle.
            if uuid in anonymous:
                bundle_infos[uuid]['group_permissions'] = []
            else:
                bundle_infos[uuid]['group_permissions'] = group_perms

    return bundle_infos


def _get_user_bundle_permissions(model, uuids):
    return model.get_user_bundle_permissions(
        request.user.user_id, uuids, model.get_bundle_owner_ids(uuids)
    )


def _get_readable_worksheet_names(model, worksheet_uuids):
    # Returns a dictionary of readable worksheet uuid's as keys and corresponding names as values
    readable_worksheet_uuids = _filter_readable_worksheet_uuids(model, worksheet_uuids)
    return dict(
        (worksheet.uuid, worksheet.name)
        for worksheet in model.batch_get_worksheets(
            fetch_items=False, uuid=readable_worksheet_uuids
        )
    )


def _filter_readable_worksheet_uuids(model, worksheet_uuids):
    # Returns a set of worksheet uuid's the user has read permission for
    worksheet_permissions = model.get_user_worksheet_permissions(
        request.user.user_id, worksheet_uuids, model.get_worksheet_owner_ids(worksheet_uuids)
    )
    return set(
        uuid
        for uuid, permission in worksheet_permissions.items()
        if permission >= GROUP_OBJECT_PERMISSION_READ
    )


def check_target_has_read_permission(target):
    check_bundles_have_read_permission(local.model, request.user, [target.bundle_uuid])


def get_target_info(target, depth):
    """
    Returns information about an individual target inside the bundle
    Raises NotFoundError if target bundle or path don't exist
    """
    check_target_has_read_permission(target)
    target_info = local.download_manager.get_target_info(target, depth)
    if target_info['resolved_target'] != target:
        check_target_has_read_permission(target_info['resolved_target'])
    return target_info


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
        abort(http.client.CONFLICT, 'Group with name %s already exists' % name)


def get_group_info(group_spec, need_admin, access_all_groups=False):
    """
    Resolve |group_spec| and return the associated group_info.
    """
    user_id = request.user.user_id
    is_root_user = user_id == local.model.root_user_id

    # If we're root, then we can access any group, otherwise get is_admin column with group_info
    if is_root_user or access_all_groups:
        # note: the returned object will NOT contain the 'is_admin' column
        group_info = unique_group(local.model, group_spec, user_id=None)
    else:
        # note: the returned object will contain the 'is_admin' column
        group_info = unique_group(local.model, group_spec, user_id=user_id)

    # If not root and need admin access, but don't have it, raise error.
    if not is_root_user and need_admin and group_info.get('is_admin') is False:
        abort(http.client.FORBIDDEN, 'You are not the admin of group %s.' % group_spec)

    # No one can admin the public group (not even root), because it's a special group.
    if need_admin and group_info['uuid'] == local.model.public_group_uuid:
        abort(http.client.FORBIDDEN, 'Cannot modify the public group %s.' % group_spec)

    return group_info
