"""
Helper functions for working with the BundleModel.
Most of these are adapted from the LocalBundleClient methods,
Placed in this central location to prevent circular imports.
"""
import httplib
import os
import re
import sys

from bottle import abort, HTTPError, local, request

from codalab.common import http_error_to_exception, State
from codalab.bundles import PrivateBundle
from codalab.common import UsageError, PermissionError
from codalab.lib import bundle_util, worksheet_util, spec_util, canonicalize
from codalab.model.tables import GROUP_OBJECT_PERMISSION_READ
from codalab.objects.permission import (
    check_bundles_have_all_permission,
    check_worksheet_has_all_permission,
    parse_permission,
    unique_group,
)
from codalab.objects.worksheet import Worksheet


def get_resource_ids(document, type_):
    links = document['data']
    if not isinstance(links, list):
        links = [links]
    if any(link['type'] != type_ for link in links):
        raise abort(httplib.BAD_REQUEST, 'type must be %r' % type_)
    return [link['id'] for link in links]


class DummyRequest(object):
    """
    Dummy class for local_bundle_client_compatible shim.
    Delete along with the decorator when cleaning up.
    """
    def __init__(self, user):
        self.user = user


def local_bundle_client_compatible(f):
    """
    Temporary hack to make decorated functions callable from LocalBundleClient.
    This allows us to share code between LocalBundleClient and the REST server.
    To call a decorated function from LocalBundleClient, pass in self as the
    |client| kwarg.

    TODO(sckoo): To clean up, for each decorated function:
        - Un-decorate function
        - Remove |local| and |request| arguments
    """
    def wrapper(*args, **kwargs):
        # Shim in local and request
        if 'client' in kwargs:
            client = kwargs.pop('client')
            user = client.model.get_user(user_id=client._current_user_id())
            request_ = DummyRequest(user=user)
            local_ = client
        else:
            request_ = request
            local_ = local

        # Translate HTTP errors back to CodaLab exceptions
        try:
            return f(local_, request_, *args, **kwargs)
        except HTTPError as e:
            raise http_error_to_exception(e.status_code, e.message)
    return wrapper


#############################################################
# WORKSHEETS
#############################################################


@local_bundle_client_compatible
def set_worksheet_permission(local, request, worksheet_uuid, group_uuid, permission):
    """
    Give the given |group_uuid| the desired |permission| on |worksheet_uuid|.
    """
    check_worksheet_has_all_permission(local.model, request.user.user_id, worksheet_uuid)
    local.model.set_group_worksheet_permission(group_uuid, worksheet_uuid, permission)


# FIXME(sckoo): fix when implementing worksheets API
@local_bundle_client_compatible
def populate_dashboard(local, request, worksheet):
    raise NotImplementedError
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../objects/dashboard.ws')
    lines = [line.rstrip() for line in open(file_path, 'r').readlines()]
    items, commands = worksheet_util.parse_worksheet_form(lines, self, worksheet.uuid)
    info = self.get_worksheet_info(worksheet.uuid, True)
    self.update_worksheet_items(info, items)
    self.update_worksheet_metadata(worksheet.uuid, {'title': 'Codalab Dashboard'})


@local_bundle_client_compatible
def get_worksheet_uuid_or_none(local, request, base_worksheet_uuid, worksheet_spec):
    """
    Helper: Return the uuid of the specified worksheet if it exists. Otherwise, return None.
    """
    try:
        return canonicalize.get_worksheet_uuid(local.model, base_worksheet_uuid, worksheet_spec)
    except UsageError:
        return None


@local_bundle_client_compatible
def ensure_unused_worksheet_name(local, request, name):
    """
    Ensure worksheet names are unique.
    Note: for simplicity, we are ensuring uniqueness across the system, even on
    worksheet names that the user may not have access to.
    """
    # If trying to set the name to a home worksheet, then it better be
    # user's home worksheet.
    if spec_util.is_home_worksheet(name) and spec_util.home_worksheet(request.user.user_name) != name:
        raise UsageError('Cannot create %s because this is potentially the home worksheet of another user' % name)
    if get_worksheet_uuid_or_none(None, name) is not None:
        raise UsageError('Worksheet with name %s already exists' % name)


@local_bundle_client_compatible
def new_worksheet(local, request, name):
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
    set_worksheet_permission(worksheet.uuid, local.model.public_group_uuid,
                             GROUP_OBJECT_PERMISSION_READ)
    if spec_util.is_dashboard(name):
        populate_dashboard(worksheet)
    return worksheet.uuid


@local_bundle_client_compatible
def get_worksheet_uuid(local, request, base_worksheet_uuid, worksheet_spec):
    """
    Return the uuid of the specified worksheet if it exists.
    If not, create a new worksheet if the specified worksheet is home_worksheet
    or dashboard. Otherwise, throw an error.
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


@local_bundle_client_compatible
def delete_bundles(local, request, uuids, force, recursive, data_only, dry_run):
    """
    Delete the bundles specified by |uuids|.
    If |force|, allow deletion of bundles that have descendants or that appear across multiple worksheets.
    If |recursive|, add all bundles downstream too.
    If |data_only|, only remove from the bundle store, not the bundle metadata.
    """
    relevant_uuids = local.model.get_self_and_descendants(uuids, depth=sys.maxint)
    if not recursive:
        # If any descendants exist, then we only delete uuids if force = True.
        if (not force) and set(uuids) != set(relevant_uuids):
            relevant = local.model.batch_get_bundles(uuid=(set(relevant_uuids) - set(uuids)))
            raise UsageError('Can\'t delete bundles %s because the following bundles depend on them:\n  %s' % (
                ' '.join(uuids),
                '\n  '.join(bundle.simple_str() for bundle in relevant),
            ))
        relevant_uuids = uuids
    check_bundles_have_all_permission(local.model, request.user, relevant_uuids)

    # Make sure we don't delete bundles which are active.
    states = local.model.get_bundle_states(uuids)
    active_uuids = [uuid for (uuid, state) in states.items() if state in State.ACTIVE_STATES]
    if len(active_uuids) > 0:
        raise UsageError('Can\'t delete bundles: %s. ' % (' '.join(active_uuids)) +
                         'For run bundles, kill them first. ' +
                         'Bundles stuck not running will eventually ' +
                         'automatically be moved to a state where they ' +
                         'can be deleted.')

    # Make sure that bundles are not referenced in multiple places (otherwise, it's very dangerous)
    result = local.model.get_host_worksheet_uuids(relevant_uuids)
    for uuid, host_worksheet_uuids in result.items():
        worksheets = local.model.batch_get_worksheets(fetch_items=False, uuid=host_worksheet_uuids)
        frozen_worksheets = [worksheet for worksheet in worksheets if worksheet.frozen]
        if len(frozen_worksheets) > 0:
            raise UsageError("Can't delete bundle %s because it appears in frozen worksheets "
                             "(need to delete worksheet first):\n  %s" %
                             (uuid, '\n  '.join(worksheet.simple_str() for worksheet in frozen_worksheets)))
        if not force and len(host_worksheet_uuids) > 1:
            raise UsageError("Can't delete bundle %s because it appears in multiple worksheets "
                             "(--force to override):\n  %s" %
                             (uuid, '\n  '.join(worksheet.simple_str() for worksheet in worksheets)))

    # Delete the actual bundle
    if not dry_run:
        if data_only:
            # Just remove references to the data hashes
            local.model.remove_data_hash_references(relevant_uuids)
        else:
            # Actually delete the bundle
            local.model.delete_bundles(relevant_uuids)

        # Update user statistics
        local.model.update_user_disk_used(request.user.user_id)

    # Delete the data.
    for uuid in relevant_uuids:
        # check first is needs to be deleted
        bundle_location = local.bundle_store.get_bundle_location(uuid)
        if os.path.lexists(bundle_location):
            local.bundle_store.cleanup(uuid, dry_run)

    return relevant_uuids


@local_bundle_client_compatible
def get_bundle_infos(local, request, uuids, get_children=False,
                     get_host_worksheets=False, get_permissions=False):
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
            bundle_dict[uuid] = mask_bundle(bundle_dict[uuid])

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
        # Fill the info
        group_result = local.model.batch_get_group_bundle_permissions(request.user.user_id, uuids)
        result = local.model.get_user_bundle_permissions(request.user.user_id, uuids, local.model.get_bundle_owner_ids(uuids))
        for uuid, info in bundle_dict.items():
            info['group_permissions'] = group_result[uuid]
            info['permission'] = result[uuid]

    return bundle_dict


@local_bundle_client_compatible
def resolve_owner_in_keywords(local, request, keywords):
    # Resolve references to owner ids
    def resolve(keyword):
        # Example: owner=codalab => owner_id=0
        m = re.match('owner=(.+)', keyword)
        if not m:
            return keyword
        return 'owner_id=%s' % getattr(local.model.get_user(username=m.group(1)), 'user_id', 'x')
    return map(resolve, keywords)


@local_bundle_client_compatible
def set_bundle_permissions(local, request, new_permissions):
    # Check if current user has permission to set bundle permissions
    check_bundles_have_all_permission(
        local.model, request.user, [p['object_uuid'] for p in new_permissions])
    # Sequentially set bundle permissions
    for p in new_permissions:
        local.model.set_group_bundle_permission(
            p['group_uuid'], p['object_uuid'], p['permission'])


#############################################################
# GROUPS
#############################################################


@local_bundle_client_compatible
def ensure_unused_group_name(local, request, name):
    """
    Ensure group names are unique.  Note: for simplicity, we are
    ensuring uniqueness across the system, even on group names that
    the user may not have access to.
    """
    groups = local.model.batch_get_groups(name=name)
    if len(groups) != 0:
        abort(httplib.CONFLICT, 'Group with name %s already exists' % name)


@local_bundle_client_compatible
def get_group_info(local, request, group_spec, need_admin):
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

    return group_info

