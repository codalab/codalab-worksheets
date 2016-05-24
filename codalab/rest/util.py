"""
Helper functions for working with the BundleModel.
Most of these are adapted from the LocalBundleClient methods,
Placed in this central location to prevent circular imports.
"""
import httplib
import re

from bottle import abort, HTTPError, local, request

from codalab.common import http_error_to_exception
from codalab.bundles import PrivateBundle
from codalab.common import UsageError, PermissionError
from codalab.lib import worksheet_util, spec_util, canonicalize
from codalab.model.tables import GROUP_OBJECT_PERMISSION_READ
from codalab.objects.permission import (
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
    """Dummy classes for local_bundle_client_compatible shim."""
    class DummyUser(object):
        def __init__(self, user_id):
            self.user_id = user_id

    def __init__(self, user=None, user_id=None):
        if user is not None:
            self.user = user
        elif user_id is not None:
            self.user = DummyRequest.DummyUser(user_id)


def local_bundle_client_compatible(f):
    """
    Temporary hack to make decorated functions callable from LocalBundleClient.
    This allows us to share code between LocalBundleClient and the REST server.
    To call a decorated function from LocalBundleClient, pass in self as the
    |client| kwarg and optionally the authenticated User as |user| or the
    ID of the authenticated user as |user_id|.

    TODO(sckoo): To clean up, for each decorated function:
        - Un-decorate function
        - Remove |local| and |request| arguments
    """
    def wrapper(*args, **kwargs):
        # Shim in local and request
        local_ = kwargs.pop('client', local)
        if 'user' in kwargs:
            request_ = DummyRequest(user=kwargs.pop('user'))
        elif 'user_id' in kwargs:
            request_ = DummyRequest(user_id=kwargs.pop('user_id'))
        else:
            request_ = request
        # Translate HTTP errors back to CodaLab exceptions
        try:
            return f(local_, request_, *args, **kwargs)
        except HTTPError as e:
            raise http_error_to_exception(e.status_code, e.message)
    return wrapper


#############################################################
# WORKSHEETS
#############################################################


def check_worksheet_not_frozen(worksheet):
    if worksheet.frozen:
        raise PermissionError('Cannot mutate frozen worksheet %s(%s).' % (worksheet.uuid, worksheet.name))


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


# FIXME(sckoo): fix when implementing worksheets API
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
        pass
        # FIXME
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

