"""
Helper functions for working with the BundleModel.
Most of these are adapted from the LocalBundleClient methods,
Placed in this central location to prevent circular imports.
"""
import httplib
import sys
import threading
from functools import wraps

from bottle import abort, HTTPError, local, request

from codalab.bundles import PrivateBundle
from codalab.common import http_error_to_exception, precondition
from codalab.lib import bundle_util
from codalab.lib.server_util import rate_limited
from codalab.model.tables import GROUP_OBJECT_PERMISSION_READ
from codalab.objects.permission import (
    unique_group,
)


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


local_bundle_client_context = threading.local()


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
    @wraps(f)
    def wrapper(*args, **kwargs):
        # Always pop out the 'client' kwarg
        client = kwargs.pop('client', None)
        try:
            # Test to see if request context is initialized
            _ = request.user.user_id
        except (AttributeError, RuntimeError):
            # Request context not initialized: we are NOT in a Bottle app
            # Fabricate a thread-local context for LocalBundleClient
            if client is not None:
                # notify_admin() actually expects client=CodaLabManager rather
                # than LocalBundleClient, so this is a hack to avoid an
                # AttributeError for this special case
                user_id = (client._current_user_id()
                           if hasattr(client, '_current_user_id')
                           else None)
                # User will be None if not logged in (a 'public' user)
                from codalab.objects.user import PUBLIC_USER
                if user_id is None:
                    user = PUBLIC_USER
                else:
                    user = client.model.get_user(user_id=user_id)
                local_bundle_client_context.local = client
                local_bundle_client_context.request = DummyRequest(user)

            precondition((hasattr(local_bundle_client_context, 'local') and
                          hasattr(local_bundle_client_context, 'request')),
                         'LocalBundleClient environment failed to initialize')

            try:
                # Shim in local and request
                return f(local_bundle_client_context.local,
                         local_bundle_client_context.request,
                         *args, **kwargs)
            except HTTPError as e:
                # Translate HTTP errors back to CodaLab exceptions
                raise http_error_to_exception(e.status_code, e.message)
            finally:
                # Clean up when this request is done, thread may be recycled
                # But should only do this on the root call, where 'client'
                # was passed as a kwarg: all recursive calls of REST methods
                # that derive from the original call need to use the same
                # context.
                if client is not None:
                    delattr(local_bundle_client_context, 'local')
                    delattr(local_bundle_client_context, 'request')
        else:
            # We are in the Bottle app, all is good
            return f(local, request, *args, **kwargs)

    return wrapper


# For non-REST services, should call with client=CodaLabManager
@rate_limited(max_calls_per_hour=6)
@local_bundle_client_compatible
def notify_admin(local, request, message):
    # Caller is responsible for logging message anyway if desired
    if 'admin_email' not in local.config['server']:
        print >>sys.stderr, 'Warning: No admin_email configured, so no email sent.'
        return

    subject = "CodaLab Admin Notification"
    if 'instance_name' in local.config['server']:
        subject += " (%s)" % local.config['server']['instance_name']

    local.emailer.send_email(subject=subject,
                             body=message,
                             recipient=local.config['server']['admin_email'])


#############################################################
# BUNDLES
#############################################################

# Placed here to prevent cyclic imports between rest.bundles and rest.worksheets
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
        # Fill the info
        group_result = local.model.batch_get_group_bundle_permissions(request.user.user_id, uuids)
        result = local.model.get_user_bundle_permissions(request.user.user_id, uuids, local.model.get_bundle_owner_ids(uuids))
        for uuid, info in bundle_dict.items():
            info['group_permissions'] = group_result[uuid]
            info['permission'] = result[uuid]

    return bundle_dict


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

