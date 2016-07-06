"""
Helper functions for working with the BundleModel.
Most of these are adapted from the LocalBundleClient methods,
Placed in this central location to prevent circular imports.
"""
import httplib
import threading

from bottle import abort, HTTPError, local, request

from codalab.common import http_error_to_exception
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
    def wrapper(*args, **kwargs):
        # Shim in local and request
        if 'client' in kwargs:
            client = kwargs.pop('client')
            user = client.model.get_user(user_id=client._current_user_id())
            local_bundle_client_context.local = client
            local_bundle_client_context.request = DummyRequest(user)
        request_ = getattr(local_bundle_client_context, 'request', request)
        local_ = getattr(local_bundle_client_context, 'local', local)

        # Translate HTTP errors back to CodaLab exceptions
        try:
            return f(local_, request_, *args, **kwargs)
        except HTTPError as e:
            raise http_error_to_exception(e.status_code, e.message)

    return wrapper


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

