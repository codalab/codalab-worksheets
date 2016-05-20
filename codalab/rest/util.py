"""
Helper functions for working with the BundleModel.
Most of these are adapted from the LocalBundleClient methods,
Placed in this central location to prevent circular imports.
"""
import httplib
import os
import re

from bottle import abort, local, request

from codalab.common import UsageError, PermissionError, NotFoundError
from codalab.lib import worksheet_util, spec_util, canonicalize, formatting
from codalab.model.util import LikeQuery


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

