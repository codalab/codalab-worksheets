"""
Worksheets REST API Groups Views.
"""
import httplib

from bottle import abort, get, delete, post, request, local

from codalab.lib.server_util import json_api_include
from codalab.rest.schemas import GroupSchema, UserSchema
from codalab.rest.util import (
    ensure_unused_group_name,
    get_group_info,
    get_resource_ids,
)
from codalab.server.authenticated_plugin import AuthenticatedPlugin


@get('/groups/<group_spec>', apply=AuthenticatedPlugin())
def fetch_group(group_spec):
    """Fetch a single group."""
    group = get_group_info(group_spec, need_admin=False, access_all_groups=True)
    load_group_members(group)
    document = GroupSchema().dump(group).data
    include_group_relationships(document, [group])
    return document


@get('/groups', apply=AuthenticatedPlugin())
def fetch_groups():
    """Fetch list of groups readable by the authenticated user."""
    if request.user.user_id == local.model.root_user_id:
        groups = local.model.batch_get_all_groups(
            None,
            {'user_defined': True},
            None
        )
    else:
        groups = local.model.batch_get_all_groups(
            None,
            {'owner_id': request.user.user_id, 'user_defined': True},
            {'user_id': request.user.user_id}
        )

    for group in groups:
        load_group_members(group)

    document = GroupSchema(many=True).dump(groups).data
    include_group_relationships(document, groups)
    return document


def load_group_members(group):
    memberships = local.model.batch_get_user_in_group(group_uuid=group['uuid'])
    group['admins'] = []
    group['members'] = []
    for m in memberships:
        if m['user_id'] == group['owner_id']:
            continue
        elif m['is_admin']:
            group['admins'].append(m['user_id'])
        else:
            group['members'].append(m['user_id'])


def include_group_relationships(document, groups):
    user_ids = set()
    for group in groups:
        user_ids.add(group['owner_id'])
        user_ids.update(group['members'])
        user_ids.update(group['admins'])
    json_api_include(document, UserSchema(), local.model.get_users(user_ids))


@delete('/groups', apply=AuthenticatedPlugin())
def delete_groups():
    """Delete groups."""
    group_ids = get_resource_ids(request.json, 'groups')

    # Check permissions first
    for group_id in group_ids:
        get_group_info(group_id, need_admin=True)

    # Delete groups
    for group_id in group_ids:
        local.model.delete_group(group_id)

    abort(httplib.NO_CONTENT)


@post('/groups', apply=AuthenticatedPlugin())
def create_group():
    """Create a group."""
    groups = GroupSchema(strict=True, many=True).load(request.json, partial=True).data
    created_groups = []
    for group in groups:
        ensure_unused_group_name(group['name'])
        group['owner_id'] = request.user.user_id
        group['user_defined'] = True
        group = local.model.create_group(group)
        local.model.add_user_in_group(request.user.user_id, group['uuid'], True)
        created_groups.append(group)
    return GroupSchema(many=True).dump(created_groups).data


@post('/groups/<group_spec>/relationships/admins', apply=AuthenticatedPlugin())
def add_group_admins(group_spec):
    return add_group_members_helper(group_spec, True)


@post('/groups/<group_spec>/relationships/members', apply=AuthenticatedPlugin())
def add_group_members(group_spec):
    return add_group_members_helper(group_spec, False)


def add_group_members_helper(group_spec, is_admin):
    user_ids = get_resource_ids(request.json, 'users')
    group_uuid = get_group_info(group_spec, need_admin=True,
                                access_all_groups=True)['uuid']
    members = set(m['user_id'] for m in local.model.batch_get_user_in_group(
        user_id=user_ids, group_uuid=group_uuid))
    for user_id in user_ids:
        if user_id in members:
            local.model.update_user_in_group(user_id, group_uuid, is_admin)
        else:
            local.model.add_user_in_group(user_id, group_uuid, is_admin)
    return request.json


@delete('/groups/<group_spec>/relationships/admins', apply=AuthenticatedPlugin())
@delete('/groups/<group_spec>/relationships/members', apply=AuthenticatedPlugin())
def delete_group_members(group_spec):
    # For now, both routes will delete a member entirely from the group.
    user_ids = get_resource_ids(request.json, 'users')
    group = get_group_info(group_spec, need_admin=True)
    for user_id in user_ids:
        local.model.delete_user_in_group(user_id, group['uuid'])
    abort(httplib.NO_CONTENT)
