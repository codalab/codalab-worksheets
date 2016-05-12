"""
Worksheets REST API Groups Views.
"""
import httplib

from bottle import abort, get, delete, post, request, local
from marshmallow_jsonapi import Schema, fields

from codalab.lib.server_util import (
    json_api_include,
    query_get_list,
)
from codalab.lib.spec_util import validate_uuid, validate_name
from codalab.rest.users import UserSchema
from codalab.rest.util import get_group_info, ensure_unused_group_name
from codalab.server.authenticated_plugin import AuthenticatedPlugin
from codalab.server.json_api_plugin import JsonApiPlugin

#############################################################
#  GROUP DE/SERIALIZATION AND VALIDATION SCHEMAS
#############################################################


class GroupSchema(Schema):
    id = fields.String(validate=validate_uuid, attribute='uuid')
    name = fields.String(required=True, validate=validate_name)
    user_defined = fields.Bool(dump_only=True)
    owner = fields.Relationship(include_data=True, type_='users', attribute='owner_id')
    memberships = fields.Relationship(dump_only=True, many=True, type_='group-memberships', include_data=True)

    class Meta:
        type_ = 'groups'


class GroupMembershipSchema(Schema):
    id = fields.Integer(as_string=True)
    user = fields.Relationship(required=True, include_data=True, type_='users', attribute='user_id')
    group = fields.Relationship(required=True, include_data=True, type_='groups', attribute='group_uuid')
    is_admin = fields.Boolean(required=True)

    class Meta:
        type_ = 'group-memberships'


#############################################################
#  GROUP REST API ENDPOINTS
#############################################################


@get('/groups/<group_spec>', apply=[AuthenticatedPlugin(), JsonApiPlugin()])
def fetch_group(group_spec):
    """Fetch a single group."""
    group = get_group_info(group_spec, need_admin=False)
    document = GroupSchema().dump(group).data
    include_group_relationships(document, [group])
    return document


@get('/groups', apply=[AuthenticatedPlugin(), JsonApiPlugin()])
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
        group['memberships'] = local.model.batch_get_user_in_group(
            group_uuid=group['uuid'])

    document = GroupSchema(many=True).dump(groups).data
    include_group_relationships(document, groups)
    return document


def include_group_relationships(document, groups):
    include = query_get_list('include')
    if 'group-memberships' in include:
        memberships = [m for group in groups for m in group['memberships']]
        json_api_include(document, GroupMembershipSchema(), memberships)
    if 'users' in include:
        user_ids = set(m['user_id'] for group in groups for m in group['memberships'])
        user_ids |= set(group['owner_id'] for group in groups)
        json_api_include(document, UserSchema(), local.model.get_users(user_ids))


@delete('/groups', apply=[AuthenticatedPlugin(), JsonApiPlugin()])
def delete_groups():
    """Delete groups."""
    groups = GroupSchema(strict=True, many=True)\
        .load(request.json, partial=True).data

    for group in groups:
        group = get_group_info(group['uuid'], need_admin=True)
        local.model.delete_group(group['uuid'])

    abort(httplib.NO_CONTENT)


# TODO(sckoo): support bulk create
@post('/groups', apply=[AuthenticatedPlugin(), JsonApiPlugin()])
def create_group():
    """Create a group."""
    group = GroupSchema(strict=True).load(request.json, partial=True).data
    ensure_unused_group_name(group['name'])
    group['owner_id'] = request.user.user_id
    group = local.model.create_group(group)
    local.model.add_user_in_group(request.user.user_id, group['uuid'], True)
    return GroupSchema().dump(group).data


# TODO(sckoo): support bulk create
@post('/group-memberships', apply=[AuthenticatedPlugin(), JsonApiPlugin()])
def create_or_update_group_membership():
    membership_updates = GroupMembershipSchema(strict=True)\
        .load(request.json).data
    memberships = local.model.batch_get_user_in_group(
        user_id=membership_updates['user_id'],
        group_uuid=membership_updates['group_uuid'])

    if len(memberships) > 0:
        membership = memberships[0]
        membership.update(membership_updates)
        local.model.update_user_in_group(
            membership['user_id'],
            membership['group_uuid'],
            membership['is_admin'])
    else:
        membership = membership_updates
        membership = local.model.add_user_in_group(
            membership['user_id'],
            membership['group_uuid'],
            membership['is_admin'])

    return GroupMembershipSchema().dump(membership).data


@delete('/group-memberships', apply=[AuthenticatedPlugin(), JsonApiPlugin()])
def delete_group_memberships():
    data = GroupMembershipSchema(strict=True, many=True)\
        .load(request.json, partial=True).data
    membership_ids = [m['id'] for m in data]
    memberships = {
        membership['id']: membership
        for membership in local.model.batch_get_user_in_group(id=membership_ids)
    }

    for membership_id in membership_ids:
        if membership_id not in memberships:
            abort(httplib.NOT_FOUND, "Membership %d not found" % membership_id)

    for membership in memberships.itervalues():
        # Check permissions first
        get_group_info(membership['group_uuid'], need_admin=True)
        local.model.delete_user_in_group(
            membership['user_id'], membership['group_uuid'])

    abort(httplib.NO_CONTENT)

