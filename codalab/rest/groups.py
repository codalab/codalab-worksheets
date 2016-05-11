"""
Worksheets REST API Groups Views.
"""
import httplib

from bottle import abort, get, delete, post, request, local
from marshmallow_jsonapi import Schema, fields

from codalab.lib.server_util import bottle_patch as patch, json_api_include, diff_info
from codalab.lib.spec_util import validate_uuid, validate_name
from codalab.rest.users import UserSchema
from codalab.rest.util import get_group_info, ensure_unused_group_name
from codalab.server.authenticated_plugin import AuthenticatedPlugin
from codalab.server.json_api_plugin import JsonApiPlugin

#############################################################
#  GROUP DE/SERIALIZATION AND VALIDATION SCHEMAS
#############################################################


class GroupSchema(Schema):
    id = fields.String(validate=validate_uuid, dump_only=True, attribute='uuid')
    name = fields.String(required=True, validate=validate_name)
    user_defined = fields.Bool(dump_only=True)
    owner = fields.Relationship(include_data=True, type_='users', attribute='owner_id')
    memberships = fields.Relationship(dump_only=True, many=True, type_='group-memberships', include_data=True)

    class Meta:
        type_ = 'groups'


class GroupMembershipSchema(Schema):
    id = fields.Integer(as_string=True, dump_only=True)
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

    # TODO(sckoo): consider requiring include params?
    # If include=group-memberships
    json_api_include(document, GroupMembershipSchema(), group['memberships'])

    # If include=users
    user_ids = set([group['owner_id']] + [m['user_id'] for m in group['memberships']])
    json_api_include(document, UserSchema(), local.model.get_users(user_ids))

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

    user_ids = set()
    memberships = []
    for group in groups:
        group['memberships'] = local.model.batch_get_user_in_group(group_uuid=group['uuid'])
        memberships.extend(group['memberships'])
        user_ids |= set(m['user_id'] for m in group['memberships'])

    document = GroupSchema(many=True).dump(groups).data
    # TODO(sckoo): consider using query params
    json_api_include(document, GroupMembershipSchema(), memberships)
    json_api_include(document, UserSchema(), local.model.get_users(user_ids))

    return document


@delete('/groups/<group_spec>', apply=[AuthenticatedPlugin(), JsonApiPlugin()])
def delete_group(group_spec):
    """Delete a single group."""
    group = get_group_info(group_spec, need_admin=True)
    local.model.delete_group(group['uuid'])
    abort(httplib.NO_CONTENT)


@patch('/groups/<group_spec>', apply=[AuthenticatedPlugin(), JsonApiPlugin()])
def update_group(group_spec):
    """Update a single group."""
    group = get_group_info(group_spec, need_admin=True)
    group_updates = GroupSchema(strict=True).load(request.json, partial=True).data

    patch = diff_info(group, group_updates)
    patch['uuid'] = group['uuid']
    if 'name' in patch:
        ensure_unused_group_name(group['name'])

    local.model.update_group(patch)
    group.update(patch)
    return GroupSchema().dump(group).data


# TODO(sckoo): support bulk add
@post('/groups', apply=[AuthenticatedPlugin(), JsonApiPlugin()])
def create_group():
    """Create a group."""
    group = GroupSchema(strict=True).load(request.json, partial=True).data
    ensure_unused_group_name(group['name'])
    group['owner_id'] = request.user.user_id
    group = local.model.create_group(group)
    local.model.add_user_in_group(request.user.user_id, group['uuid'], True)
    return GroupSchema().dump(group).data


# TODO(sckoo): support bulk add
@post('/group-memberships', apply=[AuthenticatedPlugin(), JsonApiPlugin()])
def create_or_update_group_membership():
    membership_updates = GroupMembershipSchema(strict=True).load(request.json).data

    # FIXME(sckoo): this is not transactional
    memberships = local.model.batch_get_user_in_group(user_id=membership_updates['user_id'], group_uuid=membership_updates['group_uuid'])
    if len(memberships) > 0:
        membership = memberships[0]
        membership.update(membership_updates)
        local.model.update_user_in_group(membership['user_id'], membership['group_uuid'], membership['is_admin'])
    else:
        membership = membership_updates
        membership = local.model.add_user_in_group(membership['user_id'], membership['group_uuid'], membership['is_admin'])

    return GroupMembershipSchema().dump(membership).data


# TODO(sckoo): support bulk delete
@delete('/group-memberships/<membership_id>', apply=[AuthenticatedPlugin(), JsonApiPlugin()])
def delete_group_membership(membership_id):
    memberships = local.model.batch_get_user_in_group(id=membership_id)
    if not memberships:
        abort(httplib.NOT_FOUND, "Membership %d not found" % membership_id)
    membership = memberships[0]
    local.model.delete_user_in_group(membership['user_id'], membership['group_uuid'])
    abort(httplib.NO_CONTENT)

