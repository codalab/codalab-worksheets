"""
Worksheets REST API Users Views.

TODO(sckoo): Factor out the reusable parts of the JSON API views.
"""
from bottle import get, route, response, request, local, HTTPResponse
from marshmallow import ValidationError
from marshmallow_jsonapi import Schema, fields

from codalab.common import UsageError
from codalab.lib import formatting
from codalab.server.authenticated_plugin import AuthenticatedPlugin
from codalab.server.json_api_plugin import JsonApiPlugin


PUBLIC_USER_FIELDS = ('id', 'user_name', 'email', 'first_name', 'last_name',
                      'date_joined', 'affiliation', 'url')


class DataSize(fields.Field):
    def _serialize(self, value, attr, obj):
        return formatting.size_str(value)

    def _deserialize(self, value, attr, data):
        return formatting.parse_size(value)


class Duration(fields.Field):
    def _serialize(self, value, attr, obj):
        return formatting.duration_str(value)

    def _deserialize(self, value, attr, data):
        return formatting.parse_duration(value)


class UserSchema(Schema):
    id = fields.String(dump_only=True, attribute='user_id')
    user_name = fields.String()
    email = fields.Email(dump_only=True)
    first_name = fields.String(allow_none=True)
    last_name = fields.String(allow_none=True)
    affiliation = fields.String(allow_none=True)
    url = fields.Url(allow_none=True)
    time_quota = Duration(dump_only=True)
    time_used = Duration(dump_only=True)
    disk_quota = DataSize(dump_only=True)
    disk_used = DataSize(dump_only=True)
    last_login = fields.LocalDateTime("%c", dump_only=True)
    date_joined = fields.LocalDateTime("%c", dump_only=True)

    class Meta:
        type_ = 'users'


@get('/user', apply=[JsonApiPlugin(), AuthenticatedPlugin()])
def fetch_authenticated_user():
    """Fetch authenticated user."""
    fieldset = request.jsonapi.fields.get('users', None)
    return UserSchema(only=fieldset).dump(request.user).data


@route('/user', method=['PUT', 'PATCH'], apply=[JsonApiPlugin(), AuthenticatedPlugin()])
def update_authenticated_user():
    """Update one or multiple fields of the authenticated user."""
    # Load update request data
    try:
        user_info, errors = UserSchema(strict=True).load(request.json, partial=True)
    except ValidationError as e:
        response.status = '403 Forbidden'
        return e.messages

    # Patch in user_id manually (do not allow requests to change id)
    user_info['user_id'] = request.user.user_id

    # Ensure that user name is not taken
    if (user_info.get('user_name', request.user.user_name) != request.user.user_name and
        local.model.user_exists(user_info['user_name'], None)):
        response.status = '403 Forbidden'
        raise UsageError("User name %s is already taken." % user_info['user_name'])

    # Update user
    local.model.update_user_info(user_info)
    request.user = local.model.get_user(request.user.user_id)

    # Return updated user
    return fetch_authenticated_user()


@get('/users/<id>', apply=JsonApiPlugin())
def fetch_user(id):
    """Fetch a single user."""
    user = local.model.get_user(id)
    if user is None:
        return HTTPResponse(status=404)

    # Filter fieldset if specified (None means dump all attributes)
    # Additionally filter fieldset for users that aren't the current user
    fieldset = request.jsonapi.fields.get('users', None)
    if request.user is None or request.user.user_id != id:
        if fieldset is None:
            fieldset = PUBLIC_USER_FIELDS
        else:
            fieldset &= set(PUBLIC_USER_FIELDS)

    return UserSchema(only=fieldset).dump(user).data


@route('/users/<id>', method=['PUT', 'PATCH'], apply=JsonApiPlugin())
def update_user(id):
    """Allow updates to authenticated user ONLY."""
    if request.user is None or request.user.user_id != id:
        return HTTPResponse(status='403 Forbidden')
    return update_authenticated_user()


@get('/users', apply=[JsonApiPlugin()])
def get_users():
    """Fetch list of users, filterable by username and email."""
    # Fetch users, combining username and email filters
    usernames = set(request.jsonapi.filter.get('user_name', []))
    usernames |= set(request.jsonapi.filter.get('email', []))
    users = local.model.get_users(usernames=(usernames or None))

    # Filter fieldset if specified (None means dump all attributes)
    fieldset = request.jsonapi.fields.get('users', None)
    if fieldset is None:
        fieldset = PUBLIC_USER_FIELDS
    else:
        fieldset &= set(PUBLIC_USER_FIELDS)

    return UserSchema(many=True, only=fieldset).dump(users).data


