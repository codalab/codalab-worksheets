"""
Worksheets REST API Users Views.
"""
import httplib

from bottle import abort, get, route, request, local
from marshmallow import ValidationError
from marshmallow_jsonapi import Schema, fields

from codalab.lib import formatting
from codalab.lib.spec_util import NAME_REGEX
from codalab.server.authenticated_plugin import (
    AuthenticatedPlugin,
    UserVerifiedPlugin,
)


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


@get('/user', apply=AuthenticatedPlugin(), skip=UserVerifiedPlugin)
def fetch_authenticated_user():
    """Fetch authenticated user."""
    return UserSchema().dump(request.user).data


@route('/user', method='PATCH', apply=AuthenticatedPlugin(), skip=UserVerifiedPlugin)
def update_authenticated_user():
    """Update one or multiple fields of the authenticated user."""
    # Load update request data
    try:
        user_info, errors = UserSchema(strict=True).load(request.json, partial=True)
    except ValidationError as err:
        message = ' '.join([e['detail'] for e in err.messages['errors']])
        abort(httplib.BAD_REQUEST, message)

    # Patch in user_id manually (do not allow requests to change id)
    user_info['user_id'] = request.user.user_id

    # Ensure that user name is not taken
    if (user_info.get('user_name', request.user.user_name) != request.user.user_name and
        local.model.user_exists(user_info['user_name'], None)):
        abort(httplib.BAD_REQUEST, "User name %s is already taken." % user_info['user_name'])

    # Validate user name
    if not NAME_REGEX.match(user_info.get('user_name', request.user.user_name)):
        abort(httplib.BAD_REQUEST, "User name characters must be alphanumeric, underscores, periods, or dashes.")

    # Update user
    local.model.update_user_info(user_info)

    # Return updated user
    return UserSchema().dump(local.model.get_user(request.user.user_id)).data


@get('/users/<id>')
def fetch_user(id):
    """Fetch a single user."""
    user = local.model.get_user(id)
    if user is None:
        abort(httplib.NOT_FOUND, "User %s not found" % id)
    return UserSchema(only=PUBLIC_USER_FIELDS).dump(user).data


@get('/users')
def fetch_users():
    """Fetch list of users, filterable by username and email.

    Takes the following query parameters:
        filter[user_name]=name1,name2,...
        filter[email]=email1,email2,...

    Fetches all users that match any of these usernames or emails.
    """
    # Combine username and email filters
    usernames = set(request.query.get('filter[user_name]', '').split(','))
    usernames |= set(request.query.get('filter[email]', '').split(','))
    usernames.discard('')  # str.split(',') will return '' on empty strings
    users = local.model.get_users(usernames=(usernames or None))
    return UserSchema(many=True, only=PUBLIC_USER_FIELDS).dump(users).data
