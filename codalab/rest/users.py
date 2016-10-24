"""
Worksheets REST API Users Views.
"""
import httplib

from bottle import abort, get, request, local

from codalab.lib.spec_util import NAME_REGEX
from codalab.lib.server_util import bottle_patch as patch
from codalab.rest.schemas import (
    AuthenticatedUserSchema,
    USER_READ_ONLY_FIELDS,
    UserSchema,
)
from codalab.server.authenticated_plugin import (
    AuthenticatedPlugin,
    UserVerifiedPlugin,
)


@get('/user', apply=AuthenticatedPlugin(), skip=UserVerifiedPlugin)
def fetch_authenticated_user():
    """Fetch authenticated user."""
    return AuthenticatedUserSchema().dump(request.user).data


@patch('/user', apply=AuthenticatedPlugin(), skip=UserVerifiedPlugin)
def update_authenticated_user():
    """Update one or multiple fields of the authenticated user."""
    # Load update request data
    user_info = AuthenticatedUserSchema(
        strict=True,
    ).load(request.json, partial=False).data

    if any(k in user_info for k in USER_READ_ONLY_FIELDS):
        abort(httplib.FORBIDDEN,
              "These fields are read-only: " + ', '.join(USER_READ_ONLY_FIELDS))

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
    return AuthenticatedUserSchema().dump(local.model.get_user(request.user.user_id)).data


def allowed_user_schema():
    """Return schema with more fields if authenticated user is root."""
    if request.user.user_id == local.model.root_user_id:
        return AuthenticatedUserSchema
    else:
        return UserSchema


@get('/users/<user_spec>')
def fetch_user(user_spec):
    """Fetch a single user."""
    user = local.model.get_user(user_id=user_spec)
    user = user or local.model.get_user(username=user_spec)
    if user is None:
        abort(httplib.NOT_FOUND, "User %s not found" % user_spec)
    return allowed_user_schema()().dump(user).data


@get('/users')
def fetch_users():
    """
    Fetch list of users, filterable by username and email.

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
    return allowed_user_schema()(many=True).dump(users).data


@patch('/users')
def update_users():
    """
    Update arbitrary users.

    This operation is reserved for the root user. Other users can update their
    information through the /user "authenticated user" API.
    Follows the bulk-update convention in the CodaLab API, but currently only
    allows one update at a time.
    """
    if request.user.user_id != local.model.root_user_id:
        abort(httplib.FORBIDDEN, "Only root user can update other users.")

    users = AuthenticatedUserSchema(
        strict=True, many=True
    ).load(request.json, partial=True).data

    if len(users) != 1:
        abort(httplib.BAD_REQUEST, "Users can only be updated on at a time.")

    local.model.update_user_info(users[0])

    # Return updated users
    users = local.model.get_users(user_ids=[users[0]['user_id']])
    return AuthenticatedUserSchema(many=True).dump(users).data
