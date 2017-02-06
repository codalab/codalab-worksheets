import httplib

from bottle import abort, get, request, local, post, template

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

@post('/help/', apply=AuthenticatedPlugin(), skip=UserVerifiedPlugin)
def fetch_help():
    # import pdb; pdb.set_trace()
    user = AuthenticatedUserSchema().dump(request.user).data
    email = user['data']['attributes']['email']
    username = user['data']['attributes']['user_name']
    message = request.json['message']

    local.emailer.send_email(
        subject="",
        body=template('help_message_to_codalab_body', username=username, email=email, message=message),
        recipient=email,
    )
    return '{}'
