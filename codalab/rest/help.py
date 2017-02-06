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
    user = AuthenticatedUserSchema().dump(request.user).data
    support_email = local.config['support_email']
    username = user['data']['attributes']['user_name']
    message = request.json['message']
    user_email = user['data']['attributes']['email']

    local.emailer.send_email(
        subject="Message from %s" % user_email,
        body=template('help_message_to_codalab_body', username=username, email=support_email, message=message, sender=user_email),
        recipient=support_email,
    )
    return '{}'
