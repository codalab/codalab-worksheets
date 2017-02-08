import httplib

from bottle import abort, get, request, local, post, template

from codalab.lib.spec_util import NAME_REGEX
from codalab.lib.server_util import bottle_patch as patch
from codalab.server.authenticated_plugin import (
    AuthenticatedPlugin,
    UserVerifiedPlugin,
)

@post('/help/', apply=AuthenticatedPlugin(), skip=UserVerifiedPlugin)
def fetch_help():
    support_email = local.config['support_email']
    username = request.user.user_name
    message = request.json['message']
    user_email = request.user.email

    local.emailer.send_email(
        subject="Message from %s" % user_email,
        body=template('help_message_to_codalab_body', username=username, email=support_email, message=message),
        recipient=support_email,
        sender=user_email
    )
