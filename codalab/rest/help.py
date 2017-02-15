import sys
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
    message = request.json['message']

    if local.config and 'support_email' not in local.config['server']:
        print >>sys.stderr, 'Warning: No support_email configured, so no email sent.'
        print >>sys.stderr, 'User\'s message: %s' % message
        return

    support_email = local.config['server']['support_email']
    username = request.user.user_name
    user_email = request.user.email
    real_name = ("%s %s" % (request.user.first_name, request.user.last_name))

    local.emailer.send_email(
        subject="Message from %s" % user_email,
        body=template('help_message_to_codalab_body', real_name=real_name, username=username, email=user_email, message=message),
        recipient=support_email,
        sender=user_email
    )
