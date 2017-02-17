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
    if 'support_email' not in local.config:
        print >>sys.stderr, 'Warning: No support_email configured, so no email sent.'
        print >>sys.stderr, 'User\'s message: %s' % message
        return

    support_email = local.config['support_email']
    username = request.user.user_name
    user_email = request.user.email

    first_name = request.user.first_name if request.user.first_name else ''
    last_name = request.user.last_name if request.user.last_name else ''
    real_name = ("%s %s" % (first_name, last_name))
    message = message.encode('utf-8')

    local.emailer.send_email(
        subject="Message from %s" % user_email,
        body=template('help_message_to_codalab_body', username=username, email=user_email, message=message),
        recipient=support_email,
        sender=user_email,
        charset='utf-8',
    )
