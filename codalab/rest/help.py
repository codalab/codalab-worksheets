import sys

from bottle import request, local, post, template

from codalab.server.authenticated_plugin import (
    AuthenticatedPlugin,
    UserVerifiedPlugin,
)


@post('/help/', apply=AuthenticatedPlugin(), skip=UserVerifiedPlugin)
def send_help_message():
    message = request.json['message']

    if 'server' in local.config and 'support_email' not in local.config['server']:
        print >>sys.stderr, 'Warning: No support_email configured, so no email sent.'
        print >>sys.stderr, 'User\'s message: %s' % message
        return

    support_email = local.config['server']['support_email']
    username = request.user.user_name
    user_email = request.user.email

    first_name = request.user.first_name if request.user.first_name else ''
    last_name = request.user.last_name if request.user.last_name else ''
    real_name = ("%s %s" % (first_name, last_name))
    message = message.encode('utf-8')

    local.emailer.send_email(
        subject="Message from %s" % user_email,
        body=template('help_message_to_codalab_body', real_name=real_name, username=username, email=user_email, message=message),
        recipient=support_email,
        sender=user_email,
        charset='utf-8',
    )
