"""
Sends email notifications to users with notifications set to a certain
threshold.  Keeps track of partial progress in a file.

Actually send emails:

    python scripts/send-email-notifications.py \
        --subject "Fall 2020 CodaLab Worksheets Newsletter" \
        --body-file docs/blog/2020-fall.md \
        --sent-file sent.jsonl \
        --threshold 2 \
        --doit

Test sending emails:

    python scripts/send-email-notifications.py \
        --subject "[TEST] Fall 2020 CodaLab Worksheets Newsletter" \
        --body-file docs/blog/2020-fall.md \
        --sent-file sent.jsonl \
        --emails codalab.worksheets@gmail.com \
        --threshold 2 \
        --doit
"""

# # A notifications value is one of the following:
# NOTIFICATIONS_NONE = 0x00  # Receive no notifications
# NOTIFICATIONS_IMPORTANT = 0x01  # Receive only important notifications
# NOTIFICATIONS_GENERAL = 0x02  # Receive general notifications (new features)

import argparse
import json
import os
import sys
import time
import markdown2

from sqlalchemy import select
from codalab.lib.codalab_manager import CodaLabManager
from codalab.model.tables import user as cl_user

sys.path.append('.')
HEADER = ['user_name', 'email', 'first_name', 'last_name', 'notifications']


def get_to_send_list(model, threshold):
    """Returns the list of (name, email) tuples with a given threshold.
    These are the ones we need to send to."""
    with model.engine.begin() as conn:
        rows = conn.execute(
            select(
                [
                    cl_user.c.user_name,
                    cl_user.c.email,
                    cl_user.c.first_name,
                    cl_user.c.last_name,
                    cl_user.c.notifications,
                ]
            ).where(cl_user.c.notifications >= threshold)
        ).fetchall()
        return [dict(list(zip(HEADER, row))) for row in rows if row.email]


def get_sent_list(sent_file):
    """Return the list of (name, email) tuples that we've already sent to."""
    results = []
    if os.path.exists(sent_file):
        for line in open(sent_file):
            info = json.loads(line)
            results.append(info)
    return results


def main(args):
    manager = CodaLabManager()

    # Get the the message
    subject = args.subject
    with open(args.body_file) as f:
        body_template = f.read()

    if args.body_file.endswith('.md'):
        body_template = f"""
            <div style='margin:auto; width: 100%; max-width: 600px'>
                <img src=https://worksheets.codalab.org/img/codalab-logo.png style='max-width: 100%;' />
                <h1>CodaLab Worksheets</h1>
                {markdown2.markdown(body_template)}<br><br>
                <small>If you'd like stop receiving these emails, please <a href='https://worksheets.codalab.org/account/profile'>update your account settings on CodaLab</a>.</small>
            </div>
        """

    mime_type = (
        'html' if args.body_file.endswith('.html') or args.body_file.endswith('.md') else 'plain'
    )

    # Figure out who we want to send
    to_send_list = (
        [
            {'email': e, 'first_name': '', 'last_name': '', 'user_name': '', 'notifications': 2}
            for e in args.emails.split(",")
        ]
        if args.emails
        else get_to_send_list(manager.model(), args.threshold)
    )
    sent_list = get_sent_list(args.sent_file)
    sent_emails = set(info['email'] for info in sent_list)
    pending_to_send_list = [info for info in to_send_list if info['email'] not in sent_emails]
    print('Already sent %d emails, %d to go' % (len(sent_list), len(pending_to_send_list)))

    for i, info in enumerate(pending_to_send_list):
        if args.only_email and args.only_email != info['email']:
            continue

        # Derived fields
        info['greeting_name'] = info['first_name'] or info['last_name'] or info['user_name']
        info['full_name'] = ' '.join([x for x in [info['first_name'], info['last_name']] if x])
        info['email_description'] = (
            '%s <%s>' % (info['full_name'], info['email']) if info['full_name'] else info['email']
        )
        info['sent_time'] = time.time()

        print(
            (
                'Sending %s/%s (%s>=%s, doit=%s): [%s] %s'
                % (
                    i,
                    len(pending_to_send_list),
                    info['notifications'],
                    args.threshold,
                    args.doit,
                    info['user_name'],
                    info['email_description'],
                )
            )
        )

        # Apply template to get body of message
        body = body_template
        for field, value in info.items():
            body = body.replace('{{' + field + '}}', str(value or ''))

        if args.verbose >= 1:
            print('To      : %s' % info['email_description'])
            print('Subject : %s' % subject)
            print(body)
            print('-------')

        if not args.doit:
            continue

        # Send the actual email
        manager.emailer.send_email(
            recipient=info['email_description'], subject=subject, body=body, mime_type=mime_type
        )

        # Record that we sent
        with open(args.sent_file, 'a') as f:
            print(json.dumps(info), file=f)
            f.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--threshold', help='Send emails to people with this threshold', type=int, required=True
    )
    parser.add_argument('--doit', help='Actually sends emails', action='store_true')
    parser.add_argument('--subject', help='Subject of email', required=True)
    parser.add_argument(
        '--body-file', help='File containing body of email to be sent', required=True
    )
    parser.add_argument('--emails', help='List of emails to send to (only used for testing)')
    parser.add_argument(
        '--sent-file',
        help='File that keeps track of who we\'ve already sent email to ',
        required=True,
    )
    parser.add_argument('--only-email', help='Only send to this email')
    parser.add_argument('--verbose', help='Show more information', type=int, default=0)
    args = parser.parse_args()
    main(args)
