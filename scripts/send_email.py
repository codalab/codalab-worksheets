"""send_email.py

This is an emailing script that sends emails to users that have signed up to recieve
notifications on codalab.

Example:
        $ python scripts/send_email.py -a --subject "Hello World"
          --body body.html --sent sent_file.txt --doit
"""

import sys
import argparse
sys.path.append('.')
from sqlalchemy import select
from codalab.model.tables import user as cl_user
from codalab.lib.codalab_manager import CodaLabManager

manager = CodaLabManager()
model = manager.model()
CODALAB_HOME = manager.codalab_home

def get_name_email_list(get_all):
    """Gets names and emails from mysql database using sqlalchemy
    
    Args:
        get_all (bool): whether to get users that requested all emails to be sent
            to them

    Returns:
        list of tuples with name and email of users
    """
    with model.engine.begin() as bundle_db:
        # Get set of emails in bundles db
        bundle_emails = bundle_db.execute(
            select([cl_user.c.user_name, cl_user.c.email])
            .where(cl_user.c.send_notifications_flag%2 if get_all else\
                   cl_user.c.send_notifications_flag != 0)).fetchall()
        return [(name, email) for name, email in bundle_emails if email != '']

def get_sent_list(sent_file):
    """Gets list of tuples with name and email of users that emails have been sent to

    Args:
        sent_file (string): name of file that has the list

    Returns:
        list of tuples with name and email of users
    """
    with open(sent_file) as s_file:
        return [(line.split()[0], line.split()[1][1:-1]) for line in s_file]


def main(args):
    name_email_list = get_name_email_list(args.all)
    if len(sys.argv) == 1 or (len(sys.argv) == 2 and args.all):
        for name, email in name_email_list:
            print '%s <%s>' % (name, email)
        return
    body = template(args.html_file) if args.html_file else ''
    subject = args.subject or 'none'
    sent_list = get_sent_list(args.sent_file) if args.sent_file else []
    sender_list = list(set(name_email_list).difference(set(sent_list)))
    if args.doit:
        with open(args.sent_file or 'sent_list.txt', 'a') as sent_list:
            index = 0
            for name, email in sender_list:
                manager.emailer.send_email(
                    subject=subject,
                    body=body,
                    recipient=email
                )
                sent_list.write('%s <%s>\n' % (name, email))
                sent_list.flush()
                index += 1
                print "Sent %d of %d emails. %d remaining." %\
                    (index, len(sender_list), len(sender_list)-index)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='''
        This is an emailing script that sends emails to
        users that have signed up to recieve
        notifications on codalab.
        ''')
    parser.add_argument('-v', '--verbose',
                        help='increase output verbosity',
                        action='store_true',
                        default=False)
    parser.add_argument('-a', '--all',
                        help='Select emails of users that have\
                        asked for all emails to be sent',
                        action='store_true',
                        default=False)
    parser.add_argument('--doit',
                        help='Actually sends the notification emails',
                        action='store_true',
                        default=False)
    parser.add_argument('--subject',
                        help='Subject of email',
                        action='store',
                        dest='subject')
    parser.add_argument('--body',
                        help='Template of email to be sent',
                        action='store',
                        dest='html_file')
    parser.add_argument('--sent',
                        help='File containing emails addresses we have sent to',
                        action='store',
                        dest='sent_file')
    args = parser.parse_args()
    main(args)
