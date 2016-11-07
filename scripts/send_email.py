description = """
This is an emailing script that sends emails to users that have signed up to recieve
notifications on codalab.
"""
import sys, argparse
sys.path.append(".")
from bottle import local, template
from sqlalchemy import select
from codalab.model.tables import user as cl_user
from codalab.lib.codalab_manager import CodaLabManager

manager = CodaLabManager()
model = manager.model()
CODALAB_HOME = manager.codalab_home

# def send_email(email, subject, body):
    # local.emailer.send_email(
        # subject=subject,
        # body=body,
        # recipient=email
    # )

# send_email("ysavani@stanford.edu", "Hello, World!", "Try this on for size.")

def get_name_email_list():
    with model.engine.begin() as bundle_db:
        # Get set of emails in bundles db
        bundle_emails = bundle_db.execute(
                select([cl_user.c.user_name, cl_user.c.email])
                .where(cl_user.c.send_some_notifications == 1)) .fetchall()
        return [x for x in bundle_emails if x[1] != '']

def get_sent_list(sent_file):
    with open(sent_file) as f:
        return [line.split()[1][1:-1] for line in f]


def main(args):
    name_email_list = get_name_email_list()
    email_list = [email for name, email in name_email_list]
    if(len(sys.argv) == 1):
        for name, email in name_email_list:
            print "%s <%s>"%(name,email)
        return
    body = template(args.html_file) if args.html_file else ""
    subject = args.subject or "none"
    sent_list = get_sent_list(args.sent_file) if args.sent_file else []
    sender_list = list(set(email_list).difference(set(sent_list)))
    if args.doit:
        for email in sender_list:
            manager.emailer.send_email(
                subject=subject,
                body=body,
                recipient=email
            )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("-v", "--verbose",
                        help="increase output verbosity",
                        action="store_true",
                        default=False)
    parser.add_argument("--doit",
                        help="Actually sends the notification emails",
                        action="store_true",
                        default=False)
    parser.add_argument("--subject",
                        help="Subject of email",
                        action="store",
                        dest="subject")
    parser.add_argument("--body",
                        help="Template of email to be sent",
                        action="store",
                        dest="html_file")
    parser.add_argument("--sent",
                        help="File containing emails addresses we have sent to",
                        action="store",
                        dest="sent_file")
    args = parser.parse_args()
    main(args)
