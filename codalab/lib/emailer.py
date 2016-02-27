""" Emailer """

from email.mime.text import MIMEText
import smtplib
import sys


class Emailer(object):
    def send_email(self, subject, body, recipient, sender=None):
        raise NotImplementedError


class SMTPEmailer(Emailer):
    def __init__(self, host, user, password, default_sender, server_email, port=587, use_tls=True):
        """
        :param host: SMTP server hostname
        :param user: SMTP user name
        :param password:  SMTP password
        :param default_sender: default 'From' header
        :param server_email: email address to send from
        :param port: SMTP server port
        :param use_tls: boolean True iff use TLS encryption
        :return:
        """
        self.host = host
        self.user = user
        self.password = password
        self.default_sender = default_sender
        self.server_email = server_email
        self.port = port
        self.use_tls = use_tls

    def send_email(self, subject, body, recipient, sender=None):
        """
        Send email

        TODO(skoo): allow recipients of the form "Codalab <codalab@codalab.org>"

        :param subject: subject of email
        :param body: body of email
        :param recipient: recipient of email, must be valid email address
        :param sender: optional alternative 'From' header
        :return:
        """
        mail_server = smtplib.SMTP(self.host, self.port)
        mail_server.ehlo()

        if self.use_tls:
            # All following commands will be encrypted
            mail_server.starttls()
            mail_server.ehlo()

        mail_server.login(self.user, self.password)

        message = MIMEText(body)
        message["From"] = sender or self.default_sender
        message["To"] = recipient
        message["Subject"] = subject

        mail_server.sendmail(self.server_email, recipient, message.as_string())
        mail_server.close()


class ConsoleEmailer(Emailer):
    def __init__(self, out=sys.stdout):
        """

        :param out: File object to write to
        :return:
        """
        self.out = out

    def send_email(self, subject, body, recipient, sender=None):
        print >>self.out, "From: %s" % (sender or 'console')
        print >>self.out, "To: %s" % recipient
        print >>self.out, "Subject: %s" % subject
        print >>self.out
        print >>self.out, body

