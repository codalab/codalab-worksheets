"""
Login views.
"""
from datetime import datetime, timedelta
from urllib import urlencode
from urlparse import urlparse

from bottle import request, response, template, local, redirect, route, default_app, get, post

from codalab.lib import spec_util
from codalab.lib.server_util import get_random_string


class LoginSession(object):
    """
    Represents the user's session after logging in, usually stored as a cookie.
    """
    KEY = "codalab_session"
    _SECRET = None

    def __init__(self, user_id, max_age):
        self.user_id = user_id
        self.max_age = max_age
        self.expires = datetime.utcnow() + timedelta(seconds=max_age)

    @classmethod
    def get_secret(cls):
        """
        Return the current (lazily-generated) secret for signing cookies.
        This secret only lasts as long as the REST server process is running.
        If the server is restarted, the old cookies will be lost and users will
        have to log in again, but this is not a big deal since the session cookies
        are meant to be relatively short-lived anyway.
        :return:
        """
        if cls._SECRET is None:
            cls._SECRET = get_random_string(30, '+/abcdefghijklmnopqrstuvwxyz'
                                                'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
        return cls._SECRET

    def save(self, response=response):
        """
        Save session as cookie on the Bottle response object.
        :param response: Optional. A specific Bottle response object to save cookie on.
        :return: None
        """
        response.set_cookie(self.KEY, self, secret=self.get_secret(), max_age=self.max_age)

    @classmethod
    def get(cls, request=request):
        """
        Get session from cookie on the Bottle request object.
        Will only return session if exists and not expired yet.

        :param request: Optional. A specific Bottle request object to get cookie from.
        :return: LoginSession or None
        """
        session = request.get_cookie(cls.KEY, secret=cls.get_secret(), default=None)
        if session and session.expires > datetime.utcnow():
            return session
        else:
            return None

    @classmethod
    def clear(cls, response=response):
        """
        Delete session cookie on the Bottle response object.

        :param response: Optional. A specific Bottle response object to delete cookie from.
        :return: None
        """
        response.delete_cookie(cls.KEY)


class AuthenticationPlugin(object):
    """Bottle plugin that ensures that the client is authenticated.
    local.user is set to the authenticated User if authenticated,
    otherwise the client is redirected to the sign-in page.
    """
    api = 2
    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            session = LoginSession.get()
            if session:
                local.user = local.model.get_user(user_id=session.user_id)
            else:
                # Make sure X-Forwarded-Host is set properly if behind reverse-proxy to use request.url
                redirect(default_app().get_url('login', redirect_uri=request.url))
            return callback(*args, **kwargs)
        return wrapper


@get('/logout', name='logout')
def do_logout():
    LoginSession.clear()
    redirect_uri = request.query.get('redirect_uri')
    if redirect_uri:
        return redirect(redirect_uri)
    else:
        return redirect(default_app().get_url('success', message="Successfully signed out from CodaLab."))


@get('/login', name='login')
def show_login():
    return template("login")


@post('/login')
def do_login():
    redirect_uri = request.query.get('redirect_uri')
    username = request.forms.get('username')
    password = request.forms.get('password')

    user = local.model.get_user(username=username)
    if user.check_password(password):
        session = LoginSession(user.user_id, max_age=3600)
        session.save()
        if redirect_uri:
            return redirect(redirect_uri)
        else:
            return redirect(default_app().get_url('success', message="Successfully signed into CodaLab."))
    else:
        return template("login", errors=["Login/password did not match."])


@get('/success', name='success')
def show_success():
    message = request.query.get('message', '') or request.params.get('message', '')
    return template('success', message=message)


@get('/signup', name='signup')
def show_signup():
    return template('signup')


@post('/signup')
def do_signup():
    username = request.forms.get('username')
    password = request.forms.get('password')
    email = request.forms.get('email')

    errors = []
    if request.forms.get('confirm_password') != password:
        errors.append("Passwords do not match.")

    if not spec_util.NAME_REGEX.match(username):
        errors.append("Username must only contain letter, digits, hyphens, underscores, and periods.")

    # Only do a basic validation of email -- the only guaranteed way to check
    # whether an email address is valid is by sending an actual email.
    if not spec_util.BASIC_EMAIL_REGEX.match(email):
        errors.append("Email address is invalid.")

    if local.model.user_exists(username, email):
        errors.append("User with this username or email already exists.")

    if errors:
        return template('signup', errors=errors)

    # Create unverified user
    user_id, verification_key = local.model.add_user(username, email, password)

    # Send verification key to given email address
    hostname = request.get_header('Host')
    local.emailer.send_email(
        subject="Verify your new CodaLab account",
        body=template('email_verification_body', user=username, current_site=hostname, key=verification_key),
        recipient=email,
    )

    # Redirect to success page
    return redirect(default_app().get_url(
        'success',
        message="Thank you for signing up for a CodaLab account! "
                "A link to verify your account has been sent to %s." % email)
    )


@get('/verify')
def do_verify():
    if local.model.verify_user(request.query['key']):
        return redirect(default_app().get_url('login', message="Account verified! Please login."))
    else:
        return "Invalid verification key. Please try copying the link directly from your email."






