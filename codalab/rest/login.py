"""
Login views.
"""
from datetime import datetime, timedelta
from urllib import urlencode
from urlparse import urlparse

from bottle import request, response, template, local, redirect, route, default_app, get, post

from codalab.lib.server_util import get_random_string


class SessionCookie(object):
    KEY = "codalab_session"
    _SECRET = None

    def __init__(self, user_id, max_age):
        self.user_id = user_id
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


@get('/logout', name='logout')
def do_logout():
    response.delete_cookie("user_id")
    redirect_uri = request.query.get('redirect_uri')
    if redirect_uri:
        return redirect(redirect_uri)
    else:
        return "<p>Successfully signed out from CodaLab.</p>"


@get('/login', name='login')
def show_login():
    return template("login", error=None)


@post('/login')
def do_login():
    redirect_uri = request.query.get('redirect_uri')
    username = request.forms.get('username')
    password = request.forms.get('password')

    user = local.model.get_user(username=username)
    if user.check_password(password):
        response.set_cookie(SessionCookie.KEY, SessionCookie(user.user_id, 3600),
                            secret=SessionCookie.get_secret(), max_age=3600)
        if redirect_uri:
            return redirect(redirect_uri)
        else:
            return "<p>Successfully signed into CodaLab.</p>"
    else:
        return template("login", redirect_uri=redirect_uri, error="Login/password did not match.")


# The other way to do this is to write a Plugin and add it to the "apply" param to the authorize view function
def require_login(callback):
    def wrapper(*args, **kwargs):
        """Check that user is defined on session cookie"""
        session = request.get_cookie(SessionCookie.KEY, secret=SessionCookie.get_secret(), default=None)
        if session:
            local.user = local.model.get_user(user_id=session.user_id)
        else:
            # Make sure X-Forwarded-Host is set properly if behind reverse-proxy to use request.url
            redirect("%s?%s" % ('/login', urlencode({"redirect_uri": request.url})))

        return callback(*args, **kwargs)

    return wrapper
