"""
Handles reading the session cookie.
"""
import datetime

from bottle import (
  local,
  request,
  response
)

from codalab.server.authenticated_plugin import user_is_authenticated


class LoginCookie(object):
    """
    Represents the user's session cookie after logging in.
    """
    KEY = "codalab_session"
    PATH = "/"

    def __init__(self, user_id, max_age):
        self.user_id = user_id
        self.max_age = max_age
        self.expires = datetime.datetime.utcnow() + datetime.timedelta(seconds=max_age)

    def save(self):
        """
        Save cookie on the Bottle response object.
        """
        self.clear()
        response.set_cookie(
            self.KEY, self, secret=local.config['server']['secret_key'],
            max_age=self.max_age, path=self.PATH)

    @classmethod
    def get(cls):
        """
        Get cookie on the Bottle request object.
        Will only return cookie if exists and not expired yet.

        :return: LoginCookie or None
        """
        cookie = request.get_cookie(
            cls.KEY, secret=local.config['server']['secret_key'], default=None)
        if cookie and cookie.expires > datetime.datetime.utcnow():
            return cookie
        else:
            return None

    @classmethod
    def clear(cls):
        """
        Delete cookie on the Bottle response object.
        """
        response.delete_cookie(cls.KEY, path=cls.PATH)


class CookieAuthenticationPlugin(object):
    """
    Bottle plugin that checks the cookie and populates request.user if it is
    found and valid.
    """
    api = 2

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            if not user_is_authenticated():
                cookie = LoginCookie.get()
                if cookie:
                    request.user = local.model.get_user(user_id=cookie.user_id)
                else:
                    request.user = None

            return callback(*args, **kwargs)

        return wrapper
