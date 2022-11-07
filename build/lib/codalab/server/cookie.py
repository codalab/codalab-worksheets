"""
Handles reading the session cookie.
"""
import datetime
import json

from bottle import local, request, response

from codalab.server.authenticated_plugin import AuthPlugin


class LoginCookie(object):
    """
    Represents the user's session cookie after logging in.
    """

    KEY = "codalab_session"
    PATH = "/"

    def __init__(self, user_id, max_age, expires=None):
        self.user_id = user_id
        self.max_age = max_age
        self.expires = expires or (datetime.datetime.utcnow() + datetime.timedelta(seconds=max_age))

    def save(self):
        """
        Save cookie on the Bottle response object.
        """
        self.clear()
        response.set_cookie(
            self.KEY,
            self.serialize(),
            secret=local.config['server']['secret_key'],
            max_age=self.max_age,
            path=self.PATH,
        )

    def serialize(self):
        return json.dumps(
            {"user_id": self.user_id, "max_age": self.max_age, "expires": self.expires.timestamp()}
        )

    @classmethod
    def get(cls):
        """
        Get cookie on the Bottle request object.
        Will only return cookie if it exists and has not expired yet.

        :return: LoginCookie or None
        """
        try:
            cookie = request.get_cookie(
                cls.KEY, secret=local.config['server']['secret_key'], default=None
            )
        except UnicodeDecodeError:
            return None
        if not cookie:
            return None
        try:
            cookie = json.loads(cookie)
        except (TypeError, json.JSONDecodeError):
            # TypeError is raised when the cookie is stored in the old pickle format.
            return None
        cookie = LoginCookie(
            user_id=cookie["user_id"],
            max_age=cookie["max_age"],
            expires=datetime.datetime.fromtimestamp(cookie["expires"]),
        )
        if cookie.expires > datetime.datetime.utcnow():
            return cookie
        else:
            return None

    @classmethod
    def clear(cls):
        """
        Delete cookie on the Bottle response object.
        """
        response.delete_cookie(cls.KEY, path=cls.PATH)


class CookieAuthenticationPlugin(AuthPlugin):
    """
    Bottle plugin that checks the cookie and populates request.user if it is
    found and valid.
    """

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            if not self.user_is_authenticated():
                cookie = LoginCookie.get()
                if cookie:
                    request.user = local.model.get_user(user_id=cookie.user_id)
                else:
                    LoginCookie.clear()
                    request.user = None

            return callback(*args, **kwargs)

        return wrapper
