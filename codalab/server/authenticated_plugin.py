from abc import ABC, abstractmethod
from bottle import abort, httplib, local, redirect, request, url

from codalab.lib.server_util import redirect_with_query
from codalab.objects.user import PUBLIC_USER

import os


class AuthPlugin(ABC):
    _NOT_AUTHENTICATED_ERROR = 'User is not authenticated.'
    _NOT_VERIFIED_ERROR = 'User is not verified.'
    _ACCESS_DENIED_ERROR = 'User has not been given access to this CodaLab instance. Please contact the admin for access.'

    api = 2

    @abstractmethod
    def apply(self, callback, route):
        pass

    def is_protected_mode(self):
        return os.environ.get('CODALAB_PROTECTED_MODE') == 'True'

    def user_is_authenticated(self):
        if os.getenv("CODALAB_TEST_USER"):
            # Only used for testing
            request.user = local.model.get_user(username=os.getenv("CODALAB_TEST_USER"))
        return (
            hasattr(request, 'user')
            and request.user is not None
            and request.user is not PUBLIC_USER
        )

    def check_user_verified(self):
        if self.user_is_authenticated() and not request.user.is_verified:
            if request.is_ajax:
                abort(httplib.UNAUTHORIZED, AuthPlugin._NOT_VERIFIED_ERROR)
            else:
                redirect(url('resend_key'))

    def check_user_authenticated(self):
        if not self.user_is_authenticated():
            if request.is_ajax:
                abort(httplib.UNAUTHORIZED, AuthPlugin._NOT_AUTHENTICATED_ERROR)
            else:
                redirect_with_query('/account/login', {'next': request.url})

    def check_has_access(self):
        if not request.user.has_access:
            if request.is_ajax:
                abort(httplib.UNAUTHORIZED, AuthPlugin._ACCESS_DENIED_ERROR)
            else:
                redirect_with_query('/account/login', {'next': request.url})


class UserVerifiedPlugin(AuthPlugin):
    """
    In all modes, fails the request if the user is authenticated but not verified.

    The handling of AJAX requests is the same as AuthenticatedPlugin.
    """

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            self.check_user_verified()
            return callback(*args, **kwargs)

        return wrapper


class PublicUserPlugin(AuthPlugin):
    """
    Sets request.user to PUBLIC_USER if none set yet.
    """

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            if not self.user_is_authenticated():
                request.user = PUBLIC_USER
            return callback(*args, **kwargs)

        return wrapper


class AuthenticatedPlugin(AuthPlugin):
    """
    In all modes, checks if the user is authenticated (i.e. request.user has been set).

    This method redirects the user to login unless the request is an AJAX request
    (i.e. has X-Requested-With header set to XMLHttpRequest). This header should
    be set by:
      - AJAX requests in the Javascript app.
      - Requests from the workers.
      - Requests from the CLI.
    """

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            self.check_user_authenticated()
            return callback(*args, **kwargs)

        return wrapper


class AuthenticatedProtectedPlugin(AuthPlugin):
    """
    In non-protected mode, checks if the user is authenticated.
    In protected mode, checks if the user is authenticated and has access to the instance.

    The handling of AJAX requests is the same as AuthenticatedPlugin.
    """

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            self.check_user_authenticated()
            if self.is_protected_mode():
                self.check_has_access()
            return callback(*args, **kwargs)

        return wrapper


class ProtectedPlugin(AuthPlugin):
    """
    In non-protected mode, grants anonymous access.
    In protected mode, checks if the user is authenticated and has access to the instance.

    The handling of AJAX requests is the same as AuthenticatedPlugin.
    """

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            if self.is_protected_mode():
                self.user_is_authenticated()
                self.check_has_access()
            return callback(*args, **kwargs)

        return wrapper
