from abc import ABC, abstractmethod
from bottle import abort, httplib, redirect, request, url

from codalab.lib.server_util import redirect_with_query
from codalab.objects.user import PUBLIC_USER

import os

# TODO: delete later -tony
import logging

logger = logging.getLogger(__name__)


class AuthenticationPlugin(ABC):
    _NOT_AUTHENTICATED_ERROR = 'User is not authenticated'
    _NOT_VERIFIED_ERROR = 'User is not verified'
    _ACCESS_DENIED_ERROR = (
        'User is not given access to this CodaLab instance. Please contact the admin for access.'
    )

    api = 2

    @abstractmethod
    def apply(self, callback, route):
        pass

    def is_protected_mode(self):
        return os.environ.get('CODALAB_PROTECTED_MODE') == 'True'

    def user_is_authenticated(self):
        return (
            hasattr(request, 'user')
            and request.user is not None
            and request.user is not PUBLIC_USER
        )

    def check_user_verified(self):
        if self.user_is_authenticated() and not request.user.is_verified:
            if request.is_ajax:
                abort(httplib.UNAUTHORIZED, AuthenticationPlugin._NOT_VERIFIED_ERROR)
            else:
                redirect(url('resend_key'))

    def check_user_authenticated(self):
        if not self.user_is_authenticated():
            if request.is_ajax:
                abort(httplib.UNAUTHORIZED, AuthenticationPlugin._NOT_AUTHENTICATED_ERROR)
            else:
                redirect_with_query('/account/login', {'next': request.url})

    def check_has_access(self):
        # Only check if the user has access in protected mode
        if not self.is_protected_mode():
            return

        self.check_user_authenticated()
        if not request.user.has_access:
            if request.is_ajax:
                abort(httplib.UNAUTHORIZED, AuthenticationPlugin._ACCESS_DENIED_ERROR)
            else:
                redirect_with_query('/account/login', {'next': request.url})


class UserVerifiedPlugin(AuthenticationPlugin):
    """
    Fails the request if the user is authenticated but not verified.

    The handling of AJAX requests is the same as above for AuthenticatedPlugin.
    """

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            self.check_user_verified()
            return callback(*args, **kwargs)

        return wrapper


class PublicUserPlugin(AuthenticationPlugin):
    """
    Sets request.user to PUBLIC_USER if none set yet.
    """

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            if not self.user_is_authenticated():
                request.user = PUBLIC_USER
            return callback(*args, **kwargs)

        return wrapper


class AuthenticatedPlugin(AuthenticationPlugin):
    """
    Fails the request if the user is not authenticated (i.e. request.user hasn't
    been set).

    This method redirects the user to login unless the request is an AJAX request
    (i.e. has X-Requested-With header set to XMLHttpRequest). This header should
    be set by:
      - AJAX requests in the Javascript app.
      - Requests from the workers.
      - Requests from the CLI.
    """

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            # TODO: delete later -tony
            logger.info(
                'Tony - Inside AuthenticatedPlugin() - function={}, CODALAB_PROTECTED_MODE={}, authenticated={}, request.user={}'.format(
                    callback.__name__,
                    os.environ.get('CODALAB_PROTECTED_MODE'),
                    self.user_is_authenticated(),
                    str(request.user),
                )
            )
            self.check_user_authenticated()
            return callback(*args, **kwargs)

        return wrapper


class AuthenticatedProtectedPlugin(AuthenticationPlugin):
    # TODO: refactor and add docs -Tony
    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            self.check_user_authenticated()
            self.check_has_access()
            return callback(*args, **kwargs)

        return wrapper


class ProtectedPlugin(AuthenticationPlugin):
    # TODO: refactor and add docs -Tony
    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            # TODO: delete later -tony
            logger.info(
                'Tony - Inside ProtectedPlugin() - function={}, CODALAB_PROTECTED_MODE={}, authenticated={}, request.user={}'.format(
                    callback.__name__,
                    os.environ.get('CODALAB_PROTECTED_MODE'),
                    self.user_is_authenticated(),
                    str(request.user),
                )
            )
            self.check_has_access()
            return callback(*args, **kwargs)

        return wrapper
