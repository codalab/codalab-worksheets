from bottle import (
  abort,
  httplib,
  redirect,
  request,
  url,
)

from codalab.lib.server_util import redirect_with_query


class AuthenticatedPlugin(object):
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
    api = 2

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            if not hasattr(request, 'user') or request.user is None:
                if request.is_ajax:
                    abort(httplib.UNAUTHORIZED, 'Not authorized')
                else:
                    redirect_with_query('/account/login', {'next': request.url})

            return callback(*args, **kwargs)

        return wrapper


class UserVerifiedPlugin(object):
    """
    Fails the request if the user is authenticated but not verified.

    The handling of AJAX requests is the same as above for AuthenticatedPlugin.
    """
    api = 2

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            if (hasattr(request, 'user') and request.user is not None and
                not request.user.is_verified):
                if request.is_ajax:
                    abort(httplib.UNAUTHORIZED, 'User is not verified')
                else:
                    redirect(url('resend_key'))

            return callback(*args, **kwargs)

        return wrapper
