"""
This module exports some simple names used throughout the CodaLab bundle system:
  - The various CodaLab error classes, with documentation for each.
  - The State class, an enumeration of all legal bundle states.
  - precondition, a utility method that check's a function's input preconditions.
"""
import http.client

# Increment this on master when ready to cut a release.
# http://semver.org/
CODALAB_VERSION = '0.5.22'
BINARY_PLACEHOLDER = '<binary>'
URLOPEN_TIMEOUT_SECONDS = 30


class IntegrityError(ValueError):
    """
    Raised by the model when there is a database integrity issue.

    Indicates a serious error that either means that there was a bug in the model
    code that left the database in a bad state, or that there was an out-of-band
    database edit with the same result.
    """


class PreconditionViolation(ValueError):
    """
    Raised when a value generated by one module fails to satisfy a precondition
    required by another module.

    This class of error is serious and should indicate a problem in code, but it
    it is not an AssertionError because it is not local to a single module.
    """


class UsageError(ValueError):
    """
    Raised when user input causes an exception. This error is the only one for
    which the command-line client suppresses output.
    """


class NotFoundError(UsageError):
    """
    Raised when a requested resource has not been found. Similar to HTTP status
    404.
    """


class AuthorizationError(UsageError):
    """
    Raised when access to a resource is refused because authentication is required
    and has not been provided. Similar to HTTP status 401.
    """


class PermissionError(UsageError):
    """
    Raised when access to a resource is refused because the user does not have
    necessary permissions. Similar to HTTP status 403.
    """


# Listed in order of most specific to least specific.
http_codes_and_exceptions = [
    (http.client.FORBIDDEN, PermissionError),
    (http.client.UNAUTHORIZED, AuthorizationError),
    (http.client.NOT_FOUND, NotFoundError),
    (http.client.BAD_REQUEST, UsageError),
]


def exception_to_http_error(e):
    """
    Returns the appropriate HTTP error code and message for the given exception.
    """
    for known_code, exception_type in http_codes_and_exceptions:
        if isinstance(e, exception_type):
            return known_code, str(e)
    return http.client.INTERNAL_SERVER_ERROR, str(e)


def http_error_to_exception(code, message):
    """
    Returns the appropriate exception for the given HTTP error code and message.
    """
    for known_code, exception_type in http_codes_and_exceptions:
        if code == known_code:
            return exception_type(message)
    if code >= 400 and code < 500:
        return UsageError(message)
    return Exception(message)


def precondition(condition, message):
    if not condition:
        raise PreconditionViolation(message)


def ensure_str(response):
    """
    Ensure the data type of input response to be string
    :param response: a response in bytes or string
    :return: the input response in string
    """
    if isinstance(response, str):
        return response
    try:
        return response.decode()
    except UnicodeDecodeError:
        return BINARY_PLACEHOLDER
