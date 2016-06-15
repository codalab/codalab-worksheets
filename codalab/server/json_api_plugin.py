from bottle import abort, httplib
from marshmallow import ValidationError

from codalab.lib.server_util import query_get_bool


class JsonApiPlugin(object):
    """
    Captures marshmallow ValidationErrors and returns the appropriate result.
    """
    api = 2

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            try:
                return callback(*args, **kwargs)
            except ValidationError as err:
                format_errors = query_get_bool('format_errors', default=False)
                if format_errors:
                    msg = err.messages
                else:
                    msg = '\n'.join([e['detail'] for e in err.messages['errors']])
                abort(httplib.BAD_REQUEST, msg)

        return wrapper
