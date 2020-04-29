from bottle import abort, httplib
from marshmallow import ValidationError

from codalab.common import CODALAB_VERSION
from codalab.lib.server_util import query_get_bool, json_api_meta


class JsonApiPlugin(object):
    """
    Captures marshmallow ValidationErrors and returns the appropriate result.
    """

    api = 2

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            try:
                result = callback(*args, **kwargs)
                # If response is JSON, add server version to meta
                if isinstance(result, dict):
                    json_api_meta(result, {'version': CODALAB_VERSION})
                return result
            except ValidationError as err:
                format_errors = query_get_bool('format_errors', default=False)
                if format_errors:
                    msg = err.messages
                else:
                    msg = '\n'.join([e['detail'] for e in err.messages['errors']])
                abort(httplib.BAD_REQUEST, msg)

        return wrapper
