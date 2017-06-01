"""
Utility functions used by the server applications.
Don't import from non-REST API code, since this file imports bottle.
"""
from functools import wraps
import base64
import httplib
import sys
import threading
import time
import urllib

from bottle import abort, request, HTTPResponse, redirect, app
from oauthlib.common import to_unicode, bytes_type

from codalab.common import precondition


def exc_frame_locals():
    """
    Returns dict of local variables in the frame where exception was raised.
    Returns empty dict if no traceback available.

    Based on http://stackoverflow.com/a/5328139
    """
    _, _, tb = sys.exc_info()

    if tb is None:
        return {}

    # Traverse traceback (a linked-list) to get the last frame
    next_tb = tb.tb_next
    while next_tb is not None:
        tb = next_tb
        next_tb = tb.tb_next

    return tb.tb_frame.f_locals


class RateLimitExceededError(Exception):
    pass


def rate_limited(max_calls_per_hour):
    """
    Parameterized decorator for rate-limiting a function.

    A running count of remaining calls allowed is kept for the last hour.
    Every call beyond this limit will raise a RateLimitExceededError.
    """
    def decorate(func):
        lock = threading.Lock()
        state = {
            'calls_left': max_calls_per_hour,
            'time_of_last_call': time.time(),
        }

        @wraps(func)
        def rate_limited_function(*args, **kwargs):
            with lock:
                # Measure elapsed time since last call
                now = time.time()
                seconds_since_last_call = now - state['time_of_last_call']
                state['time_of_last_call'] = now
                
                # Increment the running count of allowed calls for the last
                # hour at a steady rate
                state['calls_left'] += seconds_since_last_call * (max_calls_per_hour / 3600)

                # Cap the count at the defined max
                if state['calls_left'] > max_calls_per_hour:
                    state['calls_left'] = max_calls_per_hour

                # No credit left - abort
                if state['calls_left'] < 1.0:
                    raise RateLimitExceededError

                # Debit the running count for this call
                state['calls_left'] -= 1

            return func(*args, **kwargs)

        return rate_limited_function

    return decorate


def query_get_list(key):
    """
    Get query parameters as a list of strings.
    See JsonApiClient._pack_params for how such a parameter value is assumed
    to be a constructed.
    """
    return request.query.getall(key)


def query_get_type(type_, key, default=None):
    value = request.query.get(key, None)
    if value is None:
        return default
    try:
        return type_(value)
    except ValueError:
        abort(httplib.BAD_REQUEST, "Invalid %s %r" % (type_.__name__, value))


def query_get_bool(key, default=False):
    value = request.query.get(key, None)
    if value is None:
        return default
    try:
        return bool(int(value))
    except ValueError:
        abort(httplib.BAD_REQUEST, '%r parameter must be integer boolean' % key)


def query_get_json_api_include_set(supported):
    """
    Get the set of related resources to include, as defined by
    http://jsonapi.org/format/#fetching-includes

    :param set[str] supported: set of supported resources to include
    """
    query_str = request.query.get('include', None)
    if query_str is None:
        return set()
    requested = set(query_str.split(','))
    if not requested <= supported:
        abort(httplib.BAD_REQUEST, '?include=%s not supported' % ','.join(list(requested - supported)))
    return requested


def json_api_meta(doc, meta_update):
    precondition(isinstance(meta_update, dict), "Meta data must be dict")
    meta = doc.setdefault('meta', {})
    meta.update(meta_update)
    return doc


def json_api_include(doc, schema, resources):
    if not isinstance(resources, list):
        resources = [resources]

    if 'included' not in doc:
        doc['included'] = []

    schema.many = True
    doc['included'].extend(schema.dump(resources).data['data'])
    return doc


def bottle_patch(path=None, **options):
    """Convenience decorator of the same form as @get and @post in the
    Bottle module.
    """
    return app().route(path, 'PATCH', **options)


def redirect_with_query(redirect_uri, params):
    """Return a Bottle redirect to the given target URI with query parameters
    encoded from the params dict.
    """
    return redirect(redirect_uri + '?' + urllib.urlencode(params))


"""
The following functions are adapted from flask_oauthlib.utils and are
:copyright: (c) 2013 - 2014 by Hsiaoming Yang.
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of flask-oauthlib nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

# BEGIN ADAPTED FROM flask_oauthlib.utils #


def extract_params(extract_body):
    """Extract request params."""
    uri = request.url
    http_method = request.method
    if extract_body:
        body = dict(request.forms)
    else:
        body = None
    headers = dict(request.headers)
    if 'wsgi.input' in headers:
        del headers['wsgi.input']
    if 'wsgi.errors' in headers:
        del headers['wsgi.errors']

    return uri, http_method, body, headers


def to_bytes(text, encoding='utf-8'):
    """Make sure text is bytes type."""
    if not text:
        return text
    if not isinstance(text, bytes_type):
        text = text.encode(encoding)
    return text


def decode_base64(text, encoding='utf-8'):
    """Decode base64 string."""
    text = to_bytes(text, encoding)
    return to_unicode(base64.b64decode(text), encoding)


def create_response(headers, body, status):
    """Create response class for Bottle."""
    return HTTPResponse(body or '', status=status, headers=headers)

# END ADAPTED FROM flask_oauthlib.utils #


"""
The following functions are adapted from werkzeug.utils and are
 :copyright: (c) 2014 by the Werkzeug Team
 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions are
 met:

     * Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

     * Redistributions in binary form must reproduce the above
       copyright notice, this list of conditions and the following
       disclaimer in the documentation and/or other materials provided
       with the distribution.

     * The names of the contributors may not be used to endorse or
       promote products derived from this software without specific
       prior written permission.

 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 Status API Training Shop Blog About Pricing
"""

# BEGIN ADAPTED FROM werkzeug.utils #


class cached_property(property):
    """A decorator that converts a function into a lazy property.  The
    function wrapped is called the first time to retrieve the result
    and then that calculated result is used the next time you access
    the value::
        class Foo(object):
            @cached_property
            def foo(self):
                # calculate something important here
                return 42
    The class has to have a `__dict__` in order for this property to
    work.
    """

    # implementation detail: A subclass of python's builtin property
    # decorator, we override __get__ to check for a cached value. If one
    # chooses to invoke __get__ by hand the property will still work as
    # expected because the lookup logic is replicated in __get__ for
    # manual invocation.

    def __init__(self, func, name=None, doc=None):
        self.__name__ = name or func.__name__
        self.__module__ = func.__module__
        self.__doc__ = doc or func.__doc__
        self.func = func

    def __set__(self, obj, value):
        obj.__dict__[self.__name__] = value

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        value = obj.__dict__.get(self.__name__, None)
        if value is None:
            value = self.func(obj)
            obj.__dict__[self.__name__] = value
        return value


def import_string(import_name, silent=False):
    """Imports an object based on a string.  This is useful if you want to
    use import paths as endpoints or something similar.  An import path can
    be specified either in dotted notation (``xml.sax.saxutils.escape``)
    or with a colon as object delimiter (``xml.sax.saxutils:escape``).
    If `silent` is True the return value will be `None` if the import fails.
    :param import_name: the dotted name for the object to import.
    :param silent: if set to `True` import errors are ignored and
                   `None` is returned instead.
    :return: imported object
    """
    # force the import name to automatically convert to strings
    # __import__ is not able to handle unicode strings in the fromlist
    # if the module is a package
    import_name = str(import_name).replace(':', '.')
    try:
        __import__(import_name)
    except ImportError:
        if '.' not in import_name:
            raise
    else:
        return sys.modules[import_name]

    module_name, obj_name = import_name.rsplit('.', 1)
    try:
        module = __import__(module_name, None, None, [obj_name])
    except ImportError:
        # support importing modules not yet set up by the parent module
        # (or package for that matter)
        module = import_string(module_name)

    try:
        return getattr(module, obj_name)
    except AttributeError as e:
        raise ImportError(e)

# END ADAPTED FROM werkzeug.utils #
