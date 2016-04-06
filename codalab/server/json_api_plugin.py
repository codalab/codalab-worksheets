"""
Utility functions for building and processing JSON API responses and requests.
"""
from bottle import response


class JsonApiPlugin(object):
    api = 2

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            response.content_type = 'application/vnd.api+json'
            return callback(*args, **kwargs)

        return wrapper
