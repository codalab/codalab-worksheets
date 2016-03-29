"""
Utility functions for building and processing JSON API responses and requests.
"""
import re

from bottle import HTTPError, request, response

from codalab.common import UsageError


FIELDSET_KEY_PATTERN = r'fields\[(\w+)\]'
FILTER_KEY_PATTERN = r'filter\[(\w+)\]'


def parse_includes(query):
    """Includes currently not supported."""
    if 'include' in query:
        raise HTTPError('400 Bad Request')
    return []


def parse_sorting(query):
    """Sorting currently not supported."""
    if 'sort' in query:
        raise HTTPError('400 Bad Request')
    return []


def parse_pagination(query):
    """Pagination currently not supported."""
    if any(k in query for k in ('first', 'last', 'prev', 'next')):
        raise HTTPError('400 Bad Request')
    return {}


def parse_filters(query):
    """Parse query parameters for filter specifications.

    http://jsonapi.org/recommendations/#filtering
    """
    filters = {}
    for key, value in query.items():
        m = re.match(FILTER_KEY_PATTERN, key)
        if m is None:
            continue
        resource_type = m.group(1)
        fields = set(value.split(','))
        filters[resource_type] = fields
    return filters


def parse_fieldsets(query):
    """
    Parses the query parameters for sparse fieldset requests.
    :param query: Bottle query parameters dict.
    :return: dict mapping resource type to sets of requested fields
    """
    fieldsets = {}
    for key, value in query.items():
        m = re.match(FIELDSET_KEY_PATTERN, key)
        if m is None:
            continue
        resource_type = m.group(1)
        fields = set(value.split(','))
        fields.add('id')  # ensure id is always included
        fieldsets[resource_type] = fields
    return fieldsets


def parse_query(query):
    return {
        'fields': parse_fieldsets(query),
        'include': parse_includes(query),
        'filter': parse_filters(query),
        'sort': parse_sorting(query),
        'page': parse_pagination(query),
    }


class JsonApiParameters(object):
    def __init__(self, query):
        self.fields = parse_fieldsets(query)
        self.include = parse_includes(query)
        self.filter = parse_filters(query)
        self.sort = parse_sorting(query)
        self.page = parse_pagination(query)


class JsonApiPlugin(object):
    """
    Parses standard JSON API query parameters.
    """
    api = 2

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            response.content_type = 'application/vnd.api+json'
            request.jsonapi = JsonApiParameters(request.query)
            try:
                return callback(*args, **kwargs)
            except UsageError as e:
                return {
                    'errors': [
                        {
                            'detail': str(e)
                        }
                    ]
                }

        return wrapper
