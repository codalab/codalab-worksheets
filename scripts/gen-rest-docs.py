#!./venv/bin/python
"""
Generate REST docs.
"""
import sys
sys.path.append('.')
from inspect import isclass
from collections import defaultdict
import os
from shutil import rmtree

from bottle import default_app, template
from marshmallow import Schema
from marshmallow_jsonapi import Schema as JsonApiSchema
from textwrap import dedent


REST_DOCS_PATH = 'docs/rest'
SCHEMA_DOC_PATH = os.path.join(REST_DOCS_PATH, 'schemas.md')
INDEX_DOC_PATH = os.path.join(REST_DOCS_PATH, 'index.md')

from codalab.server import rest_server


# TODO
#  - create python string templates in Markdown for everything
#  - filter out unwanted things (rpc endpoint, account endpoints, Schema)
#  - output .md files into static/ ?


EXCLUDED_APIS = {'account', 'titlejs', 'api', 'static'}


def get_api_routes():
    # TODO: import modules individually and sort these routes by their
    # module name. This will keep Bundles and Bundle Permissions API together.
    # Then we can also load the docstrings of the modules themselves and print
    # those on each page.
    app = default_app()
    routes_by_root = defaultdict(list)
    for route in app.routes:
        path = route.rule.split('/')
        root = path[1]
        if root in EXCLUDED_APIS: continue
        routes_by_root[root].append(route)
    return routes_by_root


def get_codalab_schemas():
    from codalab.rest import schemas as schemas_module
    for k, v in vars(schemas_module).iteritems():
        if not isclass(v): continue
        if not issubclass(v, Schema): continue
        if v is Schema: continue
        if v is JsonApiSchema: continue
        yield k, v


SCHEMA_DOC = '''\
<%
    from marshmallow_jsonapi import Schema as JsonApiSchema
    from textwrap import dedent
    from codalab.lib import spec_util
%>
% if issubclass(schema, JsonApiSchema):
# {{schema.Meta.type_}}
% else:
# {{schema.__name__}}
% end

% if schema.__doc__:
{{dedent(schema.__doc__)}}
% end

Name | Type
--- | ---
% for field_name, field in schema._declared_fields.items():
<%
    attrs = vars(field)
    field_class = field.__class__
%>
`{{field_name}}` | {{field_class.__name__}}
% end
'''

# TODO: parse out things and convert them to markdown tables
# Search bundles
#
# More Markdown **description**
#
#   :param depth: the depth to search
#   :statuscode 400: invalid arguments
#   :request-resource bulk: bundles
#   :response-resource: bundles
#   :response: { JSON format }
#
# or maybe worksheet markdown style
#
#   % param depth integer 'the depth to search'
#   % statuscode 400 'invalid arguments'
#   % request-resource 'bundles' bulk
#   % response-resource 'bundles'
#   % response { JSON format }
API_ROUTE_DOC = '''\
<%
    from textwrap import dedent
    docstring = route.get_undecorated_callback().__doc__
%>
# `{{route.method}} {{!route.rule}}`
% if docstring:
{{dedent(docstring)}}
% end
'''


INDEX_DOC = '''\
# REST API Reference

This reference and the REST API itself is still under heavy development and is
subject to change at any time. Feedback through our GitHub issues is appreciated!

## Table of Contents
- [Resource Object Schemas](schemas.md)
- API Endpoints
% for root in route_roots:
% api_name = ' '.join(root.title().split('-'))
  - [{{api_name}} API]({{root}}.md)
% end
'''

INDEX_LINK = "&larr; [Back to Table of Contents](index.md)\n"


if __name__ == '__main__':
    rmtree(REST_DOCS_PATH)
    os.mkdir(REST_DOCS_PATH)

    for root, routes in get_api_routes().items():
        doc_path = os.path.join(REST_DOCS_PATH, root + '.md')
        api_name = ' '.join(root.title().split('-'))
        with open(doc_path, 'wb') as out:
            out.write('# %s API\n' % api_name)
            out.write(INDEX_LINK)
            for route in routes:
                out.write(template(dedent(API_ROUTE_DOC), route=route))
            out.write(INDEX_LINK)

    with open(SCHEMA_DOC_PATH, 'wb') as out:
        out.write(INDEX_LINK)
        for schema_name, schema in get_codalab_schemas():
            out.write(template(dedent(SCHEMA_DOC), schema=schema))
            for field_name, field in schema._declared_fields.items():
                print vars(field)
        out.write(INDEX_LINK)

    with open(INDEX_DOC_PATH, 'wb') as out:
        out.write(template(dedent(INDEX_DOC), route_roots=get_api_routes().keys()))

