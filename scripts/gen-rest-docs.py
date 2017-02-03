#!./venv/bin/python
"""
Generate REST docs.
"""
import sys
sys.path.append('.')
from inspect import isclass
from collections import defaultdict
import os

from bottle import default_app, template
from marshmallow import Schema
from marshmallow_jsonapi import Schema as JsonApiSchema
from textwrap import dedent

from codalab.server import rest_server


REST_DOCS_PATH = 'docs/rest.md'
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



INDEX_DOC = '''\
# REST API Reference

This reference and the REST API itself is still under heavy development and is
subject to change at any time. Feedback through our GitHub issues is appreciated!

## Table of Contents
- [Resource Object Schemas](#resource-object-schemas)
- [API Endpoints](#api-endpoints)
% for root in route_roots:
% api_name = ' '.join(root.title().split('-'))
  - [{{api_name}} API](#{{root}}-api)
% end

'''


SCHEMA_DOC = '''\
<%
    from marshmallow_jsonapi import Schema as JsonApiSchema
    from textwrap import dedent
    from codalab.lib import spec_util
%>
% if issubclass(schema, JsonApiSchema):
## {{schema.Meta.type_}}
% else:
## {{schema.__name__}}
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
% if field_class.__name__ == 'Nested':
    `{{field_name}}` | {{field.nested.__name__}}
% else:
    `{{field_name}}` | {{field_class.__name__}}
%end
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
### `{{route.method}} {{!route.rule}}`
% if docstring:
{{dedent(docstring)}}
% end
'''


INDEX_LINK = "\n&uarr; [Back to Top](#table-of-contents)\n"


if __name__ == '__main__':
    if not os.path.exists(os.path.dirname(REST_DOCS_PATH)):
        os.makedirs(os.path.dirname(REST_DOCS_PATH))

    with open(REST_DOCS_PATH, 'wb') as out:
        out.write(template(dedent(INDEX_DOC), route_roots=get_api_routes().keys()))

        out.write('# Resource Object Schemas\n')
        for schema_name, schema in get_codalab_schemas():
            out.write(template(dedent(SCHEMA_DOC), schema=schema))
            # For debugging.
            # for field_name, field in schema._declared_fields.items():
            #     print vars(field)
        out.write(INDEX_LINK)

        out.write('# API Endpoints\n')
        for root, routes in get_api_routes().items():
            doc_path = os.path.join(REST_DOCS_PATH, root + '.md')
            api_name = ' '.join(root.title().split('-'))
            out.write('## %s API\n' % api_name)
            for route in routes:
                out.write(template(dedent(API_ROUTE_DOC), route=route))
            out.write(INDEX_LINK)
