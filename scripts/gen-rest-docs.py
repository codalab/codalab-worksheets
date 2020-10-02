# -*- coding: utf-8 -*-
"""
Generate REST docs.
"""
import sys

import argparse
from inspect import isclass
from collections import defaultdict, namedtuple
import os

from bottle import default_app, template
from marshmallow import Schema
from marshmallow_jsonapi import Schema as JsonApiSchema
from textwrap import dedent

from codalab.common import CODALAB_VERSION

# Ensure all REST modules are loaded
from codalab.server import rest_server  # NOQA - Ignoring flake8 errors since we want to keep it.

sys.path.append('.')


EXCLUDED_APIS = {'account', 'api', 'static', 'chats', 'faq', 'help'}


APISpec = namedtuple('APISpec', 'name anchor routes')


def get_api_routes():
    # TODO: import modules individually and sort these routes by their
    # module name. This will keep Bundles and Bundle Permissions API together.
    # Then we can also load the docstrings of the modules themselves and print
    # those on each page.
    app = default_app()
    base2routes = defaultdict(list)
    bases = set()
    for route in app.routes:
        path = route.rule.split('/')
        base = path[1]
        if base in EXCLUDED_APIS:
            continue
        base2routes[base].append(route)
        bases.add(base)

    api_specs = []
    for base in bases:
        default_name = ' '.join(base.title().split('-'))
        name = {'oauth2': 'OAuth2', 'cli': 'CLI', 'interpret': 'Worksheet Interpretation'}.get(
            base, default_name
        )
        anchor = '-'.join(name.lower().split())
        api_specs.append(APISpec(name, anchor, base2routes[base]))

    return sorted(api_specs)


def get_codalab_schemas():
    from codalab.rest import schemas as schemas_module

    for k, v in sorted(vars(schemas_module).items()):
        if not isclass(v):
            continue
        if not issubclass(v, Schema):
            continue
        if v is Schema:
            continue
        if v is JsonApiSchema:
            continue
        yield k, v


INDEX_DOC = '''\
# REST API Reference

_version {{version}}_

This reference and the REST API itself is still under heavy development and is
subject to change at any time. Feedback through our GitHub issues is appreciated!

## Table of Contents
- [Introduction](#introduction)
- [Resource Object Schemas](#resource-object-schemas)
- [API Endpoints](#api-endpoints)
% for spec in api_specs:
  - [{{spec.name}} API](#{{spec.anchor}}-api)
% end

'''

INTRODUCTION_DOC = '''\
# Introduction
We use the JSON API v1.0 specification with the Bulk extension.
- http://jsonapi.org/format/
- https://github.com/json-api/json-api/blob/9c7a03dbc37f80f6ca81b16d444c960e96dd7a57/extensions/bulk/index.md

The following specification will not provide the verbose JSON formats of the API requests and responses, as those can be found in the JSON API specification. Instead:
- Each resource type below (worksheets, bundles, etc.) specifies a list of
  attributes with their respective data types and semantics, along with a list
  of relationships.
- Each API call specifies the HTTP request method, the endpoint URI, HTTP
  parameters with their respective data types and semantics, description of the
  API call.

## How we use JSON API

Using the JSON API specification allows us to avoid redesigning the wheel when designing our request and response formats. This also means that we will not specify all the details of the API (for example, Content-Type headers, the fact that POST requests should contain a single resource object, etc.) in this document at this time, while we may choose to continue copying in more details as we go in the design and implementation process. However, since there are many optional features of the JSON API specification, we will document on a best-effort basis the ways in which we will use the specification that are specific to our API, as well as which parts of the specification we use, and which parts we do not.

## Top-level JSON structure

Every JSON request or response will have at its root a JSON object containing either a “data” field or an “error” field, but not both. Thus the presence of an “error” field will unambiguously indicate an error state.

Response documents may also contain a top-level "meta" field, containing additional constructed data that are not strictly resource objects, such as summations in a search query.

Response documents may also contain a top-level "included" field, discussed below.

## Primary Data

The JSON API standard specifies that the “data” field will contain either a [resource object](http://jsonapi.org/format/#document-resource-objects) or an array of resource objects, depending on the nature of the request. More specfically, if the client is fetching a single specific resource (e.g. GET /bundles/0x1d09b495), the “data” field will have a single JSON object at its root. If the client intends to fetch a variable number of resources, then the “data” field will have at its root an array of zero or more JSON objects.

The structure of a JSON response with a single resource object will typically look like this:

```
{
  "data": {
    "type": "bundles",
    "id": "0x1d09b495410249f89dee4465cd21d499",
    "attributes": {
      // ... this bundle's attributes
    },
    "relationships": {
      // ... this bundle's relationships
    }
  }
}
```

Note that we use UUIDs as the "id" of a resource when available (i.e. for worksheets, bundles, and groups) and some other unique key for those resources to which we have not prescribed a UUID scheme.

For each of the resource types available in the Worksheets API, we define the schema for its **attributes**, as well as list what **relationships** each instance may have defined. Relationships are analogous to relationships in relational databases and ORMs—some may be to-one (such as the "owner" of a bundle) and some may be to-many (such as the "permissions" of a bundle).

We will use the following subset of the relationship object schema for our Worksheets API (Orderly schema):

```
object {
  object {
    string related;   // URL to GET the related resource
  } links;
  object {            // used to identify resource in includes
    string type;      // type of the related resource
    string id;        // id of the related resource
  } data?;
}
```

## Query Parameters

The client may provide additional parameters for requests as query parameters in the request URL. Available parameters will be listed under each API route. In general:
- Boolean query parameters are encoded as 1 for "true" and 0 for "false".
- Some query parameters take multiple values, which can be passed by simply listing
  the parameter multiple times in the query, e.g. `GET /bundles?keywords=hello&keywords=world`

## Includes

The client will often want to fetch data for resources related to the primary resource(s) in the same request: for example, client may want to fetch a worksheet along with all of its items, as well as data about the bundles and worksheets referenced in the items.

Currently, most of the API endpoints will include related resources automatically.
For example, fetching a "worksheet" will also include the "bundles" referenced
by the worksheet in the response. These related resource objects will be
included in an array in the top-level "included" field of the response object.

## Non-JSON API Endpoints

The JSON API specification is not all-encompassing, and there are some cases in our API that fall outside of the specification. We will indicate this explicitly where it applies, and provide an alternative schema for the JSON format where necessary.

## Authorization and Authentication

The Bundle Service also serves as an OAuth2 Provider.

All requests to protected resources on the Worksheets API must include a valid
OAuth bearer token in the HTTP headers:

    Authorization: Bearer xxxxtokenxxxx

If the token is expired, does not authorize the application to access the
target resource, or is otherwise invalid, the Bundle Service will respond with
a `401 Unauthorized` or `403 Forbidden` status.

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
`{{field_name}}` | [{{field.nested.__name__}}](#{{field.nested.__name__}})
% elif field_class.__name__ == 'Relationship':
`{{field_name}}` | Relationship([{{field.type_}}](#{{field.type_}}))
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
{{!dedent(docstring)}}
% end
'''


INDEX_LINK = "\n&uarr; [Back to Top](#table-of-contents)\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--docs', default='docs')
    args = parser.parse_args()

    with open(os.path.join(args.docs, 'REST-API-Reference.md'), 'w') as out:
        out.write(template(dedent(INDEX_DOC), api_specs=get_api_routes(), version=CODALAB_VERSION))
        out.write(dedent(INTRODUCTION_DOC))

        out.write('# Resource Object Schemas\n')
        for schema_name, schema in get_codalab_schemas():
            out.write(template(dedent(SCHEMA_DOC), schema=schema))
            # For debugging.
            # for field_name, field in schema._declared_fields.items():
            #     print vars(field)
        out.write(INDEX_LINK)

        out.write('# API Endpoints\n')
        for spec in get_api_routes():
            out.write('## %s API\n' % spec.name)
            for route in spec.routes:
                out.write(template(dedent(API_ROUTE_DOC), route=route))
            out.write(INDEX_LINK)


if __name__ == '__main__':
    main()
