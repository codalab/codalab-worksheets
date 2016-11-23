"""
Generate REST docs.
"""
import sys
sys.path.append('.')
sys.path.append('venv/lib/python2.7/site-packages')
from inspect import isclass

from bottle import default_app
from marshmallow import Schema
from marshmallow_jsonapi import Schema as JsonApiSchema

from codalab.server import rest_server
from codalab.rest import schemas as schemas_module


# TODO
#  - create python string templates in Markdown for everything
#  - filter out unwanted things (rpc endpoint, account endpoints, Schema)
#  - output .md files into static/ ?


def tabulate(headers, rows):
    pass


if __name__ == '__main__':
    app = default_app()

    for route in app.routes:
        print '============================='
        print route.method + ' ' + route.rule
        print route.get_undecorated_callback().__doc__
        # TODO: parse out things and convert them to markdown tables
        print '============================='

    schemas = {k: v for k, v in vars(schemas_module).iteritems() if isclass(v) and issubclass(v, Schema)}
    for schema_name, schema in schemas.iteritems():
        if issubclass(schema, JsonApiSchema):
            print '============================='
            print schema_name
            print schema.Meta.type_
            for field_name, field in schema._declared_fields.iteritems():
                field_class = field.__class__
                # TODO: Map from field class to format string
                #  Integer --> integer
                #  PermissionSpec --> [0-2]|all|read|none
                # print '\t\t%s' % field_class.__doc__
                print '\t%s (%s)' % (field_name, field_class.__name__)
                for k, v in vars(field).iteritems():
                    print '\t\t%s: %s' % (k, v)
            print '============================='
