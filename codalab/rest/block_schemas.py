"""
Marshmallow schemas for serializing resource dicts into JSON API documents, and vice-versa.
The schemas also perform some basic validation.
Placed here in this central location to avoid circular imports.
"""
from bottle import local
from marshmallow import (
    Schema as PlainSchema,
    ValidationError,
    validate,
    validates_schema,
)
from marshmallow_jsonapi import Schema, fields


# Enum that represents different modes for a block.
class BlockModes:
    MARKUP_MODE = "markup_block"
    RECORD_MODE = "record_block"
    TABLE_MODE = "table_block"
    BUNDLE_CONTENTS_MODE = "contents_block"
    BUNDLE_IMAGE_MODE = "image_block"
    GRAPH_MODE = "graph_block"
    WSEARCH_MODE = "wsearch_block"
    SEARCH_MODE = "search_block"
    values = (MARKUP_MODE, RECORD_MODE, TABLE_MODE, BUNDLE_CONTENTS_MODE,
              BUNDLE_IMAGE_MODE, GRAPH_MODE, WSEARCH_MODE, SEARCH_MODE)


STATUS_STRINGS = ("unknown", "ready", "not_found", "no_permission")


class FetchStatusSchema(fields.Field):
    code = fields.String(validate=validate.OneOf(set(STATUS_STRINGS)))
    error_message = fields.String()


class WorksheetBlockSchema(PlainSchema):
    id = fields.Integer()
    mode = fields.String(validate=validate.OneOf(set(BlockModes.values)))
    is_refined = fields.Bool()

    class Meta:
        type_ = 'worksheet-block'


class MarkupBlockSchema(WorksheetBlockSchema):
    mode = fields.Constant(BlockModes.MARKUP_MODE)
    is_refined = fields.Bool(validate=validate.Equal(True))  # always refined
    text = fields.String()


class BundleContentsBlockSchema(WorksheetBlockSchema):
    mode = fields.Constant(BlockModes.BUNDLE_CONTENTS_MODE)
    path = fields.String()
    bundle = fields.Relationship(include_data=True, type_='bundles', attribute='bundle_uuid', allow_none=True)
    max_lines = fields.Integer()

    status = fields.Nested(FetchStatusSchema)
    files = fields.List(fields.String())
    lines = fields.List(fields.String())


class BundleImageBlockSchema(WorksheetBlockSchema):
    mode = fields.Constant(BlockModes.BUNDLE_IMAGE_MODE)
    path = fields.String()
    bundle = fields.Relationship(include_data=True, type_='bundles', attribute='bundle_uuid', allow_none=True)
    max_lines = fields.Integer()

    status = fields.Nested(FetchStatusSchema)
    image_data = fields.String()


class TableSchemaItemSchema(Schema):
    name = fields.String()
    genpath = fields.String()
    post_processor = fields.String()


class TableBlockSchema(WorksheetBlockSchema):
    mode = fields.Constant(BlockModes.TABLE_MODE)
    table_schema = fields.Nested(TableSchemaItemSchema, many=True)
    header = fields.List(fields.String())
