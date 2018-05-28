"""
Marshmallow schemas that represent worksheet block.
Used for serializing resource dicts into JSON API documents, and vice-versa.
The schemas also perform some basic validation.
"""
from bottle import local
from marshmallow import (
    Schema as PlainSchema,
    ValidationError,
    validate,
    validates_schema,
)
from marshmallow_jsonapi import Schema, fields
import sys

# Enum that represents different modes for a block.
class BlockModes:
    markup_block = 'markup_block'
    record_block = 'record_block'
    table_block = 'table_block'
    contents_block = 'contents_block'
    html_block = 'html_block'
    image_block = 'image_block'
    graph_block = 'graph_block'
    wsearch_block = 'wsearch_block'
    search_block = 'search_block'

    values = (markup_block, record_block, table_block, contents_block,
              html_block, image_block, graph_block, wsearch_block,
              search_block)


STATUS_STRINGS = ('unknown', 'pending', 'ready', 'not_found', 'no_permission')


class FetchStatusSchema(PlainSchema):
    """
    Schema that represents the status of fetching a resource.
    """
    code = fields.String(validate=validate.OneOf(set(STATUS_STRINGS)))
    error_message = fields.String()


class WorksheetBlockSchema(PlainSchema):
    """
    Parent schema for all worksheet blocks.
    """
    mode = fields.String(validate=validate.OneOf(set(BlockModes.values)))
    is_refined = fields.Bool(default=False)

    class Meta:
        type_ = 'worksheet-block'


class MarkupBlockSchema(WorksheetBlockSchema):
    """
    Schema for blocks that contain markup.
    Does not need refining, contains markup text as payload.
    """
    mode = fields.Constant(BlockModes.markup_block)
    is_refined = fields.Bool(validate=validate.Equal(True))  # always refined
    text = fields.String()


class BundleBlockSchema(WorksheetBlockSchema):
    """
    Parent schema for blocks that load data from a single bundle.
    Stores state relevant to fetching information from bundle.
    """

    target_genpath = fields.String(required=True)
    bundle_info = fields.Dict(required=True)
    status = fields.Nested(FetchStatusSchema, required=True)


class BundleContentsBlockSchema(BundleBlockSchema):
    mode = fields.Constant(BlockModes.contents_block)
    max_lines = fields.Integer()
    lines = fields.List(fields.String())


class BundleImageBlockSchema(BundleBlockSchema):
    mode = fields.Constant(BlockModes.image_block)

    image_data = fields.String()
    height = fields.Integer()
    width = fields.Integer()


class BundleHTMLBlockSchema(BundleBlockSchema):
    mode = fields.Constant(BlockModes.html_block)
    max_lines = fields.Constant(sys.maxint)
    html_lines = fields.List(fields.String())


# class TableSchemaItemSchema(PlainSchema):
#     name = fields.String()
#     genpath = fields.String()
#     post_processor = fields.String()


# class TableBlockSchema(WorksheetBlockSchema):
#     mode = fields.Constant(BlockModes.table_block)
#     table_schema = fields.Nested(TableSchemaItemSchema, many=True)
#     header = fields.List(fields.String())
