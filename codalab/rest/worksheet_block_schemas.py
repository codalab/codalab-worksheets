"""
Marshmallow schemas that represent worksheet block.
Used for serializing resource dicts into JSON API documents, and vice-versa.
The schemas also perform some basic validation.
"""
from marshmallow import Schema as PlainSchema, validate
from marshmallow_jsonapi import fields


# Enum that represents different modes for a block.
class BlockModes:
    markup_block = 'markup_block'
    record_block = 'record_block'
    table_block = 'table_block'
    contents_block = 'contents_block'
    image_block = 'image_block'
    graph_block = 'graph_block'
    schema_block = 'schema_block'
    subworksheets_block = 'subworksheets_block'
    placeholder_block = 'placeholder_block'

    values = (
        markup_block,
        record_block,
        table_block,
        contents_block,
        image_block,
        graph_block,
        schema_block,
        subworksheets_block,
        placeholder_block,
    )


class FetchStatusCodes:
    """
    The values here correspond with FETCH_STATUS_SCHEMA in the frontend.
    """

    unknown = 'unknown'
    pending = 'pending'
    briefly_loaded = 'briefly_loaded'
    ready = 'ready'
    not_found = 'not_found'
    no_permission = 'no_permission'

    values = (unknown, pending, briefly_loaded, ready, not_found, no_permission)


class FetchStatusSchema(PlainSchema):
    """
    Schema that represents the status of fetching a resource.
    """

    code = fields.String(validate=validate.OneOf(set(FetchStatusCodes.values)))
    error_message = fields.String()

    @staticmethod
    def get_unknown_status():
        return {'code': FetchStatusCodes.unknown, 'error_message': ''}

    @staticmethod
    def get_pending_status():
        return {'code': FetchStatusCodes.pending, 'error_message': ''}

    @staticmethod
    def get_briefly_loaded_status():
        return {'code': FetchStatusCodes.briefly_loaded, 'error_message': ''}

    @staticmethod
    def get_ready_status():
        return {'code': FetchStatusCodes.ready, 'error_message': ''}


class BundlesSpecSchema(PlainSchema):
    uuid_spec_type = 'uuid_spec'

    spec_types = uuid_spec_type

    # Fields
    spec_type = fields.String(validate=validate.OneOf(set(spec_types)))
    bundle_infos = fields.List(fields.Dict())
    fetch_status = fields.Nested(FetchStatusSchema, required=True)


class BundleUUIDSpecSchema(BundlesSpecSchema):
    spec_type = fields.Constant(BundlesSpecSchema.uuid_spec_type)
    uuid_spec = fields.List(fields.String(), required=True)

    @staticmethod
    def create_json(bundle_infos):
        return {
            'spec_type': BundlesSpecSchema.uuid_spec_type,
            'uuid_spec': [bundle_info['uuid'] for bundle_info in bundle_infos],
            'bundle_infos': bundle_infos,
            'fetch_status': FetchStatusSchema.get_ready_status(),
        }


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

    ids = fields.List(fields.Integer())
    sort_keys = fields.List(fields.Integer())
    mode = fields.Constant(BlockModes.markup_block)
    is_refined = fields.Bool(validate=validate.Equal(True))  # always refined
    text = fields.String()
    error = fields.Bool(default=False)  # True if the markdown shows an error


class BundleBlockSchema(WorksheetBlockSchema):
    """
    Parent schema for blocks that load data from a single bundle.
    Stores state relevant to fetching information from bundle.
    """

    bundles_spec = fields.Nested(BundlesSpecSchema, required=True)
    target_genpath = fields.String(required=True)
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


class SchemaBlockSchema(WorksheetBlockSchema):
    """
    Schema for user-defined schemas in worksheets
    """

    mode = fields.Constant(BlockModes.schema_block)
    schema_name = fields.String(required=True)
    header = fields.List(fields.String(), required=True)
    field_rows = fields.List(fields.Dict(), required=True)
    sort_keys = fields.List(fields.Integer())
    ids = fields.List(fields.Integer())


class TableBlockSchema(WorksheetBlockSchema):
    mode = fields.Constant(BlockModes.table_block)
    bundles_spec = fields.Nested(BundlesSpecSchema, required=True)
    status = fields.Nested(FetchStatusSchema, required=True)

    header = fields.List(fields.String(), required=True)
    rows = fields.List(fields.Dict(), required=True)
    sort_keys = fields.List(fields.Integer())
    first_bundle_source_index = fields.Integer()  # index for the first bundle in source
    using_schemas = fields.List(fields.String())


class RecordsRowSchema(PlainSchema):
    key = fields.String(required=True)
    value = fields.Raw(required=True)


class RecordsBlockSchema(BundleBlockSchema):
    mode = fields.Constant(BlockModes.record_block)
    bundles_spec = fields.Nested(BundlesSpecSchema, required=True)
    status = fields.Nested(FetchStatusSchema, required=True)

    header = fields.Constant(('key', 'value'))
    rows = fields.Nested(RecordsRowSchema, many=True, required=True)
    sort_keys = fields.List(fields.Integer())
    first_bundle_source_index = fields.Integer()  # index for the first bundle in source
    using_schemas = fields.List(fields.String())


class GraphTrajectorySchema(PlainSchema):
    bundle_uuid = fields.String(required=True)
    display_name = fields.String(required=True)
    target_genpath = fields.String()
    points = fields.List(fields.String())


class GraphBlockSchema(BundleBlockSchema):
    mode = fields.Constant(BlockModes.graph_block)
    bundles_spec = fields.Nested(BundlesSpecSchema, required=True)
    status = fields.Nested(FetchStatusSchema, required=True)

    trajectories = fields.Nested(GraphTrajectorySchema, many=True, required=True)
    max_lines = fields.Integer()
    xlabel = fields.String()
    ylabel = fields.String()


class SubworksheetsBlock(WorksheetBlockSchema):
    mode = fields.Constant(BlockModes.subworksheets_block)
    subworksheet_infos = fields.List(fields.Dict, required=True)
    sort_keys = fields.List(fields.Integer())


class PlaceholderBlockSchema(WorksheetBlockSchema):
    mode = fields.Constant(BlockModes.placeholder_block)
    directive = fields.String()
    sort_keys = fields.List(fields.Integer())
