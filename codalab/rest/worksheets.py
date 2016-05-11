import httplib
import os

from bottle import abort, get, post, delete, local, request, response
from marshmallow import (
    ValidationError,
    Schema as PlainSchema,
    validate,
    validates_schema,
    validates,
)
from marshmallow_jsonapi import Schema, fields

from codalab.common import UsageError, PermissionError
from codalab.lib import worksheet_util, spec_util, canonicalize, formatting
from codalab.lib.server_util import (
    bottle_patch as patch,
    json_api_include,
    json_api_meta,
    query_get_bool,
    query_get_list,
    query_get_type,
)
from codalab.lib.spec_util import validate_uuid, validate_name
from codalab.lib.worksheet_util import WORKSHEET_ITEM_TYPES
from codalab.objects.permission import (
    check_worksheet_has_all_permission,
    check_worksheet_has_read_permission,
    parse_permission,
    PermissionSpec,
)
from codalab.objects.worksheet import Worksheet
from codalab.rest.util import (
    get_bundle_infos,
    get_group_info,
    get_worksheet_info,
)
from codalab.rest.users import UserSchema
from codalab.server.authenticated_plugin import AuthenticatedPlugin
from codalab.server.json_api_plugin import JsonApiPlugin


#############################################################
#  WORKSHEET DE/SERIALIZATION AND VALIDATION SCHEMAS
#############################################################


class WorksheetPermissionSchema(Schema):
    id = fields.Integer(as_string=True, dump_only=True)
    worksheet = fields.Relationship(required=True, load_only=True, include_data=True, type_='worksheets', attribute='object_uuid')
    group = fields.Relationship(required=True, include_data=True, type_='groups', attribute='group_uuid')
    group_name = fields.String(dump_only=True)  # for convenience
    permission = fields.Integer(validate=lambda p: 0 <= p <= 2)
    permission_spec = PermissionSpec(attribute='permission')  # for convenience

    @validates_schema
    def check_permission_exists(self, data):
        if 'permission' not in data:
            raise ValidationError("One of either permission or permission_spec must be provided.")

    class Meta:
        type_ = 'worksheet-permissions'


class WorksheetItemSchema(Schema):
    bundle = fields.Relationship(type_='bundles', include_data=True)
    subworksheet = fields.Relationship(type_='worksheets', include_data=True)
    value = fields.String()
    type = fields.String(validate=validate.OneOf(WORKSHEET_ITEM_TYPES))
    sort_key = fields.Integer()

    class Meta:
        type_ = 'worksheet-items'


class WorksheetSchema(Schema):
    id = fields.String(dump_only=True, validate=validate_uuid, attribute='uuid')
    uuid = fields.String(dump_only=True, attribute='uuid')  # for backwards compatibility
    name = fields.String(validate=validate_name)
    title = fields.String()
    frozen = fields.Boolean()
    tags = fields.List(fields.String)
    owner = fields.Relationship(type_='users', include_data=True, attribute='owner_id')
    items = fields.Relationship(type_='worksheet-items', many=True, include_data=True)
    group_permissions = fields.Relationship(type_='worksheet-permissions', many=True, include_data=True)

    class Meta:
        type_ = 'worksheets'


#############################################################
#  WORKSHEET REST API ENDPOINTS
#############################################################

@get('/worksheets/<uuid:re:%s>' % spec_util.UUID_STR, apply=[AuthenticatedPlugin(), JsonApiPlugin()])
def fetch_worksheet(uuid):
    document = fetch_worksheets_helper([uuid])
    document['data'] = document['data'][0]
    return document


def fetch_worksheets_helper(uuids):
    include = set(query_get_list('include'))

    worksheets_dict = {
        uuid: get_worksheet_info(
            uuid,
            fetch_items=('worksheet-items' in include),
            fetch_permission=('worksheet-permissions' in include))
        for uuid in uuids
    }

    # Build list of worksheets in order requested
    try:
        worksheets = [worksheets_dict[uuid] for uuid in uuids]
    except KeyError as e:
        abort(httplib.NOT_FOUND, "Worksheet %s not found" % e.args[0])

    # Build response document
    document = WorksheetSchema(many=True).dump(worksheets).data

    if 'worksheet-items' in include:
        pass

    if 'users' in include:
        owner_ids = set(w['owner_id'] for w in worksheets)
        json_api_include(document, UserSchema(), local.model.get_users(owner_ids))

    if 'worksheet-permissions' in include:
        for worksheet in worksheets:
            json_api_include(document, WorksheetPermissionSchema(), worksheet['group_permissions'])

    if 'bundles' in include:
        pass

    if 'worksheets' in include:
        pass

    return document