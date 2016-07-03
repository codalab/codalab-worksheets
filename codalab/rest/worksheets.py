import httplib
import os

from bottle import abort, get, local
from marshmallow import (
    ValidationError,
    validate,
    validates_schema,
)
from marshmallow_jsonapi import Schema, fields

from codalab.common import UsageError
from codalab.lib import worksheet_util, spec_util, canonicalize
from codalab.lib.server_util import (
    json_api_include,
    query_get_list,
)
from codalab.lib.spec_util import validate_uuid, validate_name
from codalab.lib.worksheet_util import WORKSHEET_ITEM_TYPES
from codalab.model.tables import GROUP_OBJECT_PERMISSION_READ
from codalab.objects.permission import (
    check_worksheet_has_all_permission,
    PermissionSpec,
)
from codalab.objects.worksheet import Worksheet
from codalab.rest.users import UserSchema
from codalab.rest.util import (
    local_bundle_client_compatible,
)
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


@local_bundle_client_compatible
def set_worksheet_permission(local, request, worksheet, group_uuid, permission):
    """
    Give the given |group_uuid| the desired |permission| on |worksheet_uuid|.
    """
    check_worksheet_has_all_permission(local.model, request.user, worksheet)
    local.model.set_group_worksheet_permission(group_uuid, worksheet.uuid, permission)


# FIXME(sckoo): fix when implementing worksheets API
@local_bundle_client_compatible
def populate_dashboard(local, request, worksheet):
    raise NotImplementedError("Automatic dashboard creation temporarily unsupported.")
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../objects/dashboard.ws')
    lines = [line.rstrip() for line in open(file_path, 'r').readlines()]
    items, commands = worksheet_util.parse_worksheet_form(lines, self, worksheet.uuid)
    info = self.get_worksheet_info(worksheet.uuid, True)
    self.update_worksheet_items(info, items)
    self.update_worksheet_metadata(worksheet.uuid, {'title': 'Codalab Dashboard'})


@local_bundle_client_compatible
def get_worksheet_uuid_or_none(local, request, base_worksheet_uuid, worksheet_spec):
    """
    Helper: Return the uuid of the specified worksheet if it exists. Otherwise, return None.
    """
    try:
        return canonicalize.get_worksheet_uuid(local.model, base_worksheet_uuid, worksheet_spec)
    except UsageError:
        return None


@local_bundle_client_compatible
def ensure_unused_worksheet_name(local, request, name):
    """
    Ensure worksheet names are unique.
    Note: for simplicity, we are ensuring uniqueness across the system, even on
    worksheet names that the user may not have access to.
    """
    # If trying to set the name to a home worksheet, then it better be
    # user's home worksheet.
    if spec_util.is_home_worksheet(name) and spec_util.home_worksheet(request.user.user_name) != name:
        raise UsageError('Cannot create %s because this is potentially the home worksheet of another user' % name)
    if get_worksheet_uuid_or_none(None, name) is not None:
        raise UsageError('Worksheet with name %s already exists' % name)


@local_bundle_client_compatible
def new_worksheet(local, request, name):
    """
    Create a new worksheet with the given |name|.
    """
    ensure_unused_worksheet_name(name)

    # Don't need any permissions to do this.
    worksheet = Worksheet({
        'name': name,
        'title': None,
        'frozen': None,
        'items': [],
        'owner_id': request.user.user_id
    })
    local.model.new_worksheet(worksheet)

    # Make worksheet publicly readable by default
    set_worksheet_permission(worksheet, local.model.public_group_uuid,
                             GROUP_OBJECT_PERMISSION_READ)
    if spec_util.is_dashboard(name):
        populate_dashboard(worksheet)
    return worksheet.uuid


@local_bundle_client_compatible
def get_worksheet_uuid(local, request, base_worksheet_uuid, worksheet_spec):
    """
    Return the uuid of the specified worksheet if it exists.
    If not, create a new worksheet if the specified worksheet is home_worksheet
    or dashboard. Otherwise, throw an error.
    """
    if worksheet_spec == '' or worksheet_spec == worksheet_util.HOME_WORKSHEET:
        worksheet_spec = spec_util.home_worksheet(request.user.user_name)
    worksheet_uuid = get_worksheet_uuid_or_none(base_worksheet_uuid, worksheet_spec)
    if worksheet_uuid is not None:
        return worksheet_uuid
    else:
        if spec_util.is_home_worksheet(worksheet_spec) or spec_util.is_dashboard(worksheet_spec):
            return new_worksheet(worksheet_spec)
        else:
            # let it throw the correct error message
            return canonicalize.get_worksheet_uuid(local.model, base_worksheet_uuid, worksheet_spec)
