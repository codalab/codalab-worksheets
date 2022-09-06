"""
Marshmallow schemas for serializing resource dicts into JSON API documents, and vice-versa.
The schemas also perform some basic validation.
Placed here in this central location to avoid circular imports.
"""
from bottle import local
from marshmallow import Schema as PlainSchema, ValidationError, validate, validates_schema
from marshmallow_jsonapi import Schema, fields
from marshmallow.fields import Field

from codalab.common import UsageError, NotFoundError
from codalab.bundles import BUNDLE_SUBCLASSES
from codalab.lib.bundle_action import BundleAction
from codalab.lib.spec_util import SUB_PATH_REGEX, NAME_REGEX, UUID_REGEX
from codalab.lib.worksheet_util import WORKSHEET_ITEM_TYPES
from codalab.lib.unicode_util import contains_unicode
from codalab.objects.permission import parse_permission, permission_str


class CompatibleInteger(fields.Integer):
    def serialize(self, attr, obj, accessor=None):
        """Overrides change done from 2.10.2->2.10.3 in https://github.com/marshmallow-code/marshmallow/commit/d81cab413e231ec40123020f110a8c0af22163ed.
        """
        ret = Field.serialize(self, attr, obj, accessor=accessor)
        return self._to_string(ret) if (self.as_string and ret is not None) else ret


class PermissionSpec(fields.Field):
    def _serialize(self, value, attr, obj):
        try:
            return permission_str(value)
        except UsageError as e:
            raise ValidationError(str(e))

    def _deserialize(self, value, attr, data):
        try:
            return parse_permission(value)
        except UsageError as e:
            raise ValidationError(str(e))


def validate_uuid(uuid_str):
    """
    Raise a ValidationError if the uuid does not conform to its regex.
    """
    if not UUID_REGEX.match:
        raise ValidationError('uuids must match %s, was %s' % (UUID_REGEX.pattern, uuid_str))


def validate_name(name):
    if not NAME_REGEX.match(name):
        raise ValidationError('Names must match %s, was %s' % (NAME_REGEX.pattern, name))


def validate_sub_path(path):
    if not SUB_PATH_REGEX.match(path):
        raise ValidationError('Child path must match %s, was %s' % (SUB_PATH_REGEX.pattern, path))


def validate_ascii(value):
    if isinstance(value, str):
        if contains_unicode(value):
            raise ValidationError('Unsupported character detected, use ascii characters')
    elif isinstance(value, list):
        for v in value:
            validate_ascii(v)
    elif isinstance(value, dict):
        for v in value.itervalues():
            validate_ascii(v)


class WorksheetItemSchema(Schema):
    id = CompatibleInteger(as_string=True, dump_only=True)
    worksheet = fields.Relationship(
        include_resource_linkage=True, attribute='worksheet_uuid', type_='worksheets', required=True
    )
    subworksheet = fields.Relationship(
        include_resource_linkage=True,
        type_='worksheets',
        attribute='subworksheet_uuid',
        allow_none=True,
    )
    bundle = fields.Relationship(
        include_resource_linkage=True, type_='bundles', attribute='bundle_uuid', allow_none=True
    )
    value = fields.String()
    type = fields.String(validate=validate.OneOf(set(WORKSHEET_ITEM_TYPES)), required=True)
    sort_key = fields.Integer(allow_none=True)

    class Meta:
        type_ = "worksheet-items"


class WorksheetPermissionSchema(Schema):
    id = CompatibleInteger(as_string=True, dump_only=True)
    worksheet = fields.Relationship(
        include_resource_linkage=True,
        attribute='object_uuid',
        type_='worksheets',
        load_only=True,
        required=True,
    )
    group = fields.Relationship(
        include_resource_linkage=True, attribute='group_uuid', type_='groups', required=True
    )
    group_name = fields.String(dump_only=True)  # for convenience
    permission = fields.Integer(validate=lambda p: 0 <= p <= 2)
    permission_spec = PermissionSpec(attribute='permission')  # for convenience

    @validates_schema
    def check_permission_exists(self, data):
        if 'permission' not in data:
            raise ValidationError("One of either permission or permission_spec must be provided.")

    class Meta:
        type_ = 'worksheet-permissions'


class WorksheetSchema(Schema):
    id = fields.String(validate=validate_uuid, attribute='uuid')
    uuid = fields.String(attribute='uuid')  # for backwards compatibility
    name = fields.String(validate=validate_name)
    owner = fields.Relationship(include_resource_linkage=True, type_='users', attribute='owner_id')
    title = fields.String()
    frozen = fields.DateTime(allow_none=True)
    is_anonymous = fields.Bool()
    date_created = fields.DateTime(allow_none=True)
    date_last_modified = fields.DateTime(allow_none=True)
    tags = fields.List(fields.String(validate=validate_ascii))
    group_permissions = fields.Relationship(
        include_resource_linkage=True, type_='worksheet-permissions', id_field='id', many=True
    )
    items = fields.Relationship(
        include_resource_linkage=True, type_='worksheet-items', id_field='id', many=True
    )
    last_item_id = fields.Integer(dump_only=True)

    # Bundle permission of the authenticated user for convenience, read-only
    permission = fields.Integer()
    permission_spec = PermissionSpec(attribute='permission')

    class Meta:
        type_ = 'worksheets'


class BundleDependencySchema(PlainSchema):
    """
    Plain (non-JSONAPI) Marshmallow schema for a single bundle dependency.
    Not defining this as a separate resource with Relationships because we only
    create a set of dependencies once at bundle creation.
    """

    child_uuid = fields.String(validate=validate_uuid, dump_only=True)
    child_path = fields.String()  # Validated in Bundle ORMObject
    parent_uuid = fields.String(validate=validate_uuid)
    parent_path = fields.String(missing="")
    parent_name = fields.Method('get_parent_name', dump_only=True)  # for convenience
    parent_state = fields.Method('get_parent_state', dump_only=True)  # for convenience

    def get_parent_name(self, dep):
        uuid = dep['parent_uuid']
        return local.model.get_bundle_names([uuid]).get(uuid)

    def get_parent_state(self, dep):
        uuid = dep['parent_uuid']
        try:
            return local.model.get_bundle_state(uuid)
        except NotFoundError:
            return None


class BundlePermissionSchema(Schema):
    id = CompatibleInteger(as_string=True, dump_only=True)
    bundle = fields.Relationship(
        include_resource_linkage=True,
        attribute='object_uuid',
        type_='bundles',
        load_only=True,
        required=True,
    )
    group = fields.Relationship(
        include_resource_linkage=True, attribute='group_uuid', type_='groups', required=True
    )
    group_name = fields.String(dump_only=True)  # for convenience
    permission = fields.Integer(validate=lambda p: 0 <= p <= 2)
    permission_spec = PermissionSpec(attribute='permission')  # for convenience

    @validates_schema
    def check_permission_exists(self, data):
        if 'permission' not in data:
            raise ValidationError("One of either permission or permission_spec must be provided.")

    class Meta:
        type_ = 'bundle-permissions'


class BundleSchema(Schema):
    id = fields.String(validate=validate_uuid, attribute='uuid')
    uuid = fields.String(attribute='uuid')  # for backwards compatibility
    bundle_type = fields.String(
        validate=validate.OneOf({bsc.BUNDLE_TYPE for bsc in BUNDLE_SUBCLASSES})
    )
    command = fields.String(allow_none=True, validate=validate_ascii)
    data_hash = fields.String()
    state = fields.String()
    state_details = fields.String()
    owner = fields.Relationship(include_resource_linkage=True, type_='users', attribute='owner_id')
    frozen = fields.DateTime(allow_none=True)
    is_anonymous = fields.Bool()
    storage_type = fields.String()
    is_dir = fields.Bool()
    metadata = fields.Dict(values=fields.String(validate=validate_ascii))
    dependencies = fields.Nested(BundleDependencySchema, many=True)
    children = fields.Relationship(
        include_resource_linkage=True, type_='bundles', id_field='uuid', many=True
    )
    group_permissions = fields.Relationship(
        include_resource_linkage=True, type_='bundle-permissions', id_field='id', many=True
    )
    host_worksheets = fields.Relationship(
        include_resource_linkage=True, type_='worksheets', id_field='uuid', many=True
    )
    args = fields.String()

    # Bundle permission of the authenticated user for convenience, read-only
    permission = fields.Integer()
    permission_spec = PermissionSpec(attribute='permission')

    class Meta:
        type_ = 'bundles'


class BundleStoreSchema(Schema):
    id = fields.String(validate=validate_uuid, attribute='uuid')
    uuid = fields.String(attribute='uuid')
    owner = fields.Integer(attribute='owner_id')
    name = fields.String(required=True)
    storage_type = fields.String(required=True)
    storage_format = fields.String(allow_none=True)
    url = fields.String(allow_none=True)
    authentication = fields.String(allow_none=True)
    authentication_env = fields.String(allow_none=True)

    class Meta:
        type_ = 'bundle_stores'


class BundleLocationSchema(Schema):
    id = fields.String(validate=validate_uuid, attribute='uuid')
    bundle_uuid = fields.String(validate=validate_uuid, attribute='bundle_uuid')
    bundle_store_uuid = fields.String(validate=validate_uuid, attribute='bundle_store_uuid')

    class Meta:
        type_ = 'bundle_locations'


class BundleLocationListSchema(Schema):
    id = fields.String(validate=validate_uuid, attribute='bundle_store_uuid')
    bundle_store_uuid = fields.String(attribute='bundle_store_uuid')
    name = fields.String()
    storage_type = fields.String()
    storage_format = fields.String()
    url = fields.String(allow_none=True)

    class Meta:
        type_ = 'bundle_locations'


# Field-update restrictions are specified as lists below because the
# restrictions differ depending on the action

BUNDLE_CREATE_RESTRICTED_FIELDS = (
    'data_hash',
    'state',
    'owner',
    'children',
    'group_permissions',
    'host_worksheets',
    'args',
    'permission',
    'permission_spec',
)


BUNDLE_UPDATE_RESTRICTED_FIELDS = (
    'command',
    'data_hash',
    'state',
    'dependencies',
    'children',
    'group_permissions',
    'host_worksheets',
    'args',
    'permission',
    'permission_spec',
    'bundle_type',
)


class BundleActionSchema(Schema):
    id = fields.Integer(dump_only=True, default=None)
    uuid = fields.String(validate=validate_uuid)
    type = fields.String(validate=validate.OneOf({BundleAction.KILL, BundleAction.WRITE}))
    subpath = fields.String(validate=validate_sub_path)
    string = fields.String()

    class Meta:
        type_ = 'bundle-actions'


class UserSchema(Schema):
    id = fields.String(attribute='user_id')
    user_name = fields.String()
    first_name = fields.String(allow_none=True)
    last_name = fields.String(allow_none=True)
    affiliation = fields.String(allow_none=True)
    url = fields.Url(allow_none=True)
    date_joined = fields.LocalDateTime("%c")
    avatar_id = fields.String(allow_none=True)
    has_access = fields.Bool()

    class Meta:
        type_ = 'users'


class AuthenticatedUserSchema(UserSchema):
    email = fields.String()
    notifications = fields.Integer()
    time_quota = fields.Integer()
    parallel_run_quota = fields.Integer()
    time_used = fields.Integer()
    disk_quota = fields.Integer()
    disk_used = fields.Integer()
    last_login = fields.LocalDateTime("%c")


class AdminUserSchema(AuthenticatedUserSchema):
    is_verified = fields.Bool()
    has_access = fields.Bool()


# Email must be updated through the /account/changeemail interface.
# We cannot use the `dump_only` arguments to specify these filters, since
# some users (i.e. the admin) CAN use the API to update some of these fields.
USER_READ_ONLY_FIELDS = (
    'email',
    'has_access',
    'time_quota',
    'parallel_run_quota',
    'time_used',
    'disk_quota',
    'disk_used',
    'date_joined',
    'last_login',
)


class GroupSchema(Schema):
    id = fields.String(validate=validate_uuid, attribute='uuid')
    name = fields.String(required=True, validate=validate_name)
    user_defined = fields.Bool(dump_only=True)
    owner = fields.Relationship(include_resource_linkage=True, type_='users', attribute='owner_id')
    admins = fields.Relationship(include_resource_linkage=True, type_='users', many=True)
    members = fields.Relationship(include_resource_linkage=True, type_='users', many=True)

    class Meta:
        type_ = 'groups'
