import httplib
import os

import datetime
from bottle import abort, get, post, put, delete, local, request, response

from codalab.common import PermissionError, UsageError
from codalab.lib import (
    canonicalize,
    spec_util,
    worksheet_util,
)
from codalab.lib import formatting
from codalab.lib.server_util import json_api_include, query_get_list
from codalab.lib.worksheet_util import ServerWorksheetResolver
from codalab.model.tables import GROUP_OBJECT_PERMISSION_READ
from codalab.objects.permission import (
    check_worksheet_has_all_permission,
    check_worksheet_has_read_permission,
)
from codalab.objects.user import PUBLIC_USER
from codalab.objects.worksheet import Worksheet
from codalab.rest.schemas import WorksheetSchema, WorksheetPermissionSchema, \
    BundleSchema, WorksheetItemSchema
from codalab.rest.users import UserSchema
from codalab.rest.util import (
    local_bundle_client_compatible,
    get_bundle_infos
)


@get('/worksheets/<uuid:re:%s>' % spec_util.UUID_STR)
def fetch_worksheet(uuid):
    worksheet = get_worksheet_info(
        uuid,
        fetch_items=True,
        fetch_permission=True,
        use_rest=True,
    )

    # Build response document
    document = WorksheetSchema().dump(worksheet).data

    # Include items
    json_api_include(document, WorksheetItemSchema(), worksheet['items'])

    # Include bundles
    bundle_uuids = {item['bundle_uuid'] for item in worksheet['items']
                    if item['type'] == worksheet_util.TYPE_BUNDLE and item['bundle_uuid'] is not None}
    bundle_infos = get_bundle_infos(bundle_uuids).values()
    json_api_include(document, BundleSchema(), bundle_infos)

    # Include users
    user_ids = {b['owner_id'] for b in bundle_infos}
    user_ids.add(worksheet['owner_id'])
    if user_ids:
        json_api_include(document, UserSchema(), local.model.get_users(user_ids))

    # Include subworksheets
    subworksheet_uuids = {item['subworksheet_uuid']
                          for item in worksheet['items']
                          if item['type'] == worksheet_util.TYPE_WORKSHEET and item['subworksheet_uuid'] is not None}
    json_api_include(document, WorksheetSchema(), local.model.batch_get_worksheets(fetch_items=False, uuid=subworksheet_uuids))

    # FIXME: tokenizing directive args
    # value_obj = formatting.string_to_tokens(value) if type == worksheet_util.TYPE_DIRECTIVE else value

    # Include permissions
    json_api_include(document, WorksheetPermissionSchema(), worksheet['group_permissions'])

    return document


@get('/worksheets')
def fetch_worksheets():
    """
    Fetch bundles by bundle specs OR search keywords.
    """
    keywords = query_get_list('keywords')
    specs = query_get_list('specs')
    base_worksheet_uuid = request.query.get('base')

    uuids = [get_worksheet_uuid(base_worksheet_uuid, spec) for spec in specs]
    worksheets = [w.to_dict(use_rest=True) for w in local.model.batch_get_worksheets(fetch_items=False, uuid=uuids)]

    # Build response document
    document = WorksheetSchema(many=True).dump(worksheets).data

    return document


#############################################################
#  WORKSHEET HELPER FUNCTIONS
#############################################################


@local_bundle_client_compatible
def get_worksheet_info(local, request, uuid, fetch_items=False, fetch_permission=True, use_rest=False):
    """
    The returned info object contains items which are (bundle_info, subworksheet_info, value_obj, type).
    """
    worksheet = local.model.get_worksheet(uuid, fetch_items=fetch_items)
    check_worksheet_has_read_permission(local.model, request.user, worksheet)

    # Create the info by starting out with the metadata.
    result = worksheet.to_dict(use_rest=use_rest)

    # TODO(sckoo): Legacy requirement, remove when BundleClient is deprecated
    if not use_rest:
        owner = local.model.get_user(user_id=result['owner_id'])
        result['owner_name'] = owner.user_name

    # TODO(sckoo): Legacy requirement, remove when BundleClient is deprecated
    if fetch_items and not use_rest:
        result['items'] = convert_items_from_db(result['items'])

    # Note that these group_permissions is universal and permissions are relative to the current user.
    # Need to make another database query.
    if fetch_permission:
        result['group_permissions'] = local.model.get_group_worksheet_permissions(
            request.user.user_id, worksheet.uuid)
        result['permission'] = local.model.get_user_worksheet_permissions(
            request.user.user_id, [worksheet.uuid], {worksheet.uuid: worksheet.owner_id}
        )[worksheet.uuid]

    return result


# TODO(sckoo): Legacy requirement, remove when BundleClient is deprecated
@local_bundle_client_compatible
def convert_items_from_db(local, request, items):
    """
    Helper function.
    (bundle_uuid, subworksheet_uuid, value, type) -> (bundle_info, subworksheet_info, value_obj, type)
    """
    # Database only contains the uuid; need to expand to info.
    # We need to do to convert the bundle_uuids into bundle_info dicts.
    # However, we still make O(1) database calls because we use the
    # optimized batch_get_bundles multiget method.
    bundle_uuids = set(
        bundle_uuid for (bundle_uuid, subworksheet_uuid, value, type) in items
        if bundle_uuid is not None
    )

    bundle_dict = get_bundle_infos(bundle_uuids)

    # Go through the items and substitute the components
    new_items = []
    for (bundle_uuid, subworksheet_uuid, value, type) in items:
        bundle_info = bundle_dict.get(bundle_uuid, {'uuid': bundle_uuid}) if bundle_uuid else None
        if subworksheet_uuid:
            try:
                subworksheet_info = local.model.get_worksheet(subworksheet_uuid, fetch_items=False).to_dict()
            except UsageError, e:
                # If can't get the subworksheet, it's probably invalid, so just replace it with an error
                # type = worksheet_util.TYPE_MARKUP
                subworksheet_info = {'uuid': subworksheet_uuid}
                # value = 'ERROR: non-existent worksheet %s' % subworksheet_uuid
        else:
            subworksheet_info = None
        value_obj = formatting.string_to_tokens(value) if type == worksheet_util.TYPE_DIRECTIVE else value
        new_items.append((bundle_info, subworksheet_info, value_obj, type))
    return new_items


@local_bundle_client_compatible
def update_worksheet_items(local, request, worksheet_info, new_items):
    """
    Set the worksheet to have items |new_items|.
    """
    worksheet_uuid = worksheet_info['uuid']
    last_item_id = worksheet_info['last_item_id']
    length = len(worksheet_info['items'])
    worksheet = local.model.get_worksheet(worksheet_uuid, fetch_items=False)
    check_worksheet_has_all_permission(local.model, request.user, worksheet)
    worksheet_util.check_worksheet_not_frozen(worksheet)
    try:
        new_items = [worksheet_util.convert_item_to_db(item) for item in new_items]
        local.model.update_worksheet_items(worksheet_uuid, last_item_id, length, new_items)
    except UsageError:
        # Turn the model error into a more readable one using the object.
        raise UsageError('%s was updated concurrently!' % (worksheet,))


@local_bundle_client_compatible
def update_worksheet_metadata(local, request, uuid, info):
    """
    Change the metadata of the worksheet |uuid| to |info|,
    where |info| specifies name, title, owner, etc.
    """
    worksheet = local.model.get_worksheet(uuid, fetch_items=False)
    check_worksheet_has_all_permission(local.model, request.user, worksheet)
    metadata = {}
    for key, value in info.items():
        if key == 'owner_spec':
            metadata['owner_id'] = local.model.find_user(value)
        elif key == 'name':
            ensure_unused_worksheet_name(value)
            metadata[key] = value
        elif key == 'title':
            metadata[key] = value
        elif key == 'tags':
            metadata[key] = value
        elif key == 'freeze':
            metadata['frozen'] = datetime.datetime.now()
        else:
            raise UsageError('Unknown key: %s' % key)
    local.model.update_worksheet_metadata(worksheet, metadata)


@local_bundle_client_compatible
def set_worksheet_permission(local, request, worksheet, group_uuid, permission):
    """
    Give the given |group_uuid| the desired |permission| on |worksheet_uuid|.
    """
    check_worksheet_has_all_permission(local.model, request.user, worksheet)
    local.model.set_group_worksheet_permission(group_uuid, worksheet.uuid, permission)


# FIXME(sckoo): fix when implementing worksheets API
@local_bundle_client_compatible
def populate_worksheet(local, request, worksheet, name, title):
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../objects/' + name + '.ws')
    lines = [line.rstrip() for line in open(file_path, 'r').readlines()]
    items, commands = worksheet_util.parse_worksheet_form(
        lines, ServerWorksheetResolver(local.model, request.user), worksheet.uuid)
    info = get_worksheet_info(worksheet.uuid, fetch_items=True)
    update_worksheet_items(info, items)
    update_worksheet_metadata(worksheet.uuid, {'title': title})


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
    if request.user is PUBLIC_USER:
        raise PermissionError("You must be logged in to create a worksheet.")
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
        populate_worksheet(worksheet, 'dashboard', 'CodaLab Dashboard')
    if spec_util.is_public_home(name):
        populate_worksheet(worksheet, 'home', 'Public Home')
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