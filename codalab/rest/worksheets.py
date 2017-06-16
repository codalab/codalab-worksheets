import json
import os
import random

import datetime
from bottle import get, post, put, delete, response, local, redirect, request

from codalab.common import PermissionError, UsageError, NotFoundError
from codalab.lib import (
    canonicalize,
    spec_util,
    worksheet_util,
)
from codalab.lib.canonicalize import HOME_WORKSHEET
from codalab.lib.server_util import (
    bottle_patch as patch,
    json_api_include,
    query_get_bool,
    query_get_json_api_include_set,
    query_get_list,
)
from codalab.model.tables import (
    GROUP_OBJECT_PERMISSION_ALL,
    GROUP_OBJECT_PERMISSION_READ,
)
from codalab.objects.permission import (
    check_worksheet_has_all_permission,
    check_worksheet_has_read_permission,
)
from codalab.objects.worksheet import Worksheet
from codalab.rest.schemas import WorksheetSchema, WorksheetPermissionSchema, \
    BundleSchema, WorksheetItemSchema
from codalab.rest.users import UserSchema
from codalab.rest.util import (
    get_bundle_infos,
    resolve_owner_in_keywords,
    get_resource_ids)
from codalab.server.authenticated_plugin import AuthenticatedPlugin


@get('/worksheets/<uuid:re:%s>' % spec_util.UUID_STR)
def fetch_worksheet(uuid):
    """
    Fetch a single worksheet by UUID.

    Query parameters:

     - `include`: comma-separated list of related resources to include, such as "owner"
    """
    include_set = query_get_json_api_include_set(supported={'owner', 'group_permissions', 'items', 'items.bundle', 'items.bundle.owner', 'items.subworksheet'})
    worksheet = get_worksheet_info(
        uuid,
        fetch_items='items' in include_set,
        fetch_permissions='group_permissions' in include_set,
    )

    # Build response document
    document = WorksheetSchema().dump(worksheet).data

    # Include items
    if 'items' in include_set:
        json_api_include(document, WorksheetItemSchema(), worksheet['items'])

    user_ids = set()

    # Include bundles
    if 'items.bundle' in include_set:
        bundle_uuids = {item['bundle_uuid'] for item in worksheet['items']
                        if item['type'] == worksheet_util.TYPE_BUNDLE and item['bundle_uuid'] is not None}
        bundle_infos = get_bundle_infos(bundle_uuids).values()
        json_api_include(document, BundleSchema(), bundle_infos)
        if 'items.bundle.owner' in include_set:
            user_ids.update({b['owner_id'] for b in bundle_infos})

    # Include users
    if 'owner' in include_set:
        user_ids.add(worksheet['owner_id'])
    if user_ids:
        json_api_include(document, UserSchema(), local.model.get_users(user_ids))

    # Include subworksheets
    if 'items.subworksheets' in include_set:
        subworksheet_uuids = {item['subworksheet_uuid']
                              for item in worksheet['items']
                              if item['type'] == worksheet_util.TYPE_WORKSHEET and item['subworksheet_uuid'] is not None}
        json_api_include(document, WorksheetSchema(), local.model.batch_get_worksheets(fetch_items=False, uuid=subworksheet_uuids))

    # Include permissions
    if 'group_permissions' in include_set:
        json_api_include(document, WorksheetPermissionSchema(), worksheet['group_permissions'])

    return document


@get('/worksheets')
def fetch_worksheets():
    """
    Fetch worksheets by worksheet specs (names) OR search keywords.

    Query parameters:

     - `include`: comma-separated list of related resources to include, such as "owner"
    """
    keywords = query_get_list('keywords')
    specs = query_get_list('specs')
    base_worksheet_uuid = request.query.get('base')
    include_set = query_get_json_api_include_set(supported={'owner', 'group_permissions'})

    if specs:
        uuids = [get_worksheet_uuid_or_create(base_worksheet_uuid, spec) for spec in specs]
        worksheets = [w.to_dict() for w in local.model.batch_get_worksheets(fetch_items=False, uuid=uuids)]
    else:
        keywords = resolve_owner_in_keywords(keywords)
        worksheets = local.model.search_worksheets(request.user.user_id, keywords)

    # Build response document
    document = WorksheetSchema(many=True).dump(worksheets).data

    # Include users
    if 'owner' in include_set:
        owner_ids = {w['owner_id'] for w in worksheets}
        if owner_ids:
            json_api_include(document, UserSchema(), local.model.get_users(owner_ids))

    # Include permissions
    if 'group_permissions' in include_set:
        for w in worksheets:
            if 'group_permissions' in w:
                json_api_include(document, WorksheetPermissionSchema(), w['group_permissions'])

    return document


@post('/worksheets', apply=AuthenticatedPlugin())
def create_worksheets():
    # TODO: support more attributes
    worksheets = WorksheetSchema(
        strict=True, many=True  # only allow name for now
    ).load(request.json).data

    for w in worksheets:
        w['uuid'] = new_worksheet(w['name'])

    return WorksheetSchema(many=True).dump(worksheets).data


@put('/worksheets/<uuid:re:%s>/raw' % spec_util.UUID_STR)
@post('/worksheets/<uuid:re:%s>/raw' % spec_util.UUID_STR)
def update_worksheet_raw(uuid):
    """
    Request body contains the raw lines of the worksheet.
    """
    lines = request.body.read().split(os.linesep)
    new_items = worksheet_util.parse_worksheet_form(lines, local.model, request.user, uuid)
    worksheet_info = get_worksheet_info(uuid, fetch_items=True)
    update_worksheet_items(worksheet_info, new_items)
    response.status = 204  # Success, No Content


@patch('/worksheets', apply=AuthenticatedPlugin())
def update_worksheets():
    """
    Bulk update worksheets metadata.
    """
    worksheet_updates = WorksheetSchema(
        strict=True, many=True,
    ).load(request.json, partial=True).data

    for w in worksheet_updates:
        update_worksheet_metadata(w['uuid'], w)

    return WorksheetSchema(many=True).dump(worksheet_updates).data


@delete('/worksheets', apply=AuthenticatedPlugin())
def delete_worksheets():
    """
    Delete the bundles specified.
    If |force|, allow deletion of bundles that have descendants or that appear across multiple worksheets.
    If |recursive|, add all bundles downstream too.
    If |data-only|, only remove from the bundle store, not the bundle metadata.
    If |dry-run|, just return list of bundles that would be deleted, but do not actually delete.
    """
    uuids = get_resource_ids(request.json, 'worksheets')
    force = query_get_bool('force', default=False)
    for uuid in uuids:
        delete_worksheet(uuid, force)


@post('/worksheet-items', apply=AuthenticatedPlugin())
def create_worksheet_items():
    """
    Bulk add worksheet items.

    |replace| - Replace existing items in host worksheets. Default is False.
    """
    replace = query_get_bool('replace', False)

    new_items = WorksheetItemSchema(
        strict=True, many=True,
    ).load(request.json).data

    worksheet_to_items = {}
    for item in new_items:
        worksheet_to_items.setdefault(item['worksheet_uuid'], []).append(item)

    for worksheet_uuid, items in worksheet_to_items.iteritems():
        worksheet_info = get_worksheet_info(worksheet_uuid, fetch_items=True)
        if replace:
            # Replace items in the worksheet
            update_worksheet_items(worksheet_info,
                                   [Worksheet.Item.as_tuple(i) for i in items],
                                   convert_items=False)
        else:
            # Append items to the worksheet
            for item in items:
                add_worksheet_item(worksheet_uuid, Worksheet.Item.as_tuple(item))

    return WorksheetItemSchema(many=True).dump(new_items).data


@post('/worksheet-permissions', apply=AuthenticatedPlugin())
def set_worksheet_permissions():
    """
    Bulk set worksheet permissions.
    """
    new_permissions = WorksheetPermissionSchema(
        strict=True, many=True,
    ).load(request.json).data

    for p in new_permissions:
        worksheet = local.model.get_worksheet(p['object_uuid'], fetch_items=False)
        set_worksheet_permission(worksheet, p['group_uuid'], p['permission'])
    return WorksheetPermissionSchema(many=True).dump(new_permissions).data


@get('/worksheets/sample/')
def get_sample_worksheets():
    """
    Get worksheets to display on the front page.
    Keep only |worksheet_uuids|.
    """
    # Select good high-quality worksheets and randomly choose some
    list_worksheets = search_worksheets(['tag=paper,software,data'])
    list_worksheets = random.sample(list_worksheets, min(3, len(list_worksheets)))

    # Always put home worksheet in
    list_worksheets = search_worksheets(['name=home']) + list_worksheets

    # Reformat
    list_worksheets = [{'uuid': val['uuid'],
                        'display_name': val.get('title') or val['name'],
                        'owner_name': val['owner_name']} for val in list_worksheets]

    response.content_type = 'application/json'
    return json.dumps(list_worksheets)


@get('/worksheets/')
def get_worksheets_landing():
    requested_ws = request.query.get('uuid', request.query.get('name', 'home'))
    uuid = get_worksheet_uuid_or_create(None, requested_ws)
    redirect('/worksheets/%s/' % uuid)


#############################################################
#  WORKSHEET HELPER FUNCTIONS
#############################################################


def get_worksheet_info(uuid, fetch_items=False, fetch_permissions=True):
    """
    The returned info object contains items which are (bundle_info, subworksheet_info, value_obj, type).
    """
    worksheet = local.model.get_worksheet(uuid, fetch_items=fetch_items)
    check_worksheet_has_read_permission(local.model, request.user, worksheet)
    permission = local.model.get_user_worksheet_permissions(
        request.user.user_id, [worksheet.uuid], {worksheet.uuid: worksheet.owner_id}
    )[worksheet.uuid]

    # Create the info by starting out with the metadata.
    result = worksheet.to_dict()
    result['permission'] = permission
    is_anonymous = permission < GROUP_OBJECT_PERMISSION_READ or (worksheet.is_anonymous and not permission >= GROUP_OBJECT_PERMISSION_ALL)

    # Mask owner identity on anonymous worksheet if don't have ALL permission
    if is_anonymous:
        result['owner_id'] = None

    # Note that these group_permissions is universal and permissions are relative to the current user.
    # Need to make another database query.
    if fetch_permissions:
        if is_anonymous:
            result['group_permissions'] = []
        else:
            result['group_permissions'] = local.model.get_group_worksheet_permissions(
                request.user.user_id, worksheet.uuid)

    return result


def update_worksheet_items(worksheet_info, new_items, convert_items=True):
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
        if convert_items:
            new_items = [worksheet_util.convert_item_to_db(item) for item in new_items]
        local.model.update_worksheet_items(worksheet_uuid, last_item_id, length, new_items)
    except UsageError:
        # Turn the model error into a more readable one using the object.
        raise UsageError('%s was updated concurrently!' % (worksheet,))


def update_worksheet_metadata(uuid, info):
    """
    Change the metadata of the worksheet |uuid| to |info|,
    where |info| specifies name, title, owner, etc.
    """
    worksheet = local.model.get_worksheet(uuid, fetch_items=False)
    check_worksheet_has_all_permission(local.model, request.user, worksheet)
    metadata = {}
    for key, value in info.items():
        if key == 'name':
            ensure_unused_worksheet_name(value)
        elif key == 'frozen' and value and not worksheet.frozen:
            # ignore the value the client provided, just freeze as long as it's truthy
            value = datetime.datetime.now()
        metadata[key] = value

    local.model.update_worksheet_metadata(worksheet, metadata)


def set_worksheet_permission(worksheet, group_uuid, permission):
    """
    Give the given |group_uuid| the desired |permission| on |worksheet_uuid|.
    """
    check_worksheet_has_all_permission(local.model, request.user, worksheet)
    local.model.set_group_worksheet_permission(group_uuid, worksheet.uuid, permission)


def populate_worksheet(worksheet, name, title):
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../objects/' + name + '.ws')
    lines = [line.rstrip() for line in open(file_path, 'r').readlines()]
    items = worksheet_util.parse_worksheet_form(lines, local.model, request.user, worksheet.uuid)
    info = get_worksheet_info(worksheet.uuid, fetch_items=True)
    update_worksheet_items(info, items)
    update_worksheet_metadata(worksheet.uuid, {'title': title})


def ensure_unused_worksheet_name(name):
    """
    Ensure worksheet names are unique.
    Note: for simplicity, we are ensuring uniqueness across the system, even on
    worksheet names that the user may not have access to.
    """
    # If trying to set the name to a home worksheet, then it better be
    # user's home worksheet.
    if spec_util.is_home_worksheet(name) and spec_util.home_worksheet(request.user.user_name) != name:
        raise UsageError('Cannot create %s because this is potentially the home worksheet of another user' % name)
    try:
        canonicalize.get_worksheet_uuid(local.model, request.user, None, name)
        raise UsageError('Worksheet with name %s already exists' % name)
    except NotFoundError:
        pass  # all good!


def new_worksheet(name):
    """
    Create a new worksheet with the given |name|.
    """
    if not request.user.is_authenticated:
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


def get_worksheet_uuid_or_create(base_worksheet_uuid, worksheet_spec):
    """
    Return the uuid of the specified worksheet if it exists.
    If not, create a new worksheet if the specified worksheet is home_worksheet
    or dashboard. Otherwise, throw an error.
    """
    try:
        return canonicalize.get_worksheet_uuid(local.model, request.user, base_worksheet_uuid, worksheet_spec)
    except NotFoundError:
        # A bit hacky, duplicates a bit of canonicalize
        if (worksheet_spec == '' or worksheet_spec == HOME_WORKSHEET) and request.user:
            return new_worksheet(spec_util.home_worksheet(request.user.user_name))
        elif spec_util.is_dashboard(worksheet_spec):
            return new_worksheet(worksheet_spec)
        else:
            raise


def add_worksheet_item(worksheet_uuid, item):
    """
    Add the given item to the worksheet.
    """
    worksheet = local.model.get_worksheet(worksheet_uuid, fetch_items=False)
    check_worksheet_has_all_permission(local.model, request.user, worksheet)
    worksheet_util.check_worksheet_not_frozen(worksheet)
    local.model.add_worksheet_item(worksheet_uuid, item)


def delete_worksheet(uuid, force):
    worksheet = local.model.get_worksheet(uuid, fetch_items=True)
    check_worksheet_has_all_permission(local.model, request.user, worksheet)
    if not force:
        if worksheet.frozen:
            raise UsageError("Can't delete worksheet %s because it is frozen (--force to override)." %
                             worksheet.uuid)
        if len(worksheet.items) > 0:
            raise UsageError("Can't delete worksheet %s because it is not empty (--force to override)." %
                             worksheet.uuid)
    local.model.delete_worksheet(uuid)


def search_worksheets(keywords):
    keywords = resolve_owner_in_keywords(keywords)
    results = local.model.search_worksheets(request.user.user_id, keywords)
    _set_owner_names(results)
    return results


def _set_owner_names(results):
    """
    Helper function: Set owner_name given owner_id of each item in results.
    """
    owners = [local.model.get_user(r['owner_id']) for r in results]
    for r, o in zip(results, owners):
        r['owner_name'] = o.user_name


