import httplib
import mimetypes
import os
import re
import sys
import time

from bottle import abort, get, post, put, delete, local, request, response
from marshmallow import (
    ValidationError,
    Schema as PlainSchema,
    validate,
    validates_schema,
)
from marshmallow_jsonapi import Schema, fields

from codalab.bundles import BUNDLE_SUBCLASSES, get_bundle_subclass, PrivateBundle
from codalab.common import State, UsageError
from codalab.lib import (
    bundle_util,
    canonicalize,
    spec_util,
    zip_util,
    worksheet_util,
)
from codalab.lib.server_util import (
    bottle_patch as patch,
    json_api_include,
    json_api_meta,
    query_get_bool,
    query_get_list,
    query_get_type,
)
from codalab.lib.spec_util import validate_uuid, validate_child_path
from codalab.model.tables import GROUP_OBJECT_PERMISSION_READ
from codalab.objects.permission import (
    check_bundles_have_all_permission,
    check_bundles_have_read_permission,
    check_worksheet_has_all_permission,
    PermissionSpec,
)
from codalab.rest.users import UserSchema
from codalab.rest.worksheets import get_worksheet_uuid
from codalab.rest.util import (
    local_bundle_client_compatible,
    get_resource_ids,
)
from codalab.server.authenticated_plugin import AuthenticatedPlugin


#############################################################
#  BUNDLE DE/SERIALIZATION AND VALIDATION SCHEMAS
#############################################################


class BundleDependencySchema(PlainSchema):
    """
    Plain (non-JSONAPI) Marshmallow schema for a single bundle dependency.
    Not defining this as a separate resource with Relationships because we only
    create a set of dependencies once at bundle creation.
    """
    child_uuid = fields.String(validate=validate_uuid, dump_only=True)
    child_path = fields.String(validate=validate_child_path)
    parent_uuid = fields.String(validate=validate_uuid)
    parent_path = fields.String()
    parent_name = fields.Method('get_parent_name', dump_only=True)  # for convenience

    def get_parent_name(self, dep):
        uuid = dep['parent_uuid']
        return local.model.get_bundle_names([uuid]).get(uuid)


class BundlePermissionSchema(Schema):
    id = fields.Integer(as_string=True, dump_only=True)
    bundle = fields.Relationship(required=True, load_only=True, include_data=True, type_='bundles', attribute='object_uuid')
    group = fields.Relationship(required=True, include_data=True, type_='groups', attribute='group_uuid')
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
    bundle_type = fields.String(validate=validate.OneOf({bsc.BUNDLE_TYPE for bsc in BUNDLE_SUBCLASSES}))
    command = fields.String(allow_none=True)
    data_hash = fields.String()
    state = fields.String()
    owner = fields.Relationship(include_data=True, type_='users', attribute='owner_id')
    metadata = fields.Dict()
    dependencies = fields.Nested(BundleDependencySchema, many=True)
    children = fields.Relationship(many=True, type_='bundles', id_field='uuid', include_data=True)
    group_permissions = fields.Relationship(many=True, type_='bundle-permissions', id_field='id', include_data=True)
    host_worksheets = fields.List(fields.Dict)
    args = fields.String()

    # Bundle permission of the authenticated user for convenience, read-only
    permission = fields.Integer()
    permission_spec = PermissionSpec(attribute='permission')

    class Meta:
        type_ = 'bundles'


CREATE_RESTRICTED_FIELDS = ('id', 'uuid', 'data_hash', 'state', 'owner',
                            'children', 'group_permissions', 'host_worksheets',
                            'args', 'permission', 'permission_spec')


UPDATE_RESTRICTED_FIELDS = ('command', 'data_hash', 'state', 'dependencies',
                            'children', 'group_permissions', 'host_worksheets',
                            'args', 'permission', 'permission_spec',
                            'bundle_type')


#############################################################
#  BUNDLE REST API ENDPOINTS
#############################################################


@get('/bundles/<uuid:re:%s>' % spec_util.UUID_STR, apply=AuthenticatedPlugin())
def _fetch_bundle(uuid):
    document = build_bundles_document([uuid])
    document['data'] = document['data'][0]  # Flatten data list
    return document


@get('/bundles', apply=AuthenticatedPlugin())
def _fetch_bundles():
    """
    Fetch bundles by bundle specs OR search keywords.
    """
    keywords = query_get_list('keywords')
    specs = query_get_list('specs')
    worksheet_uuid = request.query.get('worksheet')

    if keywords:
        # Handle search keywords
        keywords = resolve_owner_in_keywords(keywords)
        bundle_uuids = local.model.search_bundle_uuids(request.user.user_id, worksheet_uuid, keywords)
    elif specs:
        # Resolve bundle specs
        bundle_uuids = resolve_bundle_specs(worksheet_uuid, specs)
    else:
        abort(httplib.BAD_REQUEST,
              "Request must include either 'keywords' "
              "or 'specs' query parameter")

    # Scalar result (e.g. .sum or .count queries)
    if not isinstance(bundle_uuids, list):
        return json_api_meta({}, {'result': bundle_uuids})

    return build_bundles_document(bundle_uuids)


def build_bundles_document(bundle_uuids):
    descendant_depth = query_get_type(int, 'list-descendants', None)

    bundles_dict = get_bundle_infos(
        bundle_uuids,
        get_children=True,
        get_permissions=True,
        get_host_worksheets=True,
    )

    # Create list of bundles in original order
    try:
        bundles = [bundles_dict[uuid] for uuid in bundle_uuids]
    except KeyError as e:
        abort(httplib.NOT_FOUND, "Bundle %s not found" % e.args[0])

    # Build response document
    document = BundleSchema(many=True).dump(bundles).data

    # Shim in editable metadata keys
    # Used by the front-end application
    for bundle, data in zip(bundles, document['data']):
        json_api_meta(data, {
            'editable_metadata_keys': worksheet_util.get_editable_metadata_fields(
                get_bundle_subclass(bundle['bundle_type']))
        })

    # Include users
    owner_ids = set(b['owner_id'] for b in bundles)
    json_api_include(document, UserSchema(), local.model.get_users(owner_ids))

    # Include permissions
    for bundle in bundles:
        json_api_include(document, BundlePermissionSchema(), bundle['group_permissions'])

    # Include child bundles
    children_uuids = set(c['uuid'] for bundle in bundles for c in bundle['children'])
    json_api_include(document, BundleSchema(), get_bundle_infos(children_uuids).values())

    # Include descendant ids
    if descendant_depth is not None:
        descendant_ids = local.model.get_self_and_descendants(bundle_uuids, depth=descendant_depth)
        json_api_meta(document, {'descendant_ids': descendant_ids})

    return document


@post('/bundles', apply=AuthenticatedPlugin())
def _create_bundles():
    """
    Bulk create bundles.
    """
    worksheet_uuid = request.query.get('worksheet')
    shadow_parent_uuid = request.query.get('shadows')
    if worksheet_uuid is None:
        abort(httplib.BAD_REQUEST, "Parent worksheet id must be specified as"
                                   "'worksheet' query parameter")

    # Deserialize bundle fields
    bundles = BundleSchema(
        strict=True, many=True,
        dump_only=CREATE_RESTRICTED_FIELDS,
    ).load(request.json).data

    # Check for all necessary permissions
    worksheet = local.model.get_worksheet(worksheet_uuid, fetch_items=False)
    check_worksheet_has_all_permission(local.model, request.user, worksheet)
    worksheet_util.check_worksheet_not_frozen(worksheet)
    request.user.check_quota(need_time=True, need_disk=True)

    created_uuids = []
    for bundle in bundles:
        # Prep bundle info for saving into database
        # Unfortunately cannot use the `construct` methods because they don't
        # provide a uniform interface for constructing bundles for all types
        # Hopefully this can all be unified after REST migration is complete
        bundle_uuid = spec_util.generate_uuid()
        created_uuids.append(bundle_uuid)
        bundle['uuid'] = bundle_uuid
        bundle['owner_id'] = request.user.user_id
        bundle['state'] = State.CREATED
        bundle.setdefault('metadata', {})['created'] = int(time.time())
        for dep in bundle.get('dependencies', []):
            dep['child_uuid'] = bundle_uuid

        # Create bundle object
        bundle = get_bundle_subclass(bundle['bundle_type'])(bundle, strict=False)

        # Save bundle into model
        local.model.save_bundle(bundle)

        # Inherit worksheet permissions
        group_permissions = local.model.get_group_worksheet_permissions(
            request.user.user_id, worksheet_uuid)
        set_bundle_permissions([{
            'object_uuid': bundle_uuid,
            'group_uuid': p['group_uuid'],
            'permission': p['permission'],
        } for p in group_permissions])

        # Add as item to worksheet
        if shadow_parent_uuid is None:
            local.model.add_worksheet_item(
                worksheet_uuid, worksheet_util.bundle_item(bundle_uuid))
        else:
            local.model.add_shadow_worksheet_items(
                shadow_parent_uuid, bundle_uuid)

    # Get created bundles
    bundles_dict = get_bundle_infos(created_uuids)

    # Return bundles in original order
    bundles = [bundles_dict[uuid] for uuid in created_uuids]
    return BundleSchema(many=True).dump(bundles).data


@patch('/bundles', apply=AuthenticatedPlugin())
def _update_bundles():
    """
    Bulk update bundles.
    """
    bundle_updates = BundleSchema(
        strict=True, many=True,
        dump_only=UPDATE_RESTRICTED_FIELDS,
    ).load(request.json, partial=True).data

    # Check permissions
    bundle_uuids = [b['uuid'] for b in bundle_updates]
    check_bundles_have_all_permission(local.model, request.user, bundle_uuids)

    # Update bundles
    for update in bundle_updates:
        bundle = local.model.get_bundle(update.pop('uuid'))
        local.model.update_bundle(bundle, update)

    # Get updated bundles
    bundles_dict = get_bundle_infos(bundle_uuids)

    # Create list of bundles in original order
    updated_bundles = [bundles_dict[uuid] for uuid in bundle_uuids]

    return BundleSchema(many=True).dump(updated_bundles).data


@delete('/bundles', apply=AuthenticatedPlugin())
def _delete_bundles():
    """
    Delete the bundles specified.
    If |force|, allow deletion of bundles that have descendants or that appear across multiple worksheets.
    If |recursive|, add all bundles downstream too.
    If |data-only|, only remove from the bundle store, not the bundle metadata.
    If |dry-run|, just return list of bundles that would be deleted, but do not actually delete.
    """
    uuids = get_resource_ids(request.json, 'bundles')
    force = query_get_bool('force', default=False)
    recursive = query_get_bool('recursive', default=False)
    data_only = query_get_bool('data-only', default=False)
    dry_run = query_get_bool('dry-run', default=False)
    deleted_uuids = delete_bundles(uuids, force=force, recursive=recursive,
                                   data_only=data_only, dry_run=dry_run)

    # Return list of deleted ids as meta
    return json_api_meta({}, {'ids': deleted_uuids})


@post('/bundle-permissions', apply=AuthenticatedPlugin())
def _set_bundle_permissions():
    """
    Bulk set bundle permissions.
    """
    new_permissions = BundlePermissionSchema(
        strict=True, many=True,
    ).load(request.json).data
    set_bundle_permissions(new_permissions)
    return BundlePermissionSchema(many=True).dump(new_permissions).data


@get('/bundles/<uuid:re:%s>/contents/blob/' % spec_util.UUID_STR)
@get('/bundles/<uuid:re:%s>/contents/blob/<path:path>' % spec_util.UUID_STR)
def _fetch_bundle_contents_blob(uuid, path=''):
    """
    API to download the contents of a bundle or a subpath within a bundle.

    For directories this method always returns a tarred and gzipped archive of
    the directory.

    For files, if the request has an Accept-Encoding header containing gzip,
    then the returned file is gzipped.
    """
    check_bundles_have_read_permission(local.model, request.user, [uuid])
    bundle = local.model.get_bundle(uuid)

    target_info = local.download_manager.get_target_info(uuid, path, 0)
    if target_info is None:
        abort(httplib.NOT_FOUND, 'Not found.')

    # Figure out the file name.
    if not path and bundle.metadata.name:
        filename = bundle.metadata.name
    else:
        filename = target_info['name']

    if target_info['type'] == 'directory':
        # Always tar and gzip directories.
        filename = filename + '.tar.gz'
        fileobj = local.download_manager.stream_tarred_gzipped_directory(uuid, path)
    elif target_info['type'] == 'file':
        if not zip_util.path_is_archive(filename) and request_accepts_gzip_encoding():
            # Let's gzip to save bandwidth. The browser will transparently decode
            # the file.
            filename = filename + '.gz'
            fileobj = local.download_manager.stream_file(uuid, path, gzipped=True)
        else:
            fileobj = local.download_manager.stream_file(uuid, path, gzipped=False)
    else:
        # Symlinks.
        abort(httplib.FORBIDDEN, 'Cannot download files of this type.')
    
    # Set headers.
    mimetype, _ = mimetypes.guess_type(filename, strict=False)
    response.set_header('Content-Type', mimetype or 'text/plain')
    if zip_util.get_archive_ext(filename) == '.gz' and request_accepts_gzip_encoding():
        filename = zip_util.strip_archive_ext(filename)
        response.set_header('Content-Encoding', 'gzip')
    else:
        response.set_header('Content-Encoding', 'identity')
    response.set_header('Content-Disposition', 'filename="%s"' % filename)

    return fileobj


@put('/bundles/<uuid:re:%s>/contents/blob/' % spec_util.UUID_STR,
     apply=AuthenticatedPlugin())
def _update_bundle_contents_blob(uuid):
    """
    Update the contents of the given running or uploading bundle.

    Accepts the filename as a query parameter, used to determine whether the
    upload contains an archive.
    """
    check_bundles_have_all_permission(local.model, request.user, [uuid])
    bundle = local.model.get_bundle(uuid)

    # If this bundle already has data, remove it.
    if local.upload_manager.has_contents(bundle):
        local.upload_manager.cleanup_existing_contents(bundle)

    # Store the data.
    try:
        local.upload_manager.upload_to_bundle_store(
            bundle, sources=[(request.query.filename, request['wsgi.input'])],
            follow_symlinks=False, exclude_patterns=False, remove_sources=False,
            git=False, unpack=True, simplify_archives=False)
        local.upload_manager.update_metadata_and_save(bundle, new_bundle=False)

    except Exception:
        if local.upload_manager.has_contents(bundle):
            local.upload_manager.cleanup_existing_contents(bundle)
        raise


#############################################################
#  BUNDLE HELPER FUNCTIONS
#############################################################


def request_accepts_gzip_encoding():
    # See rules for parsing here: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
    # Browsers silently decode gzipped files, so we save some bandwidth.
    if 'Accept-Encoding' not in request.headers:
        return False
    for encoding in request.headers['Accept-Encoding'].split(','):
        encoding = encoding.strip().split(';')
        if encoding[0] == 'gzip':
            if len(encoding) > 1 and encoding[1] == 'q=0':
                return False
            else:
                return True
    return False


def resolve_bundle_specs(worksheet_uuid, bundle_specs):
    return [resolve_bundle_spec(worksheet_uuid, bundle_spec)
            for bundle_spec in bundle_specs]


def resolve_bundle_spec(worksheet_uuid, bundle_spec):
    if '/' in bundle_spec:  # <worksheet_spec>/<bundle_spec>
        # Shift to new worksheet
        worksheet_spec, bundle_spec = bundle_spec.split('/', 1)
        worksheet_uuid = get_worksheet_uuid(worksheet_uuid, worksheet_spec)

    return canonicalize.get_bundle_uuid(local.model, request.user.user_id,
                                        worksheet_uuid, bundle_spec)


def mask_bundle(bundle_info):
    """
    Return a copy of the bundle_info dict that hides all fields except 'uuid'.
    """
    return {
        'uuid': bundle_info['uuid'],
        'bundle_type': PrivateBundle.BUNDLE_TYPE,
        'owner_id': None,
        'command': None,
        'data_hash': None,
        'state': None,
        'metadata': [{
            'metadata_key': 'name',
            'metadata_value': '<private>',
        }],
        'dependencies': [],
    }


@local_bundle_client_compatible
def delete_bundles(local, request, uuids, force, recursive, data_only, dry_run):
    """
    Delete the bundles specified by |uuids|.
    If |force|, allow deletion of bundles that have descendants or that appear across multiple worksheets.
    If |recursive|, add all bundles downstream too.
    If |data_only|, only remove from the bundle store, not the bundle metadata.
    """
    relevant_uuids = local.model.get_self_and_descendants(uuids, depth=sys.maxint)
    if not recursive:
        # If any descendants exist, then we only delete uuids if force = True.
        if (not force) and set(uuids) != set(relevant_uuids):
            relevant = local.model.batch_get_bundles(uuid=(set(relevant_uuids) - set(uuids)))
            raise UsageError('Can\'t delete bundles %s because the following bundles depend on them:\n  %s' % (
                ' '.join(uuids),
                '\n  '.join(bundle.simple_str() for bundle in relevant),
            ))
        relevant_uuids = uuids
    check_bundles_have_all_permission(local.model, request.user, relevant_uuids)

    # Make sure we don't delete bundles which are active.
    states = local.model.get_bundle_states(uuids)
    active_uuids = [uuid for (uuid, state) in states.items() if state in State.ACTIVE_STATES]
    if len(active_uuids) > 0:
        raise UsageError('Can\'t delete bundles: %s. ' % (' '.join(active_uuids)) +
                         'For run bundles, kill them first. ' +
                         'Bundles stuck not running will eventually ' +
                         'automatically be moved to a state where they ' +
                         'can be deleted.')

    # Make sure that bundles are not referenced in multiple places (otherwise, it's very dangerous)
    result = local.model.get_host_worksheet_uuids(relevant_uuids)
    for uuid, host_worksheet_uuids in result.items():
        worksheets = local.model.batch_get_worksheets(fetch_items=False, uuid=host_worksheet_uuids)
        frozen_worksheets = [worksheet for worksheet in worksheets if worksheet.frozen]
        if len(frozen_worksheets) > 0:
            raise UsageError("Can't delete bundle %s because it appears in frozen worksheets "
                             "(need to delete worksheet first):\n  %s" %
                             (uuid, '\n  '.join(worksheet.simple_str() for worksheet in frozen_worksheets)))
        if not force and len(host_worksheet_uuids) > 1:
            raise UsageError("Can't delete bundle %s because it appears in multiple worksheets "
                             "(--force to override):\n  %s" %
                             (uuid, '\n  '.join(worksheet.simple_str() for worksheet in worksheets)))

    # Delete the actual bundle
    if not dry_run:
        if data_only:
            # Just remove references to the data hashes
            local.model.remove_data_hash_references(relevant_uuids)
        else:
            # Actually delete the bundle
            local.model.delete_bundles(relevant_uuids)

        # Update user statistics
        local.model.update_user_disk_used(request.user.user_id)

    # Delete the data.
    for uuid in relevant_uuids:
        # check first is needs to be deleted
        bundle_location = local.bundle_store.get_bundle_location(uuid)
        if os.path.lexists(bundle_location):
            local.bundle_store.cleanup(uuid, dry_run)

    return relevant_uuids


@local_bundle_client_compatible
def get_bundle_infos(local, request, uuids, get_children=False,
                     get_host_worksheets=False, get_permissions=False):
    """
    get_children, get_host_worksheets, get_permissions:
        whether we want to return more detailed information.
    Return map from bundle uuid to info.
    """
    if len(uuids) == 0:
        return {}
    bundles = local.model.batch_get_bundles(uuid=uuids)
    bundle_dict = {bundle.uuid: bundle_util.bundle_to_bundle_info(local.model, bundle) for bundle in bundles}

    # Filter out bundles that we don't have read permission on
    def select_unreadable_bundles(uuids):
        permissions = local.model.get_user_bundle_permissions(request.user.user_id, uuids, local.model.get_bundle_owner_ids(uuids))
        return [uuid for uuid, permission in permissions.items() if permission < GROUP_OBJECT_PERMISSION_READ]

    def select_unreadable_worksheets(uuids):
        permissions = local.model.get_user_worksheet_permissions(request.user.user_id, uuids, local.model.get_worksheet_owner_ids(uuids))
        return [uuid for uuid, permission in permissions.items() if permission < GROUP_OBJECT_PERMISSION_READ]

    # Lookup the user names of all the owners
    user_ids = [info['owner_id'] for info in bundle_dict.values()]
    users = local.model.get_users(user_ids=user_ids) if len(user_ids) > 0 else []
    users = {u.user_id: u for u in users}
    if users:
        for info in bundle_dict.values():
            user = users[info['owner_id']]
            info['owner_name'] = user.user_name if user else None
            info['owner'] = '%s(%s)' % (info['owner_name'], info['owner_id'])

    # Mask bundles that we can't access
    for uuid in select_unreadable_bundles(uuids):
        if uuid in bundle_dict:
            bundle_dict[uuid] = mask_bundle(bundle_dict[uuid])

    if get_children:
        result = local.model.get_children_uuids(uuids)
        # Gather all children bundle uuids
        children_uuids = [uuid for l in result.values() for uuid in l]
        unreadable = set(select_unreadable_bundles(children_uuids))
        # Lookup bundle names
        names = local.model.get_bundle_names(children_uuids)
        # Fill in info
        for uuid, info in bundle_dict.items():
            info['children'] = [
                {
                    'uuid': child_uuid,
                    'metadata': {'name': names[child_uuid]}
                }
                for child_uuid in result[uuid] if child_uuid not in unreadable]

    if get_host_worksheets:
        # bundle_uuids -> list of worksheet_uuids
        result = local.model.get_host_worksheet_uuids(uuids)
        # Gather all worksheet uuids
        worksheet_uuids = [uuid for l in result.values() for uuid in l]
        unreadable = set(select_unreadable_worksheets(worksheet_uuids))
        worksheet_uuids = [uuid for uuid in worksheet_uuids if uuid not in unreadable]
        # Lookup names
        worksheets = dict(
            (worksheet.uuid, worksheet)
            for worksheet in local.model.batch_get_worksheets(
                fetch_items=False,
                uuid=worksheet_uuids))
        # Fill the info
        for uuid, info in bundle_dict.items():
            info['host_worksheets'] = [
                {
                    'uuid': worksheet_uuid,
                    'name': worksheets[worksheet_uuid].name
                }
                for worksheet_uuid in result[uuid]
                if worksheet_uuid not in unreadable]

    if get_permissions:
        # Fill the info
        group_result = local.model.batch_get_group_bundle_permissions(request.user.user_id, uuids)
        result = local.model.get_user_bundle_permissions(request.user.user_id, uuids, local.model.get_bundle_owner_ids(uuids))
        for uuid, info in bundle_dict.items():
            info['group_permissions'] = group_result[uuid]
            info['permission'] = result[uuid]

    return bundle_dict


@local_bundle_client_compatible
def resolve_owner_in_keywords(local, request, keywords):
    # Resolve references to owner ids
    def resolve(keyword):
        # Example: owner=codalab => owner_id=0
        m = re.match('owner=(.+)', keyword)
        if not m:
            return keyword
        return 'owner_id=%s' % getattr(local.model.get_user(username=m.group(1)), 'user_id', 'x')
    return map(resolve, keywords)


@local_bundle_client_compatible
def set_bundle_permissions(local, request, new_permissions):
    # Check if current user has permission to set bundle permissions
    check_bundles_have_all_permission(
        local.model, request.user, [p['object_uuid'] for p in new_permissions])
    # Sequentially set bundle permissions
    for p in new_permissions:
        local.model.set_group_bundle_permission(
            p['group_uuid'], p['object_uuid'], p['permission'])
