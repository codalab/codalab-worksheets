import httplib
import mimetypes
import os
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

from codalab.bundles import BUNDLE_SUBCLASSES, get_bundle_subclass
from codalab.common import UsageError, State
from codalab.lib import spec_util, zip_util, worksheet_util
from codalab.lib.server_util import (
    bottle_patch as patch,
    json_api_include,
    json_api_meta,
    query_get_bool,
    query_get_list,
    query_get_type,
)
from codalab.lib.spec_util import validate_uuid, validate_child_path
from codalab.objects.permission import (
    check_bundles_have_all_permission,
    check_bundles_have_read_permission,
    check_worksheet_has_all_permission,
    PermissionSpec,
)
from codalab.rest.users import UserSchema
from codalab.rest.util import (
    check_worksheet_not_frozen,
    get_bundle_infos,
    get_resource_ids,
    resolve_bundle_specs,
    resolve_owner_in_keywords,
)
from codalab.server.authenticated_plugin import AuthenticatedPlugin


#############################################################
#  BUNDLE DE/SERIALIZATION AND VALIDATION SCHEMAS
#############################################################


class Metadata(fields.Field):
    @staticmethod
    def _get_metadata_specs(bundle):
        if 'bundle_type' not in bundle:
            raise ValidationError("'bundle_type' required to serialize metadata")
        return get_bundle_subclass(bundle['bundle_type']).METADATA_SPECS

    def _serialize(self, rows, attr, bundle):
        """
        Serialize metadata rows into map from metadata key to value.
        Originally Metadata.collapse_dicts
        """
        metadata_specs = self._get_metadata_specs(bundle)
        metadata_dict = {}
        metadata_spec_dict = {}
        for spec in metadata_specs:
            if spec.type == list or not spec.generated:
                metadata_dict[spec.key] = spec.get_constructor()()
            metadata_spec_dict[spec.key] = spec
        for row in rows:
            (maybe_unicode_key, value) = (row['metadata_key'], row['metadata_value'])
            # If the key is Unicode text (which is the case if it was extracted from a
            # database), cast it to a string. This operation encodes it with UTF-8.
            key = str(maybe_unicode_key)
            if key not in metadata_spec_dict:
                continue  # Somewhat dangerous since we might lose information

            spec = metadata_spec_dict[key]
            if spec.type == list:
                metadata_dict[key].append(value)
            else:
                if metadata_dict.get(key):
                    # Should be internal error
                    from codalab.common import UsageError
                    raise UsageError(
                        'Got duplicate values %s and %s for key %s' %
                        (metadata_dict[key], value, key)
                    )
                # Convert string to the right type (e.g., string to int)
                metadata_dict[key] = spec.get_constructor()(value)
        return metadata_dict

    def _deserialize(self, metadata_dict, attr, bundle_info):
        """
        Serialize this metadata object and return a list of dicts that can be
        saved to a MySQL table.
        Originally Metadata.to_dicts and Metadata.validate
        """
        metadata_specs = self._get_metadata_specs(bundle_info)
        expected_keys = set(spec.key for spec in metadata_specs)
        for key in metadata_dict:
            if key not in expected_keys:
                raise ValidationError('Unexpected metadata key: %s' % (key,))
        result = []
        for spec in metadata_specs:
            if spec.key in metadata_dict:
                raw_value = metadata_dict[spec.key]
                if raw_value is None:
                    continue
                if spec.validate is not None:
                    spec.validate(raw_value)
                if spec.type == float and isinstance(raw_value, int):
                    # cast int to float
                    raw_value = float(raw_value)
                if raw_value is not None and not isinstance(raw_value, spec.type):
                    raise ValidationError(
                        'Metadata value for %s should be of type %s, was %s (type %s)' %
                        (spec.key, spec.type, raw_value, type(raw_value))
                    )
                values = raw_value if spec.type == list else (raw_value,)
                for value in values:
                    result.append({
                        'metadata_key': unicode(spec.key),
                        'metadata_value': unicode(value),
                    })
            elif not spec.generated:
                raise ValidationError('Missing metadata key: %s' % (spec.key,))

        return result


class BundleDependencySchema(PlainSchema):
    """
    Plain (non-JSON API) Marshmallow schema for a single bundle dependency.
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
    metadata = Metadata()
    dependencies = fields.Nested(BundleDependencySchema, many=True)
    children = fields.Relationship(many=True, type_='bundles', id_field='uuid', include_data=True)
    group_permissions = fields.Relationship(many=True, type_='bundle-permissions', id_field='id', include_data=True)
    host_worksheets = fields.List(fields.Dict)

    # Bundle permission of the authenticated user for convenience, read-only
    permission = fields.Integer()
    permission_spec = PermissionSpec(attribute='permission')

    class Meta:
        type_ = 'bundles'


CREATE_RESTRICTED_FIELDS = ('id', 'uuid', 'data_hash', 'state', 'owner',
                            'children', 'group_permissions', 'host_worksheets',
                            'permission', 'permission_spec')


UPDATE_RESTRICTED_FIELDS = ('command', 'data_hash', 'state', 'dependencies',
                            'children', 'group_permissions', 'host_worksheets',
                            'permission', 'permission_spec')


#############################################################
#  BUNDLE REST API ENDPOINTS
#############################################################


@get('/bundles/<uuid:re:%s>' % spec_util.UUID_STR, apply=AuthenticatedPlugin())
def fetch_bundle(uuid):
    document = fetch_bundles_helper([uuid])
    document['data'] = document['data'][0]
    return document


@get('/bundles', apply=AuthenticatedPlugin())
def fetch_bundles():
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

    return fetch_bundles_helper(bundle_uuids)


def fetch_bundles_helper(bundle_uuids):
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
    children_uuids = set(uuid for bundle in bundles for uuid in bundle['children'])
    json_api_include(document, BundleSchema(), get_bundle_infos(children_uuids).values())

    # Include descendant ids
    if descendant_depth is not None:
        descendant_ids = local.model.get_self_and_descendants(bundle_uuids, depth=descendant_depth)
        json_api_meta(document, {'descendant_ids': descendant_ids})

    return document


@post('/bundles', apply=AuthenticatedPlugin())
def create_bundles():
    many = isinstance(request.json['data'], list)
    bundles = BundleSchema(
        strict=True,
        many=many,
        dump_only=CREATE_RESTRICTED_FIELDS,
    ).load(request.json).data

    # Multiplex between single and bulk requests
    if many:
        bundles = create_bundles_helper(bundles)
    else:
        bundles = create_bundles_helper([bundles])[0]

    return BundleSchema(many=many).dump(bundles).data


def create_bundles_helper(bundles):
    worksheet_uuid = request.query.get('worksheet')
    shadow_parent_uuid = request.query.get('shadows')
    if worksheet_uuid is None:
        abort(httplib.BAD_REQUEST, "Parent worksheet id must be specified as"
                                   "'worksheet' query parameter")

    worksheet = local.model.get_worksheet(worksheet_uuid, fetch_items=False)
    check_worksheet_has_all_permission(local.model, request.user, worksheet)
    check_worksheet_not_frozen(worksheet)
    request.user.check_quota(need_time=True, need_disk=True)

    created_uuids = []
    for bundle in bundles:
        # Prep bundle info for saving into database
        bundle_uuid = spec_util.generate_uuid()
        created_uuids.append(bundle_uuid)
        bundle['uuid'] = bundle_uuid
        bundle['owner_id'] = request.user.user_id
        bundle['state'] = State.CREATED
        bundle.setdefault('metadata', [])
        bundle['metadata'].append({
            'metadata_key': 'created',
            'metadata_value': int(time.time()),
        })
        for dep in bundle.get('dependencies', []):
            dep['child_uuid'] = bundle_uuid
        for dep in bundle.get('metadata', []):
            dep['bundle_uuid'] = bundle_uuid

        # Save bundle into model
        local.model.save_bundle_rest(bundle)

        # Inherit worksheet permissions
        group_permissions = local.model.get_group_worksheet_permissions(
            request.user.user_id, worksheet_uuid)
        set_bundle_permissions_helper([{
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

    # Get updated bundles
    bundles_dict = get_bundle_infos(created_uuids)

    # Return list of bundles in original order
    return [bundles_dict[uuid] for uuid in created_uuids]


@patch('/bundles', apply=AuthenticatedPlugin())
def update_bundles():
    many = isinstance(request.json['data'], list)
    bundle_updates = BundleSchema(
        strict=True,
        many=many,
        dump_only=UPDATE_RESTRICTED_FIELDS,
    ).load(request.json, partial=True).data

    # Multiplex between single and bulk requests
    if many:
        updated_bundles = update_bundles_helper(bundle_updates)
    else:
        updated_bundles = update_bundles_helper([bundle_updates])[0]

    return BundleSchema(many=many).dump(updated_bundles).data


def update_bundles_helper(bundle_updates):
    """
    Update bundle owners and/or metadata.
    """
    # Check permissions
    bundle_uuids = [b['uuid'] for b in bundle_updates]
    check_bundles_have_all_permission(local.model, request.user, bundle_uuids)

    # Check that bundle_types match original
    bundles_dict = get_bundle_infos(bundle_uuids)
    for update in bundle_updates:
        if ('bundle_type' in update and
                    update['bundle_type'] != bundles_dict[update['uuid']]['bundle_type']):
            abort(httplib.FORBIDDEN, "Updating bundle_type is forbidden")

    # Update bundles
    for update in bundle_updates:
        # Prep bundle and save to model
        for dep in update.get('metadata', []):
            dep['bundle_uuid'] = update['uuid']
        local.model.update_bundle_rest(update)

    # Get updated bundles
    bundles_dict = get_bundle_infos(bundle_uuids)

    # Create list of bundles in original order
    bundles = [bundles_dict[uuid] for uuid in bundle_uuids]

    return bundles


@delete('/bundles', apply=AuthenticatedPlugin())
def delete_bundles():
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

    relevant_uuids = local.model.get_self_and_descendants(uuids, depth=sys.maxint)
    uuids_set = set(uuids)
    relevant_uuids_set = set(relevant_uuids)
    if not recursive:
        # If any descendants exist, then we only delete uuids if force = True.
        if (not force) and uuids_set != relevant_uuids_set:
            relevant = local.model.batch_get_bundles(uuid=(set(relevant_uuids) - set(uuids)))
            raise UsageError('Can\'t delete bundles %s because the following bundles depend on them:\n  %s' % (
                ' '.join(uuids),
                '\n  '.join(bundle.simple_str() for bundle in relevant),
            ))
        relevant_uuids = uuids
    check_bundles_have_all_permission(local.model, request.user, relevant_uuids)

    # Make sure we don't delete bundles which are active
    states = local.model.get_bundle_states(uuids)
    active_states = set([State.MAKING, State.WAITING_FOR_WORKER_STARTUP, State.STARTING, State.RUNNING])
    active_uuids = [uuid for (uuid, state) in states.items() if state in active_states]
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

    # Delete the data_hash
    for uuid in relevant_uuids_set:
        # check first is needs to be deleted
        bundle_location = local.bundle_store.get_bundle_location(uuid)
        if os.path.lexists(bundle_location):
            local.bundle_store.cleanup(uuid, dry_run)

    # Return list of deleted ids
    return json_api_meta({}, {'ids': relevant_uuids})


@post('/bundle-permissions', apply=AuthenticatedPlugin())
def set_bundle_permissions():
    many = isinstance(request.json['data'], list)
    new_permissions = BundlePermissionSchema(
        strict=True, many=many,
    ).load(request.json).data

    # Multiplex between single and bulk requests
    if many:
        set_bundle_permissions_helper(new_permissions)
    else:
        set_bundle_permissions_helper([new_permissions])

    return BundlePermissionSchema(many=many).dump(new_permissions).data


# TODO(sckoo): JSON API requires that we return updated permissions
def set_bundle_permissions_helper(new_permissions):
    # Check permissions
    bundle_uuids = [p['object_uuid'] for p in new_permissions]
    check_bundles_have_all_permission(local.model, request.user, bundle_uuids)

    # Multiplex between updating, adding, or deleting permissions
    for p in new_permissions:
        old_permission = local.model.get_group_bundle_permission(p['group_uuid'], p['object_uuid'])
        new_permission = p['permission']
        if new_permission > 0:
            if old_permission > 0:
                local.model.update_bundle_permission(p['group_uuid'], p['object_uuid'], new_permission)
            else:
                local.model.add_bundle_permission(p['group_uuid'], p['object_uuid'], new_permission)
        else:
            if old_permission > 0:
                local.model.delete_bundle_permission(p['group_uuid'], p['object_uuid'])


@get('/bundles/<uuid:re:%s>/contents/blob/' % spec_util.UUID_STR)
@get('/bundles/<uuid:re:%s>/contents/blob/<path:path>' % spec_util.UUID_STR)
def fetch_bundle_contents_blob(uuid, path=''):
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
def update_bundle_contents_blob(uuid):
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
