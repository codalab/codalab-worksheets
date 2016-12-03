import httplib
import mimetypes
import os
import re
import sys
import time
from itertools import izip

from bottle import abort, get, post, put, delete, local, request, response

from codalab.bundles import (
    get_bundle_subclass,
    UploadedBundle,
)
from codalab.common import precondition, State, UsageError
from codalab.lib import canonicalize
from codalab.lib import (
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
from codalab.objects.permission import (
    check_bundles_have_all_permission,
    check_bundles_have_read_permission,
    check_worksheet_has_all_permission,
)
from codalab.rest.schemas import (
    BundleSchema,
    BundlePermissionSchema,
    BUNDLE_CREATE_RESTRICTED_FIELDS,
    BUNDLE_UPDATE_RESTRICTED_FIELDS,
)
from codalab.rest.users import UserSchema
from codalab.rest.util import (
    get_bundle_infos,
    get_resource_ids,
    resolve_owner_in_keywords,
)
from codalab.server.authenticated_plugin import AuthenticatedPlugin


@get('/bundles/<uuid:re:%s>' % spec_util.UUID_STR)
def _fetch_bundle(uuid):
    document = build_bundles_document([uuid])
    precondition(len(document['data']) == 1, "data should have exactly one element")
    document['data'] = document['data'][0]  # Flatten data list
    return document


@get('/bundles')
def _fetch_bundles():
    """
    Fetch bundles by bundle specs OR search keywords.
    """
    keywords = query_get_list('keywords')
    specs = query_get_list('specs')
    worksheet_uuid = request.query.get('worksheet')
    descendant_depth = query_get_type(int, 'depth', None)

    if keywords:
        # Handle search keywords
        keywords = resolve_owner_in_keywords(keywords)
        bundle_uuids = local.model.search_bundle_uuids(request.user.user_id, worksheet_uuid, keywords)
    elif specs:
        # Resolve bundle specs
        bundle_uuids = canonicalize.get_bundle_uuids(local.model, request.user, worksheet_uuid, specs)
    else:
        abort(httplib.BAD_REQUEST,
              "Request must include either 'keywords' "
              "or 'specs' query parameter")

    # Find all descendants down to the provided depth
    if descendant_depth is not None:
        bundle_uuids = local.model.get_self_and_descendants(bundle_uuids, depth=descendant_depth)

    # Return simple dict if scalar result (e.g. .sum or .count queries)
    if not isinstance(bundle_uuids, list):
        return json_api_meta({}, {'result': bundle_uuids})

    return build_bundles_document(bundle_uuids)


def build_bundles_document(bundle_uuids):
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
    for bundle, data in izip(bundles, document['data']):
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

    return document


@post('/bundles', apply=AuthenticatedPlugin())
def _create_bundles():
    """
    Bulk create bundles.

    |worksheet_uuid| - The parent worksheet of the bundle, add to this worksheet
                       if not detached or shadowing another bundle. Also used
                       to inherit permissions.
    |shadow| - the uuid of the bundle to shadow
    |detached| - True ('1') if should not add new bundle to any worksheet,
                 or False ('0') otherwise. Default is False.
    |wait_for_upload| - True ('1') if the bundle state should be initialized to
                        UPLOADING regardless of the bundle type, or False ('0')
                        otherwise. This prevents run bundles that are being
                        copied from another instance from being run by the
                        BundleManager. Default is False.
    """
    worksheet_uuid = request.query.get('worksheet')
    shadow_parent_uuid = request.query.get('shadow')
    detached = query_get_bool('detached', default=False)
    if worksheet_uuid is None:
        abort(httplib.BAD_REQUEST, "Parent worksheet id must be specified as"
                                   "'worksheet' query parameter")

    # Deserialize bundle fields
    bundles = BundleSchema(
        strict=True, many=True,
        dump_only=BUNDLE_CREATE_RESTRICTED_FIELDS,
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
        bundle_uuid = bundle.setdefault('uuid', spec_util.generate_uuid())
        created_uuids.append(bundle_uuid)
        bundle_class = get_bundle_subclass(bundle['bundle_type'])
        bundle['owner_id'] = request.user.user_id
        bundle['state'] = (State.UPLOADING
                           if issubclass(bundle_class, UploadedBundle)
                           or query_get_bool('wait_for_upload', False)
                           else State.CREATED)
        bundle.setdefault('metadata', {})['created'] = int(time.time())
        for dep in bundle.setdefault('dependencies', []):
            dep['child_uuid'] = bundle_uuid

        # Create bundle object
        bundle = bundle_class(bundle, strict=False)

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
        if not detached:
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
        dump_only=BUNDLE_UPDATE_RESTRICTED_FIELDS,
    ).load(request.json, partial=True).data

    # Check permissions
    bundle_uuids = [b['uuid'] for b in bundle_updates]
    check_bundles_have_all_permission(local.model, request.user, bundle_uuids)
    bundles = local.model.batch_get_bundles(uuid=bundle_uuids)

    # Update bundles
    for bundle, update in izip(bundles, bundle_updates):
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


@get('/bundles/<uuid:re:%s>/contents/info/' % spec_util.UUID_STR, name='fetch_bundle_contents_info')
@get('/bundles/<uuid:re:%s>/contents/info/<path:path>' % spec_util.UUID_STR, name='fetch_bundle_contents_info')
def _fetch_bundle_contents_info(uuid, path=''):
    depth = query_get_type(int, 'depth', default=0)
    if depth < 0:
        abort(httplib.BAD_REQUEST, "Depth must be at least 0")

    check_bundles_have_read_permission(local.model, request.user, [uuid])
    return {
        'data': local.download_manager.get_target_info(uuid, path, depth)
    }


@get('/bundles/<uuid:re:%s>/contents/blob/' % spec_util.UUID_STR, name='fetch_bundle_contents_blob')
@get('/bundles/<uuid:re:%s>/contents/blob/<path:path>' % spec_util.UUID_STR, name='fetch_bundle_contents_blob')
def _fetch_bundle_contents_blob(uuid, path=''):
    """
    API to download the contents of a bundle or a subpath within a bundle.

    For directories this method always returns a tarred and gzipped archive of
    the directory.

    For files, if the request has an Accept-Encoding header containing gzip,
    then the returned file is gzipped.
    """
    byte_range = get_request_range()
    head_lines = query_get_type(int, 'head', default=0)
    tail_lines = query_get_type(int, 'tail', default=0)
    max_line_length = query_get_type(int, 'max_line_length', default=128)
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
        if byte_range:
            abort(httplib.BAD_REQUEST, 'Range not supported for directory blobs.')
        if head_lines:
            abort(httplib.BAD_REQUEST, 'Head not supported for directory blobs.')
        # Always tar and gzip directories.
        filename = filename + '.tar.gz'
        fileobj = local.download_manager.stream_tarred_gzipped_directory(uuid, path)
    elif target_info['type'] == 'file':
        gzipped = False
        if not zip_util.path_is_archive(filename) and request_accepts_gzip_encoding():
            # Let's gzip to save bandwidth. The browser will transparently decode
            # the file.
            filename = filename + '.gz'
            gzipped = True

        if byte_range and (head_lines or tail_lines):
            abort(httplib.BAD_REQUEST, 'Head and range not supported on the same request.')
        elif byte_range:
            start, end = byte_range
            fileobj = local.download_manager.read_file_section(uuid, path, start, end - start + 1, gzipped)
        elif head_lines or tail_lines:
            fileobj = local.download_manager.summarize_file(uuid, path, head_lines, tail_lines, max_line_length, None, gzipped)
        else:
            fileobj = local.download_manager.stream_file(uuid, path, gzipped)
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
     name='update_bundle_contents_blob', apply=AuthenticatedPlugin())
def _update_bundle_contents_blob(uuid):
    """
    Update the contents of the given running or uploading bundle.

    Query parameters:
        urls - comma-separated list of URLs from which to fetch data to fill the
               bundle, using this option will ignore any uploaded file data
        git - (optional) 1 if URL should be interpreted as git repos to clone
              or 0 otherwise, default is 0
    OR
        filename - (optional) filename of the uploaded file, used to indicate
                   whether or not it is an archive, default is 'contents'

    Query parameters that are always available:
        unpack - (optional) 1 if the uploaded file should be unpacked if it is
                 an archive, or 0 otherwise, default is 1
        simplify - (optional) 1 if the uploaded file should be 'simplified' if
                   it is an archive, or 0 otherwise, default is 1
                   (See UploadManager for full explanation of 'simplification')
        finalize_on_failure - (optional) 1 if bundle state should be set to
                              'failed' in the case of a failure during upload,
                              or 0 if the bundle state should not change on
                              failure. Default is 0.
        state_on_success - (optional) Update the bundle state to this state if
                           the upload completes successfully. Must be either
                           'ready' or 'failed'. Default is 'ready'.
    """
    check_bundles_have_all_permission(local.model, request.user, [uuid])
    bundle = local.model.get_bundle(uuid)
    if bundle.state in State.FINAL_STATES:
        abort(httplib.FORBIDDEN, 'Contents cannot be modified, bundle already finalized.')

    # Get and validate query parameters
    finalize_on_failure = query_get_bool('finalize_on_failure', default=False)
    final_state = request.query.get('state_on_success', default=State.READY)
    if final_state not in State.FINAL_STATES:
        abort(httplib.BAD_REQUEST, 'state_on_success must be one of %s' % '|'.join(State.FINAL_STATES))

    # If this bundle already has data, remove it.
    if local.upload_manager.has_contents(bundle):
        local.upload_manager.cleanup_existing_contents(bundle)

    # Store the data.
    try:
        if request.query.urls:
            sources = query_get_list('urls')
        else:
            filename = request.query.get('filename', default='contents')
            sources = [(filename, request['wsgi.input'])]

        local.upload_manager.upload_to_bundle_store(
            bundle, sources=sources, follow_symlinks=False,
            exclude_patterns=None, remove_sources=False,
            git=query_get_bool('git', default=False),
            unpack=query_get_bool('unpack', default=True),
            simplify_archives=query_get_bool('simplify', default=True))

        local.upload_manager.update_metadata_and_save(bundle, new_bundle=False)

    except Exception as e:
        # Upload failed: cleanup, update state if desired, and return HTTP error
        if local.upload_manager.has_contents(bundle):
            local.upload_manager.cleanup_existing_contents(bundle)

        msg = "Upload failed: %s" % e

        # The client may not want to finalize the bundle on failure, to keep
        # open the possibility of retrying the upload in the case of transient
        # failure.
        # Workers also use this API endpoint to upload partial contents of
        # running bundles, and they should use finalize_on_failure=0 to avoid
        # letting transient errors during upload fail the bundles prematurely.
        if finalize_on_failure:
            local.model.update_bundle(bundle, {
                'state': State.FAILED,
                'metadata': {'failure_message': msg},
            })

        abort(httplib.INTERNAL_SERVER_ERROR, msg)

    else:
        # Upload succeeded: update state
        local.model.update_bundle(bundle, {'state': final_state})


#############################################################
#  BUNDLE HELPER FUNCTIONS
#############################################################

def get_request_range():
    """
    Parses header of the form:
        Range: bytes=START-END
    into tuple:
        (int(START), int(END))
    """
    if 'Range' not in request.headers:
        return None

    m = re.match(r'bytes=(\d+)-(\d+)', request.headers['Range'].strip())
    if m is None:
        abort(httplib.BAD_REQUEST, "Range must be 'bytes=START-END'.")

    start, end = m.groups()
    return int(start), int(end)


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


def delete_bundles(uuids, force, recursive, data_only, dry_run):
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


def set_bundle_permissions(new_permissions):
    # Check if current user has permission to set bundle permissions
    check_bundles_have_all_permission(
        local.model, request.user, [p['object_uuid'] for p in new_permissions])
    # Sequentially set bundle permissions
    for p in new_permissions:
        local.model.set_group_bundle_permission(
            p['group_uuid'], p['object_uuid'], p['permission'])
