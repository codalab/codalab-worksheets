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
from codalab.lib import (
    canonicalize,
    spec_util,
    worksheet_util,
)
from codalab.lib.server_util import (
    bottle_patch as patch,
    json_api_include,
    query_get_json_api_include_set,
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
    WorksheetSchema,
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
    """
    Fetch bundle by UUID.

    Query parameters:

     - `include_display_metadata`: `1` to include additional metadata helpful
       for displaying the bundle info, `0` to omit them. Default is `0`.
     - `include`: comma-separated list of related resources to include, such as "owner"
    """
    document = build_bundles_document([uuid])
    precondition(len(document['data']) == 1, "data should have exactly one element")
    document['data'] = document['data'][0]  # Flatten data list
    return document


@get('/bundles')
def _fetch_bundles():
    """
    Fetch bundles by bundle `specs` OR search `keywords`. Behavior is undefined
    when both `specs` and `keywords` are provided.

    Query parameters:

     - `worksheet`: UUID of the base worksheet. Required when fetching by specs.
     - `specs`: Bundle spec of bundle to fetch. May be provided multiples times
        to fetch multiple bundle specs. A bundle spec is either:
        1. a UUID (8 or 32 hex characters with a preceding '0x')
        2. a bundle name referring to the last bundle with that name on the
           given base worksheet
        3. or a reverse index of the form `^N` referring to the Nth-to-last
           bundle on the given base worksheet.
     - `keywords`: Search keyword. May be provided multiples times for multiple
        keywords. Bare keywords match the names and descriptions of bundles.
        Examples of other special keyword forms:
        - `name=<name>            ` : More targeted search of using metadata fields.
        - `size=.sort             ` : Sort by a particular field.
        - `size=.sort-            ` : Sort by a particular field in reverse.
        - `size=.sum              ` : Compute total of a particular field.
        - `.mine                  ` : Match only bundles I own.
        - `.floating              ` : Match bundles that aren't on any worksheet.
        - `.count                 ` : Count the number of bundles.
        - `.limit=10              ` : Limit the number of results to the top 10.
     - `include_display_metadata`: `1` to include additional metadata helpful
       for displaying the bundle info, `0` to omit them. Default is `0`.
     - `include`: comma-separated list of related resources to include, such as "owner"

    When aggregation keywords such as `.count` are used, the resulting value
    is returned as:
    ```
    {
        "meta": {
            "results": <value>
        }
    }
    ```

    """
    keywords = query_get_list('keywords')
    specs = query_get_list('specs')
    worksheet_uuid = request.query.get('worksheet')
    descendant_depth = query_get_type(int, 'depth', None)

    if keywords:
        # Handle search keywords
        keywords = resolve_owner_in_keywords(keywords)
        bundle_uuids = local.model.search_bundle_uuids(request.user.user_id, keywords)
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
    include_set = query_get_json_api_include_set(supported={'owner', 'group_permissions', 'children', 'host_worksheets'})

    bundles_dict = get_bundle_infos(
        bundle_uuids,
        get_children='children' in include_set,
        get_permissions='group_permissions' in include_set,
        get_host_worksheets='host_worksheets' in include_set,
        ignore_not_found=False,
    )

    # Create list of bundles in original order
    bundles = [bundles_dict[uuid] for uuid in bundle_uuids]

    # Build response document
    document = BundleSchema(many=True).dump(bundles).data

    # Shim in display metadata used by the front-end application
    if query_get_bool('include_display_metadata', default=False):
        for bundle, data in izip(bundles, document['data']):
            bundle_class = get_bundle_subclass(bundle['bundle_type'])
            json_api_meta(data, {
                'editable_metadata_keys':
                    worksheet_util.get_editable_metadata_fields(bundle_class),
                'metadata_type':
                    worksheet_util.get_metadata_types(bundle_class),
            })

    if 'owner' in include_set:
        owner_ids = set(b['owner_id'] for b in bundles if b['owner_id'] is not None)
        json_api_include(document, UserSchema(), local.model.get_users(owner_ids))

    if 'group_permissions' in include_set:
        for bundle in bundles:
            json_api_include(document, BundlePermissionSchema(), bundle.get('group_permissions', []))

    if 'children' in include_set:
        for bundle in bundles:
            json_api_include(document, BundleSchema(), bundle.get('children', []))

    if 'host_worksheets' in include_set:
        for bundle in bundles:
            json_api_include(document, WorksheetSchema(), bundle.get('host_worksheets', []))

    return document


@post('/bundles', apply=AuthenticatedPlugin())
def _create_bundles():
    """
    Bulk create bundles.

    Query parameters:
    - `worksheet`: UUID of the parent worksheet of the new bundle, add to
      this worksheet if not detached or shadowing another bundle. The new
      bundle also inherits permissions from this worksheet.
    - `shadow`: UUID of the bundle to "shadow" (the new bundle will be added
      as an item immediately after this bundle in its parent worksheet).
    - `detached`: 1 if should not add new bundle to any worksheet,
      or 0 otherwise. Default is 0.
    - `wait_for_upload`: 1 if the bundle state should be initialized to
      "uploading" regardless of the bundle type, or 0 otherwise. Used when
      copying bundles from another CodaLab instance, this prevents these new
      bundles from being executed by the BundleManager. Default is 0.
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
        bundle['is_anonymous'] = worksheet.is_anonymous  # inherit worksheet anonymity
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
    bundle_uuids = [b.pop('uuid') for b in bundle_updates]
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

    Query parameters:
     - `force`: 1 to allow deletion of bundles that have descendants or that
       appear across multiple worksheets, or 0 to throw an error if any of the
       specified bundles have multiple references. Default is 0.
     - `recursive`: 1 to remove all bundles downstream too, or 0 otherwise.
       Default is 0.
     - `data-only`: 1 to only remove contents of the bundle(s) from the bundle
       store and leave the bundle metadata intact, or 0 to remove both the
       bundle contents and the bundle metadata. Default is 0.
     - `dry-run`: 1 to just return list of bundles that would be deleted with
       the given parameters without actually deleting them, or 0 to perform
       the deletion. Default is 0.
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

    A bundle permission created on a bundle-group pair will replace any
    existing permissions on the same bundle-group pair.
    """
    new_permissions = BundlePermissionSchema(
        strict=True, many=True,
    ).load(request.json).data
    set_bundle_permissions(new_permissions)
    return BundlePermissionSchema(many=True).dump(new_permissions).data


@get('/bundles/<uuid:re:%s>/contents/info/' % spec_util.UUID_STR, name='fetch_bundle_contents_info')
@get('/bundles/<uuid:re:%s>/contents/info/<path:path>' % spec_util.UUID_STR, name='fetch_bundle_contents_info')
def _fetch_bundle_contents_info(uuid, path=''):
    """
    Fetch metadata of the bundle contents or a subpath within the bundle.

    Query parameters:
    - `depth`: recursively fetch subdirectory info up to this depth.
      Default is 0.

    Response format:
    ```
    {
      "data": {
          "name": "<name of file or directory>",
          "link": "<string representing target if file is a symbolic link>",
          "type": "<file|directory|link>",
          "size": <size of file in bytes>,
          "perm": <unix permission integer>,
          "contents": [
              {
                "name": ...,
                <each file of directory represented recursively with the same schema>
              },
              ...
          ]
      }
    }
    ```
    """
    depth = query_get_type(int, 'depth', default=0)
    if depth < 0:
        abort(httplib.BAD_REQUEST, "Depth must be at least 0")

    check_bundles_have_read_permission(local.model, request.user, [uuid])
    info = local.download_manager.get_target_info(uuid, path, depth)
    if info is None:
        abort(httplib.NOT_FOUND, 'Bundle not found')

    return {
        'data': info
    }

@put('/bundles/<uuid:re:%s>/netcat/<port:int>/' % spec_util.UUID_STR, name='netcat_bundle')
def _netcat_bundle(uuid, port):
    """
    Send a raw bytestring into the specified port of the running bundle with uuid.
    Return the response from this bundle.
    """
    check_bundles_have_read_permission(local.model, request.user, [uuid])
    bundle = local.model.get_bundle(uuid)
    if bundle.state in State.FINAL_STATES:
        abort(httplib.FORBIDDEN, 'Cannot netcat bundle, bundle already finalized.')
    info = local.download_manager.netcat(uuid, port, request.json['message'])
    return {'data': info}

@post('/bundles/<uuid:re:%s>/netcurl/<port:int>/<path:re:.*>' % spec_util.UUID_STR, name='netcurl_bundle')
@put('/bundles/<uuid:re:%s>/netcurl/<port:int>/<path:re:.*>' % spec_util.UUID_STR, name='netcurl_bundle')
@delete('/bundles/<uuid:re:%s>/netcurl/<port:int>/<path:re:.*>' % spec_util.UUID_STR, name='netcurl_bundle')
@get('/bundles/<uuid:re:%s>/netcurl/<port:int>/<path:re:.*>' % spec_util.UUID_STR, name='netcurl_bundle')
@patch('/bundles/<uuid:re:%s>/netcurl/<port:int>/<path:re:.*>' % spec_util.UUID_STR, name='netcurl_bundle')
def _netcurl_bundle(uuid, port, path=''):
    """
    Forward an HTTP request into the specified port of the running bundle with uuid.
    Return the HTTP response from this bundle.
    """
    check_bundles_have_read_permission(local.model, request.user, [uuid])
    bundle = local.model.get_bundle(uuid)
    if bundle.state in State.FINAL_STATES:
        abort(httplib.FORBIDDEN, 'Cannot netcurl bundle, bundle already finalized.')

    try:
        request.path_shift(4) # shift away the routing parts of the URL

        headers_string = ['{}: {}'.format(h, request.headers.get(h)) for h in request.headers.keys()]
        message = "{} {} HTTP/1.1\r\n".format(request.method, request.path)
        message += "\r\n".join(headers_string) + "\r\n"
        message += "\r\n"
        message += request.body.read()

        info = local.download_manager.netcat(uuid, port, message)
    except:
        print >>sys.stderr, "{}".format(request.environ)
        raise
    finally:
        request.path_shift(-4) # restore the URL

    return info

@get('/bundles/<uuid:re:%s>/contents/blob/' % spec_util.UUID_STR, name='fetch_bundle_contents_blob')
@get('/bundles/<uuid:re:%s>/contents/blob/<path:path>' % spec_util.UUID_STR, name='fetch_bundle_contents_blob')
def _fetch_bundle_contents_blob(uuid, path=''):
    """
    API to download the contents of a bundle or a subpath within a bundle.

    For directories, this method always returns a tarred and gzipped archive of
    the directory.

    For files, if the request has an Accept-Encoding header containing gzip,
    then the returned file is gzipped. Otherwise, the file is returned as-is.

    HTTP Request headers:
    - `Range: bytes=<start>-<end>`: fetch bytes from the range
      `[<start>, <end>)`.
    - `Accept-Encoding: <encoding>`: indicate that the client can accept
      encoding `<encoding>`. Currently only `gzip` encoding is supported.

    Query parameters:
    - `head`: number of lines to fetch from the beginning of the file.
      Default is 0, meaning to fetch the entire file.
    - `tail`: number of lines to fetch from the end of the file.
      Default is 0, meaning to fetch the entire file.
    - `max_line_length`: maximum number of characters to fetch from each line,
      if either `head` or `tail` is specified. Default is 128.

    HTTP Response headers (for single-file targets):
    - `Content-Disposition: filename=<bundle name or target filename>`
    - `Content-Type: <guess of mimetype based on file extension>`
    - `Content-Encoding: [gzip|identity]`
    - `Target-Type: file`

    HTTP Response headers (for directories):
    - `Content-Disposition: filename=<bundle or directory name>.tar.gz`
    - `Content-Type: application/gzip`
    - `Content-Encoding: identity`
    - `Target-Type: directory`
    """
    byte_range = get_request_range()
    head_lines = query_get_type(int, 'head', default=0)
    tail_lines = query_get_type(int, 'tail', default=0)
    truncation_text = query_get_type(str, 'truncation_text', default='')
    max_line_length = query_get_type(int, 'max_line_length', default=128)
    check_bundles_have_read_permission(local.model, request.user, [uuid])
    bundle = local.model.get_bundle(uuid)

    target_info = local.download_manager.get_target_info(uuid, path, 0)
    if target_info is None:
        abort(httplib.NOT_FOUND, 'Invalid path "%s" in bundle with UUID (%s)' % (path, uuid))

    # Figure out the file name.
    if not path and bundle.metadata.name:
        filename = bundle.metadata.name
    else:
        filename = target_info['name']

    if target_info['type'] == 'directory':
        if byte_range:
            abort(httplib.BAD_REQUEST, 'Range not supported for directory blobs.')
        if head_lines or tail_lines:
            abort(httplib.BAD_REQUEST, 'Head and tail not supported for directory blobs.')
        # Always tar and gzip directories
        gzipped_stream = False  # but don't set the encoding to 'gzip'
        mimetype = 'application/gzip'
        filename += '.tar.gz'
        fileobj = local.download_manager.stream_tarred_gzipped_directory(uuid, path)
    elif target_info['type'] == 'file':
        # Let's gzip to save bandwidth.
        # For simplicity, we do this even if the file is already a packed
        # archive (which should be relatively rare).
        # The browser will transparently decode the file.
        gzipped_stream = request_accepts_gzip_encoding()

        # Since guess_type() will interpret '.tar.gz' as an 'application/x-tar' file
        # with 'gzip' encoding, which would usually go into the Content-Encoding
        # header. But if the bundle contents is actually a packed archive, we don't
        # want the client to automatically decompress the file, so we don't want to
        # set the Content-Encoding header. Instead, if guess_type() detects an
        # archive, we just set mimetype to indicate an arbitrary binary file.
        mimetype, encoding = mimetypes.guess_type(filename, strict=False)
        if encoding is not None:
            mimetype = 'application/octet-stream'

        if byte_range and (head_lines or tail_lines):
            abort(httplib.BAD_REQUEST, 'Head and range not supported on the same request.')
        elif byte_range:
            start, end = byte_range
            fileobj = local.download_manager.read_file_section(uuid, path, start, end - start + 1, gzipped_stream)
        elif head_lines or tail_lines:
            fileobj = local.download_manager.summarize_file(uuid, path, head_lines, tail_lines, max_line_length, truncation_text, gzipped_stream)
        else:
            fileobj = local.download_manager.stream_file(uuid, path, gzipped_stream)
    else:
        # Symlinks.
        abort(httplib.FORBIDDEN, 'Cannot download files of this type (%s).' % target_info['type'])

    # Set headers.
    response.set_header('Content-Type', mimetype or 'text/plain')
    response.set_header('Content-Encoding', 'gzip' if gzipped_stream else 'identity')
    response.set_header('Content-Disposition', 'attachment; filename="%s"' % filename)
    response.set_header('Target-Type', target_info['type'])

    return fileobj


@put('/bundles/<uuid:re:%s>/contents/blob/' % spec_util.UUID_STR,
     name='update_bundle_contents_blob', apply=AuthenticatedPlugin())
def _update_bundle_contents_blob(uuid):
    """
    Update the contents of the given running or uploading bundle.

    Query parameters:
    - `urls`: (optional) comma-separated list of URLs from which to fetch data
      to fill the bundle, using this option will ignore any uploaded file data
    - `git`: (optional) 1 if URL should be interpreted as git repos to clone
      or 0 otherwise, default is 0.
    - `filename`: (optional) filename of the uploaded file, used to indicate
      whether or not it is an archive, default is 'contents'
    - `unpack`: (optional) 1 if the uploaded file should be unpacked if it is
      an archive, or 0 otherwise, default is 1
    - `simplify`: (optional) 1 if the uploaded file should be 'simplified' if
      it is an archive, or 0 otherwise, default is 1.
    - `finalize_on_failure`: (optional) 1 if bundle state should be set
      to 'failed' in the case of a failure during upload, or 0 if the bundle
      state should not change on failure. Default is 0.
    - `state_on_success`: (optional) Update the bundle state to this state if
      the upload completes successfully. Must be either 'ready' or 'failed'.
      Default is 'ready'.
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
            simplify_archives=query_get_bool('simplify', default=True)) # See UploadManager for full explanation of 'simplify'

        local.upload_manager.update_metadata_and_save(bundle, enforce_disk_quota=True)

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
