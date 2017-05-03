"""
Legacy REST APIs moved from the codalab-worksheets Django REST server.

Interfaces here should be removed over time as the frontend code migrates to
using the newer interfaces defined in the other modules in this package.
"""
import json
import threading

from bottle import (
  abort,
  get,
  httplib,
  local,
  post,
  request,
)

from codalab.bundles import get_bundle_subclass, PrivateBundle
from codalab.common import UsageError
from codalab.lib import (
  formatting,
  spec_util,
  worksheet_util,
)
from codalab.model.tables import GROUP_OBJECT_PERMISSION_ALL
from codalab.objects.permission import (
    permission_str,
    check_bundles_have_all_permission,
    check_bundles_have_read_permission,
)
from codalab.rest import util as rest_util
from codalab.rest.worksheets import (
    update_worksheet_items,
    get_worksheet_info,
)
from codalab.server.authenticated_plugin import AuthenticatedPlugin


@post('/api/worksheets/<uuid:re:%s>/' % spec_util.UUID_STR,
      apply=AuthenticatedPlugin())
def post_worksheet_content(uuid):
    """
    DEPRECATED: Use `POST /worksheets/<uuid>/raw` instead.
    """
    data = request.json
    lines = data['lines']
    parse_and_update_worksheet(uuid, lines)
    return {}


@get('/api/bundles/<uuid:re:%s>/' % spec_util.UUID_STR)
def get_bundle_info_(uuid):
    """
    DEPRECATED: Use `GET /bundles/<uuid>` instead.
    """
    bundle_info = get_bundle_info(uuid)
    if bundle_info is None:
        abort(httplib.NOT_FOUND, 'The bundle is not available')
    if bundle_info['bundle_type'] != PrivateBundle.BUNDLE_TYPE:
        bundle_info.update(get_bundle_file_contents(uuid))
    return bundle_info


@post('/api/bundles/<uuid:re:%s>/' % spec_util.UUID_STR)
def post_bundle_info(uuid):
    """
    Save metadata information for a bundle.

    DEPRECATED: Use `PATCH /bundles` instead.
    """
    bundle_info = get_bundle_info(uuid)
    # Save only if we're the owner.
    if bundle_info['edit_permission']:
        # TODO(klopyrev): The Content-Type header is not set correctly in
        # editable_field.jsx, so we can't use request.json.
        data = json.loads(request.body.read())
        new_metadata = data['metadata']

        # TODO: do this generally based on the CLI specs.
        # Remove generated fields.
        for key in ['data_size', 'created', 'time', 'time_user', 'time_system', 'memory', 'exitcode', 'actions', 'started', 'last_updated', 'run_status', 'job_handle']:
            if key in new_metadata:
                del new_metadata[key]

        # Convert to arrays
        for key in ['tags', 'language', 'architectures']:
            if key in new_metadata and isinstance(new_metadata[key], basestring):
                new_metadata[key] = new_metadata[key].split(',')

        # Convert to ints
        for key in ['request_cpus', 'request_gpus', 'request_priority']:
            if key in new_metadata:
                new_metadata[key] = int(new_metadata[key])

        update_bundle_metadata(uuid, new_metadata)
        bundle_info = get_bundle_info(uuid)
        return bundle_info
    else:
        abort(httplib.FORBIDDEN, 'Can\'t save unless you\'re the owner')


@get('/api/users/')
def get_users():
    """
    DEPRECATED: Use `GET /user` instead.
    """
    if request.user.is_authenticated:
        user_info = local.model.get_user_info(request.user.user_id, fetch_extra=False)
    else:
        user_info = None
    return {
        'user_info': user_info
    }


# Helper methods

def get_bundle_info(uuid):
    bundle_info = rest_util.get_bundle_info(uuid, True, True, True)

    if bundle_info is None:
        return None
    # Set permissions
    bundle_info['edit_permission'] = (bundle_info['permission'] == GROUP_OBJECT_PERMISSION_ALL)
    # Format permissions into strings
    bundle_info['permission_str'] = permission_str(bundle_info['permission'])
    for group_permission in bundle_info['group_permissions']:
        group_permission['permission_str'] = permission_str(group_permission['permission'])

    metadata = bundle_info['metadata']

    cls = get_bundle_subclass(bundle_info['bundle_type'])
    for key, value in worksheet_util.get_formatted_metadata(cls, metadata):
        metadata[key] = value

    bundle_info['metadata'] = metadata
    bundle_info['editable_metadata_fields'] = worksheet_util.get_editable_metadata_fields(cls)

    return bundle_info


def parse_and_update_worksheet(uuid, lines):
    """
    Replace worksheet |uuid| with the raw contents given by |lines|.
    """
    worksheet_info = get_worksheet_info(uuid, fetch_items=True, legacy=True)
    new_items, commands = worksheet_util.parse_worksheet_form(lines, local.model, request.user, worksheet_info['uuid'])
    update_worksheet_items(worksheet_info, new_items)
    # Note: commands are ignored


def validate_user_metadata(bundle_subclass, metadata):
    """
    Check that the user did not supply values for any auto-generated metadata.
    Raise a UsageError with the offending keys if they are.
    """
    # Allow generated keys as well
    legal_keys = set(spec.key for spec in bundle_subclass.METADATA_SPECS)
    illegal_keys = [key for key in metadata if key not in legal_keys]
    if illegal_keys:
        raise UsageError('Illegal metadata keys: %s' % (', '.join(illegal_keys),))


def check_target_has_read_permission(target):
    check_bundles_have_read_permission(local.model, request.user, [target[0]])


def get_bundle_file_contents(uuid):
    """
    If bundle is a single file, get file contents.
    Otherwise, get stdout and stderr.
    For each file, return a truncated version.
    """
    def get_summary(download_manager, info, name):
        if info['type'] == 'file':
            TRUNCATION_TEXT = (
                '\n'
                '... Truncated. Click link above to see full file. ...\n'
                '\n')
            contents = download_manager.summarize_file(
                uuid, name,
                num_head_lines=50, num_tail_lines=50, max_line_length=128,
                truncation_text=TRUNCATION_TEXT, gzipped=False)
            return formatting.verbose_contents_str(contents)
        elif info['type'] == 'link':
            return  ' -> ' + info['link']

    info = get_top_level_contents((uuid, ''))
    if info is None:
        return {}

    if info['type'] == 'file' or info['type'] == 'link':
        info['file_contents'] = get_summary(local.download_manager, info, '')
    elif info['type'] == 'directory':
        # Read contents of stdout and stderr, in parallel since when
        # fetching the data from a worker the read can be slow.
        info['stdout'] = None
        info['stderr'] = None

        info_lock = threading.Lock()
        def get_and_update_summary(download_manager, item, name):
            result = get_summary(download_manager, item, name)
            with info_lock:
                info[name] = result

        read_threads = []
        for item in info['contents']:
            name = item['name']
            if name in ['stdout', 'stderr'] and (item['type'] == 'file' or item['type'] == 'link'):
                th = threading.Thread(target=get_and_update_summary, args=[local.download_manager, item, name])
                th.start()
                read_threads.append(th)

        for th in read_threads:
            th.join()
    return info


# TODO: Replace with appropriate calls to download manager
def get_top_level_contents(target):
    info = get_target_info(target, 1)
    # Pre-format file sizes
    if info is not None and info['type'] == 'directory':
        for item in info['contents']:
            item['size_str'] = formatting.size_str(item['size'])
    return info


def get_target_info(target, depth):
    """
    Returns information about an individual target inside the bundle, or
    None if the target doesn't exist.
    """
    check_target_has_read_permission(target)
    return local.download_manager.get_target_info(target[0], target[1], depth)


def update_bundle_metadata(uuid, metadata):
    check_bundles_have_all_permission(local.model, request.user, [uuid])
    bundle = local.model.get_bundle(uuid)
    validate_user_metadata(bundle, metadata)
    local.model.update_bundle(bundle, {'metadata': metadata})


