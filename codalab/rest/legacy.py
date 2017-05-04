"""
Legacy REST APIs moved from the codalab-worksheets Django REST server.

This entire module and the routes defined here are deprecated.
Interfaces here should be removed over time as the frontend code migrates to
using the newer interfaces defined in the other modules in this package.
"""
import threading

from bottle import abort, get, httplib, local

from codalab.bundles import get_bundle_subclass, PrivateBundle
from codalab.lib import (
  formatting,
  spec_util,
  worksheet_util,
)
from codalab.model.tables import GROUP_OBJECT_PERMISSION_ALL
from codalab.objects.permission import permission_str
from codalab.rest import util as rest_util


@get('/api/bundles/<uuid:re:%s>/' % spec_util.UUID_STR)
def get_bundle_info_(uuid):
    """
    Fetch bundle info along with summaries of its top level contents.

    DEPRECATED: Use `GET /bundles/<uuid>` and `GET /bundles/<uuid>/contents/*` instead.
    """
    bundle_info = get_bundle_info(uuid)
    if bundle_info is None:
        abort(httplib.NOT_FOUND, 'The bundle is not available')
    if bundle_info['bundle_type'] != PrivateBundle.BUNDLE_TYPE:
        bundle_info.update(get_bundle_file_contents(uuid))
    return bundle_info


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

    info = rest_util.get_target_info((uuid, ''), 1)
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
