"""
Worksheet interpretation API.

While not deprecated, this API is subject to further redesign and refactoring
due to its relative complexity.

The distinction between the placement of functions in this module versus
the worksheet_util module can appear blurry. The distinction is that
worksheet_util does not make any calls to the model, they are kind of just like
static helper functions.
"""
import base64
import types
from contextlib import closing
import json

import yaml
from bottle import (
    get,
    post,
    local,
    request,
)

from codalab.common import UsageError
from codalab.lib import (
    formatting,
    spec_util,
)
from codalab.lib.worksheet_util import (
    TYPE_DIRECTIVE,
    format_metadata,
    get_default_schemas,
    get_worksheet_lines,
    apply_func,
    interpret_items,
    is_file_genpath,
    markup_item,
    directive_item,
    bundle_item,
    subworksheet_item,
)
from codalab.model.tables import GROUP_OBJECT_PERMISSION_ALL
from codalab.objects.permission import permission_str
from codalab.rest import util as rest_util
from codalab.rest.worksheets import (
    get_worksheet_info,
    search_worksheets,
)


@post('/interpret/search')
def _interpret_search():
    """
    Returns worksheet items given a search query for bundles.

    JSON request body:
    ```
    {
        "keywords": [ list of search keywords ],
        "schemas": {
            schema_key: [ list of schema columns ],
            ...
        },
        "display": [ display args ]
    }
    ```
    """
    return interpret_search(request.json)


@post('/interpret/wsearch')
def _interpret_wsearch():
    """
    Returns worksheet items given a search query for worksheets.

    JSON request body:
    ```
    {
        "keywords": [ list of search keywords ]
    }
    ```
    """
    return interpret_wsearch(request.json)


@post('/interpret/file-genpaths')
def _interpret_file_genpaths():
    """
    Interpret a file genpath.

    JSON request body:
    ```
    {
        "queries": [
            {
                "bundle_uuid": "<uuid>",
                "genpath": "<genpath>",
                "post": "<postprocessor spec>",
            },
            ...
        ]
    }
    ```

    Response body:
    ```
    {
        "data": [
            "<resolved file genpath data>",
            ...
        ]
    }
    ```
    """
    queries = [(q['bundle_uuid'], q['genpath'], q['post']) for q in request.json['queries']]
    results = interpret_file_genpaths(queries)
    return {
        'data': results
    }


@post('/interpret/genpath-table-contents')
def _interpret_genpath_table_contents():
    """
    Takes a table and fills in unresolved genpath specifications.

    JSON request body:
    ```
    {
        "contents": [
            {
                col1: "<resolved string>",
                col2: (bundle_uuid, genpath, post),
                ...
            },
            ...
        ]
    }
    ```

    JSON response body:
    ```
    {
        "contents": [
            {
                col1: "<resolved string>",
                col2: "<resolved string>",
                ...
            },
            ...
        ]
    }
    ```
    """
    contents = request.json['contents']
    new_contents = interpret_genpath_table_contents(contents)
    return {
        'contents': new_contents
    }


@get('/interpret/worksheet/<uuid:re:%s>' % spec_util.UUID_STR)
def fetch_interpreted_worksheet(uuid):
    """
    Return information about a worksheet. Calls
    - get_worksheet_info: get basic info
    - resolve_interpreted_items: get more information about a worksheet.
    In the future, for large worksheets, might want to break this up so
    that we can render something basic.
    """
    bundle_uuids = request.query.getall('bundle_uuid')
    worksheet_info = get_worksheet_info(uuid, fetch_items=True, fetch_permissions=True)

    # Shim in additional data for the frontend
    worksheet_info['items'] = resolve_items_into_infos(worksheet_info['items'])
    if worksheet_info['owner_id'] is None:
        worksheet_info['owner_name'] = None
    else:
        owner = local.model.get_user(user_id=worksheet_info['owner_id'])
        worksheet_info['owner_name'] = owner.user_name

    # Fetch items.
    worksheet_info['raw'] = get_worksheet_lines(worksheet_info)

    # Set permissions
    worksheet_info['edit_permission'] = (worksheet_info['permission'] == GROUP_OBJECT_PERMISSION_ALL)
    # Check enable chat box
    worksheet_info['enable_chat'] = local.config.get('enable_chat', False)
    # Format permissions into strings
    worksheet_info['permission_spec'] = permission_str(worksheet_info['permission'])
    for group_permission in worksheet_info['group_permissions']:
        group_permission['permission_spec'] = permission_str(group_permission['permission'])

    # Go and fetch more information about the worksheet contents by
    # resolving the interpreted items.
    try:
        interpreted_items = interpret_items(
            get_default_schemas(),
            worksheet_info['items'])
    except UsageError, e:
        interpreted_items = {'items': []}
        worksheet_info['error'] = str(e)

    # bundle_uuids is an optional argument that, if exists, contain the uuids of all the unfinished run bundles that need updating
    # In this case, full_worksheet will return a list of item parallel to ws.info.items that contain only items that need updating.
    # More specifically, all items that don't contain run bundles that need updating are None.
    # Also, a non-None item could contain a list of bundle_infos, which represent a list of bundles. Usually not all of them need updating.
    # The bundle_infos for bundles that don't need updating are also None.
    if bundle_uuids:
        for i, item in enumerate(interpreted_items['items']):
            if 'bundle_info' not in item:
                interpreted_items['items'][i] = None
            else:
                if isinstance(item['bundle_info'], dict):
                    item['bundle_info'] = [item['bundle_info']]
                is_relevant_item = False
                for j, bundle in enumerate(item['bundle_info']):
                    if bundle['uuid'] in bundle_uuids:
                        is_relevant_item = True
                    else:
                        item['bundle_info'][j] = None
                if not is_relevant_item:
                    interpreted_items['items'][i] = None

    worksheet_info['items'] = resolve_interpreted_items(interpreted_items['items'])
    worksheet_info['raw_to_interpreted'] = interpreted_items['raw_to_interpreted']
    worksheet_info['interpreted_to_raw'] = interpreted_items['interpreted_to_raw']

    for item in worksheet_info['items']:
        if item is None:
            continue
        if item['mode'] == 'table':
            for row_map in item['interpreted'][1]:
                for k, v in row_map.iteritems():
                    if v is None:
                        row_map[k] = formatting.contents_str(v)
        if 'bundle_info' in item:
            infos = []
            if isinstance(item['bundle_info'], list):
                infos = item['bundle_info']
            elif isinstance(item['bundle_info'], dict):
                infos = [item['bundle_info']]
            for bundle_info in infos:
                if bundle_info is None:
                    continue
                if 'bundle_type' not in bundle_info:
                    continue  # empty info: invalid bundle reference
                if isinstance(bundle_info, dict):
                    format_metadata(bundle_info.get('metadata'))
    if bundle_uuids:
        return {'items': worksheet_info['items']}
    return worksheet_info


#############################################################
#  INTERPRETATION HELPER FUNCTIONS
#############################################################


def cat_target(target):
    """
    Prints the contents of the target file into the file-like object out.
    The caller should ensure that the target is a file.
    """
    rest_util.check_target_has_read_permission(target)
    with closing(local.download_manager.stream_file(target[0], target[1], gzipped=False)) as fileobj:
        return fileobj.read()


# Maximum number of bytes to read per line requested
MAX_BYTES_PER_LINE = 1024


def head_target(target, max_num_lines, replace_non_unicode=False):
    """
    Return the first max_num_lines of target as a list of strings.

    The caller should ensure that the target is a file.

    :param target: (bundle_uuid, subpath)
    :param max_num_lines: max number of lines to fetch
    :param replace_non_unicode: replace non-unicode characters with something printable
    """
    rest_util.check_target_has_read_permission(target)
    lines = local.download_manager.summarize_file(
        target[0], target[1],
        max_num_lines, 0, MAX_BYTES_PER_LINE, None,
        gzipped=False).splitlines(True)

    if replace_non_unicode:
        lines = map(formatting.verbose_contents_str, lines)

    return lines


# Default number of lines to pull for each display mode.
DEFAULT_CONTENTS_MAX_LINES = 10
DEFAULT_GRAPH_MAX_LINES = 100


def resolve_interpreted_items(interpreted_items):
    """
    Called by the web interface.  Takes a list of interpreted worksheet
    items (returned by worksheet_util.interpret_items) and fetches the
    appropriate information, replacing the 'interpreted' field in each item.
    The result can be serialized via JSON.
    """
    def error_data(mode, message):
        if mode == 'record' or mode == 'table':
            return (('ERROR',), [{'ERROR': message}])
        else:
            return [message]

    for item in interpreted_items:
        if item is None:
            continue
        mode = item['mode']
        data = item['interpreted']
        properties = item['properties']

        try:
            # Replace data with a resolved version.
            if mode == 'markup':
                # no need to do anything
                pass
            elif mode == 'record' or mode == 'table':
                # header_name_posts is a list of (name, post-processing) pairs.
                header, contents = data
                # Request information
                contents = interpret_genpath_table_contents(contents)
                data = (header, contents)
            elif mode == 'contents':
                try:
                    max_lines = int(properties.get('maxlines', DEFAULT_CONTENTS_MAX_LINES))
                except ValueError:
                    raise UsageError("maxlines must be integer")

                target_info = rest_util.get_target_info(data, 0)
                if target_info is not None and target_info['type'] == 'directory':
                    data = ['<directory>']
                elif target_info is not None and target_info['type'] == 'file':
                    data = head_target(data, max_lines, replace_non_unicode=True)
                else:
                    data = None
            elif mode == 'html':
                target_info = rest_util.get_target_info(data, 0)
                if target_info is not None and target_info['type'] == 'file':
                    data = head_target(data, None)
                else:
                    data = None
            elif mode == 'image':
                target_info = rest_util.get_target_info(data, 0)
                if target_info is not None and target_info['type'] == 'file':
                    data = base64.b64encode(cat_target(data))
                else:
                    data = None
            elif mode == 'graph':
                try:
                    max_lines = int(properties.get('maxlines', DEFAULT_CONTENTS_MAX_LINES))
                except ValueError:
                    raise UsageError("maxlines must be integer")

                # data = list of {'target': ...}
                # Add a 'points' field that contains the contents of the target.
                for info in data:
                    target = info['target']
                    target_info = rest_util.get_target_info(target, 0)
                    if target_info is not None and target_info['type'] == 'file':
                        contents = head_target(target, max_lines, replace_non_unicode=True)
                        # Assume TSV file without header for now, just return each line as a row
                        info['points'] = points = []
                        for line in contents:
                            row = line.split('\t')
                            points.append(row)
            elif mode == 'search':
                data = interpret_search(data)
            elif mode == 'wsearch':
                data = interpret_wsearch(data)
            elif mode == 'worksheet':
                pass
            else:
                raise UsageError('Invalid display mode: %s' % mode)

        except UsageError as e:
            data = error_data(mode, e.message)

        except StandardError:
            import traceback
            traceback.print_exc()
            data = error_data(mode, "Unexpected error interpreting item")

        # Assign the interpreted from the processed data
        item['interpreted'] = data

    return interpreted_items


def interpret_search(query):
    """
    Input: specification of a bundle search query.
    Output: worksheet items based on the result of issuing the search query.
    """
    # Perform search
    keywords = rest_util.resolve_owner_in_keywords(query['keywords'])
    bundle_uuids = local.model.search_bundle_uuids(request.user.user_id, keywords)

    # Single number, just print it out...
    if not isinstance(bundle_uuids, list):
        return interpret_items(query['schemas'], [markup_item(str(bundle_uuids))])

    # Set display
    items = [directive_item(('display',) + tuple(query['display']))]

    # Show bundles
    bundle_infos = rest_util.get_bundle_infos(bundle_uuids)
    for bundle_uuid in bundle_uuids:
        items.append(bundle_item(bundle_infos[bundle_uuid]))

    # Finally, interpret the items
    return interpret_items(query['schemas'], items)


def interpret_wsearch(query):
    """
    Input: specification of a worksheet search query.
    Output: worksheet items based on the result of issuing the search query.
    """
    # Get the worksheet uuids
    worksheet_infos = search_worksheets(query['keywords'])
    items = [subworksheet_item(worksheet_info) for worksheet_info in worksheet_infos]

    # Finally, interpret the items
    return interpret_items([], items)


def interpret_genpath_table_contents(contents):
    """
    contents represents a table, but some of the elements might not be
    interpreted yet, so fill them in.
    """
    # if called after an RPC call tuples may become lists
    need_gen_types = (types.TupleType, types.ListType)

    # Request information
    requests = []
    for r, row in enumerate(contents):
        for key, value in row.items():
            # value can be either a string (already rendered) or a (bundle_uuid, genpath, post) triple
            if isinstance(value, need_gen_types):
                requests.append(value)
    responses = interpret_file_genpaths(requests)

    # Put it in a table
    new_contents = []
    ri = 0
    for r, row in enumerate(contents):
        new_row = {}
        for key, value in row.items():
            if isinstance(value, need_gen_types):
                value = responses[ri]
                ri += 1
            new_row[key] = value
        new_contents.append(new_row)
    return new_contents


def interpret_file_genpaths(requests):
    """
    Helper function.
    requests: list of (bundle_uuid, genpath, post-processing-func)
    Return responses: corresponding list of strings
    """
    target_cache = {}
    responses = []
    for (bundle_uuid, genpath, post) in requests:
        value = interpret_file_genpath(target_cache, bundle_uuid, genpath, post)
        responses.append(value)
    return responses


def interpret_file_genpath(target_cache, bundle_uuid, genpath, post):
    """
    |cache| is a mapping from target (bundle_uuid, subpath) to the info map,
    which is to be read/written to avoid reading/parsing the same file many
    times.
    |genpath| specifies the subpath and various fields (e.g., for
    /stats:train/errorRate, subpath = 'stats', key = 'train/errorRate').
    |post| function to apply to the resulting value.
    Return the string value.
    """
    MAX_LINES = 1000  # Maximum number of lines we need to read from a file.

    # Load the file
    if not is_file_genpath(genpath):
        raise UsageError('Not file genpath: %s' % genpath)
    genpath = genpath[1:]
    if ':' in genpath:  # Looking for a particular key in the file
        subpath, key = genpath.split(':')
    else:
        subpath, key = genpath, None

    target = (bundle_uuid, subpath)
    if target not in target_cache:
        target_info = rest_util.get_target_info(target, 0)

        # Try to interpret the structure of the file by looking inside it.
        if target_info is not None and target_info['type'] == 'file':
            contents = head_target(target, MAX_LINES)

            if len(contents) == 0:
                info = ''
            elif all('\t' in x for x in contents):
                # Tab-separated file (key\tvalue\nkey\tvalue...)
                info = {}
                for x in contents:
                    kv = x.strip().split("\t", 1)
                    if len(kv) == 2: info[kv[0]] = kv[1]
            else:
                try:
                    # JSON file
                    info = json.loads(''.join(contents))
                except (TypeError, ValueError):
                    try:
                        # YAML file
                        # Use safe_load because yaml.load() could execute
                        # arbitrary Python code
                        info = yaml.safe_load(''.join(contents))
                    except yaml.YAMLError:
                        # Plain text file
                        info = ''.join(contents)
        else:
            info = None
        target_cache[target] = info

    # Traverse the info object.
    info = target_cache.get(target, None)
    if key is not None and info is not None:
        for k in key.split('/'):
            if isinstance(info, dict):
                info = info.get(k, None)
            elif isinstance(info, list):
                try:
                    info = info[int(k)]
                except (KeyError, ValueError):
                    info = None
            else:
                info = None
            if info is None: break
    return apply_func(post, info)


def resolve_items_into_infos(items):
    """
    Helper function.
    {'bundle_uuid': '...', 'subworksheet_uuid': '...', 'value': '...', 'type': '...')
        -> (bundle_info, subworksheet_info, value_obj, type)
    """
    # Database only contains the uuid; need to expand to info.
    # We need to do to convert the bundle_uuids into bundle_info dicts.
    # However, we still make O(1) database calls because we use the
    # optimized batch_get_bundles multiget method.
    bundle_uuids = set(
        i['bundle_uuid'] for i in items
        if i['bundle_uuid'] is not None
    )

    bundle_dict = rest_util.get_bundle_infos(bundle_uuids)

    # Go through the items and substitute the components
    new_items = []
    for i in items:
        bundle_info = bundle_dict.get(i['bundle_uuid'], {'uuid': i['bundle_uuid']}) if i['bundle_uuid'] else None
        if i['subworksheet_uuid']:
            try:
                subworksheet_info = local.model.get_worksheet(i['subworksheet_uuid'], fetch_items=False).to_dict()
            except UsageError, e:
                # If can't get the subworksheet, it's probably invalid, so just replace it with an error
                # type = worksheet_util.TYPE_MARKUP
                subworksheet_info = {'uuid': i['subworksheet_uuid']}
                # value = 'ERROR: non-existent worksheet %s' % subworksheet_uuid
        else:
            subworksheet_info = None
        value_obj = formatting.string_to_tokens(i['value']) if i['type'] == TYPE_DIRECTIVE else i['value']
        new_items.append((bundle_info, subworksheet_info, value_obj, i['type']))
    return new_items
