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
from itertools import chain
import json
import sys

import yaml
from bottle import get, post, local, request

from codalab.common import UsageError, NotFoundError
from codalab.lib import formatting, spec_util
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
    get_command,
)
from codalab.model.tables import GROUP_OBJECT_PERMISSION_ALL
from codalab.objects.permission import permission_str
from codalab.rest import util as rest_util
from codalab.rest.worksheets import get_worksheet_info, search_worksheets
from codalab.rest.worksheet_block_schemas import BlockModes, MarkupBlockSchema, FetchStatusCodes
from codalab.worker.download_util import BundleTarget


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
    return {'data': results}


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
    return {'contents': new_contents}


@get('/interpret/worksheet/<uuid:re:%s>' % spec_util.UUID_STR)
def fetch_interpreted_worksheet(uuid):
    """
    Return information about a worksheet. Calls
    - get_worksheet_info: get basic info
    - resolve_interpreted_items: get more information about a worksheet.
    In the future, for large worksheets, might want to break this up so
    that we can render something basic.
    Return: 
        worksheet_info dict{}:
            key:[value_type] <description>
            blocks:[list] 
                    Resolved worksheet blocks from raw_items.
                        Bundles will be grouped into table block items, 
                        text items might be grouped into one markdown block etc.
            source:[list] source lines
            raw_to_block:[list] 
                            Raw_items to its block index pair.
                                For example, assume the first resolved block item is a bundle table that has 2 rows,
                                then the 2nd element in the list would be [0, 1]
                                [0, 1]: 0 means the item belongs to the first block,
                                        1 means the item is the second item of the block (2nd bundle in our example)
                                NOTE: Used for setting focus on frontend
            block_to_raw:[dict] 
                            Maps the blocks (table, markdown, records) to their corresponding source line indices,
                            it's mostly a reverse mapping of raw_to_block, by mostly: raw_to_block has some bug,
                            please refer to worksheet_utils flush_bundles function.
                            This can be used to index the source on the frontend
                            Example:
                            [0, 0]: 0
                            [0, 1]: 1
                            [1, 0]: 9
                            This means the first blocks' first item corresponds to the first line in source,
                            the second item corresponds to the second line in source
                            The second block corresponds the 10th line in source. 
                            2-8 can be skipped for multiple reasons: blank lines, comments, schema lines etc.
                                NOTE: Used for setting focus on frontend                                      

    This endpoint can be called with &brief=1 in order to give an abbreviated version,
    which does not resolve searches or wsearches.

    To return an interpreted worksheet that only resolves a particular search/wsearch,
    pass in the search query to the "directive" argument. The value for this argument
    must be a search/wsearch query -- for example, &directive=search 0x .limit=100
    """
    bundle_uuids = request.query.getall('bundle_uuid')
    brief = request.query.get("brief", "0") == "1"

    directive = request.query.get("directive", None)
    print(directive)
    search_results = []

    worksheet_info = get_worksheet_info(uuid, fetch_items=True, fetch_permissions=True)

    # Shim in additional data for the frontend
    worksheet_info['items'] = resolve_items_into_infos(worksheet_info['items'])

    if worksheet_info['owner_id'] is None:
        worksheet_info['owner_name'] = None
    else:
        owner = local.model.get_user(user_id=worksheet_info['owner_id'])
        worksheet_info['owner_name'] = owner.user_name

    # Fetch items.
    worksheet_info['source'] = get_worksheet_lines(worksheet_info)

    if not directive and not brief:
        expanded_items = []
        for index, raw_item in enumerate(worksheet_info['items']):
            expanded = expand_search_item(raw_item)
            expanded_items.append(expanded)
            # Multiple items can correspond to the same source line (i.e: search directives)
            # raw_items_to_source_index.extend([index] * len(expanded))
        worksheet_info['items'] = list(chain.from_iterable(expanded_items))
    elif directive:
        # Only expand the search item corresponding to the given directive.
        # Used in async loading to only load a single table.
        item_idx = 0
        for i, item in enumerate(worksheet_info['items']):
            (bundle_info, subworksheet_info, value_obj, item_type, id, sort_key) = item
            if directive == formatting.tokens_to_string(value_obj):
                search_results = perform_search_query(value_obj)
                item_idx = i
                break
        # Make sure the search item is at the end of worksheet_info['items'],
        # so we can isolate it later after interpret_items is called.
        worksheet_info['items'] = worksheet_info['items'][:item_idx]
        worksheet_info['items'].extend(search_results)

    # Set permissions
    worksheet_info['edit_permission'] = worksheet_info['permission'] == GROUP_OBJECT_PERMISSION_ALL
    # Check enable chat box
    worksheet_info['enable_chat'] = local.config.get('enable_chat', False)
    # Format permissions into strings
    worksheet_info['permission_spec'] = permission_str(worksheet_info['permission'])
    for group_permission in worksheet_info['group_permissions']:
        group_permission['permission_spec'] = permission_str(group_permission['permission'])

    # Go and fetch more information about the worksheet contents by
    # resolving the interpreted items.
    try:
        interpreted_blocks = interpret_items(
            get_default_schemas(), worksheet_info['items'], db_model=local.model
        )
    except UsageError as e:
        interpreted_blocks = {'blocks': []}
        worksheet_info['error'] = str(e)

    # bundle_uuids is an optional argument that, if exists, contain the uuids of all the unfinished run bundles that need updating
    # In this case, full_worksheet will return a list of item parallel to ws.info.items that contain only items that need updating.
    # More specifically, all blocks that don't contain run bundles that need updating are None.
    # Also, a non-None block could contain a list of bundle_infos, which represent a list of bundles. Usually not all of them need updating.
    # The bundle_infos for bundles that don't need updating are also None.
    if bundle_uuids:
        for i, block in enumerate(interpreted_blocks['blocks']):
            if 'bundle_info' not in block:
                interpreted_blocks['blocks'][i] = None
            else:
                if isinstance(block['bundle_info'], dict):
                    block['bundle_info'] = [block['bundle_info']]
                is_relevant_block = False
                for j, bundle in enumerate(block['bundle_info']):
                    if bundle['uuid'] in bundle_uuids:
                        is_relevant_block = True
                    else:
                        block['bundle_info'][j] = None
                if not is_relevant_block:
                    interpreted_blocks['blocks'][i] = None
    # Grouped individual items into blocks
    worksheet_info['blocks'] = resolve_interpreted_blocks(interpreted_blocks['blocks'])
    worksheet_info['raw_to_block'] = interpreted_blocks['raw_to_block']
    worksheet_info['block_to_raw'] = interpreted_blocks['block_to_raw']

    if directive:
        # If we're only async loading a single table_block / subworksheets_block,
        # return only that block (which is at the end of worksheet_info['items'])
        worksheet_info['blocks'] = [worksheet_info['blocks'][-1]] if len(search_results) else []

    for block in worksheet_info['blocks']:
        if block is None:
            continue
        if block['mode'] == 'table':
            for row_map in block['rows']:
                for k, v in row_map.items():
                    if v is None:
                        row_map[k] = formatting.contents_str(v)
        if 'bundle_info' in block:
            infos = []
            if isinstance(block['bundle_info'], list):
                infos = block['bundle_info']
            elif isinstance(block['bundle_info'], dict):
                infos = [block['bundle_info']]
            for bundle_info in infos:
                if bundle_info is None:
                    continue
                if 'bundle_type' not in bundle_info:
                    continue  # empty info: invalid bundle reference
                if isinstance(bundle_info, dict):
                    format_metadata(bundle_info.get('metadata'))
    # Frontend doesn't use individual 'items' for now
    del worksheet_info['items']
    if bundle_uuids:
        return {'blocks': worksheet_info['blocks']}
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
    with closing(local.download_manager.stream_file(target, gzipped=False)) as fileobj:
        return fileobj.read()


# Maximum number of bytes to read per line requested
MAX_BYTES_PER_LINE = 1024


def head_target(target, max_num_lines):
    """
    Return the first max_num_lines of target as a list of strings.

    The caller should ensure that the target is a file.

    :param target: A worker.download_util.BundleTarget with bundle_uuid and subpath
    :param max_num_lines: max number of lines to fetch
    """
    rest_util.check_target_has_read_permission(target)
    # Note: summarize_file returns bytes, but should be decodable to a string.
    lines = (
        local.download_manager.summarize_file(
            target, max_num_lines, 0, MAX_BYTES_PER_LINE, None, gzipped=False
        )
        .decode()
        .splitlines(True)
    )

    return lines


# Default number of lines to pull for each display mode.
DEFAULT_GRAPH_MAX_LINES = 100


def resolve_interpreted_blocks(interpreted_blocks):
    """
    Called by the web interface.  Takes a list of interpreted worksheet
    items (returned by worksheet_util.interpret_items) and fetches the
    appropriate information, replacing the 'interpreted' field in each item.
    The result can be serialized via JSON.
    """

    def set_error_data(block_index, message):
        interpreted_blocks[block_index] = (
            MarkupBlockSchema().load({'id': block_index, 'text': 'ERROR: ' + message}).data
        )

    for block_index, block in enumerate(interpreted_blocks):
        if block is None:
            continue
        mode = block['mode']

        try:
            # Replace data with a resolved version.
            if mode in (BlockModes.markup_block, BlockModes.placeholder_block):
                # no need to do anything
                pass
            elif mode == BlockModes.record_block or mode == BlockModes.table_block:
                # header_name_posts is a list of (name, post-processing) pairs.
                contents = block['rows']
                # Request information
                contents = interpret_genpath_table_contents(contents)

                block['rows'] = contents
            elif mode == BlockModes.contents_block or mode == BlockModes.image_block:
                bundle_uuid = block['bundles_spec']['bundle_infos'][0]['uuid']
                target_path = block['target_genpath']
                target = BundleTarget(bundle_uuid, target_path)
                try:
                    target_info = rest_util.get_target_info(target, 0)
                    if target_info['type'] == 'directory' and mode == BlockModes.contents_block:
                        block['status']['code'] = FetchStatusCodes.ready
                        block['lines'] = ['<directory>']
                    elif target_info['type'] == 'file':
                        block['status']['code'] = FetchStatusCodes.ready
                        if mode == BlockModes.contents_block:
                            block['lines'] = head_target(
                                target_info['resolved_target'], block['max_lines']
                            )
                        elif mode == BlockModes.image_block:
                            block['status']['code'] = FetchStatusCodes.ready
                            block['image_data'] = base64.b64encode(
                                bytes(cat_target(target_info['resolved_target']))
                            ).decode('utf-8')
                    else:
                        block['status']['code'] = FetchStatusCodes.not_found
                        if mode == BlockModes.contents_block:
                            block['lines'] = None
                        elif mode == BlockModes.image_block:
                            block['image_data'] = None
                except NotFoundError as e:
                    block['status']['code'] = FetchStatusCodes.not_found
                    if mode == BlockModes.contents_block:
                        block['lines'] = None
                    elif mode == BlockModes.image_block:
                        block['image_data'] = None

            elif mode == BlockModes.graph_block:
                # data = list of {'target': ...}
                # Add a 'points' field that contains the contents of the target.
                for info in block['trajectories']:
                    target = BundleTarget(info['bundle_uuid'], info['target_genpath'])
                    try:
                        target_info = rest_util.get_target_info(target, 0)
                    except NotFoundError as e:
                        continue
                    if target_info['type'] == 'file':
                        contents = head_target(target_info['resolved_target'], block['max_lines'])
                        # Assume TSV file without header for now, just return each line as a row
                        info['points'] = points = []
                        for line in contents:
                            row = line.split('\t')
                            points.append(row)
            elif mode == BlockModes.subworksheets_block:
                # do nothing
                pass
            else:
                raise UsageError('Invalid display mode: %s' % mode)

        except UsageError as e:
            set_error_data(block_index, str(e))

        except Exception:
            import traceback

            traceback.print_exc()
            set_error_data(block_index, "Unexpected error interpreting item")

        block['is_refined'] = True

    return interpreted_blocks


def is_bundle_genpath_triple(value):
    # if called after an RPC call tuples may become lists
    need_gen_types = (tuple, list)

    return isinstance(value, need_gen_types) and len(value) == 3


def interpret_genpath_table_contents(contents):
    """
    contents represents a table, but some of the elements might not be
    interpreted yet, so fill them in.
    """

    # Request information
    requests = []
    for r, row in enumerate(contents):
        for key, value in row.items():
            # value can be either a string (already rendered) or a (bundle_uuid, genpath, post) triple
            if is_bundle_genpath_triple(value):
                requests.append(value)
    responses = interpret_file_genpaths(requests)

    # Put it in a table
    new_contents = []
    ri = 0
    for r, row in enumerate(contents):
        new_row = {}
        for key, value in row.items():
            if is_bundle_genpath_triple(value):
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
    MAX_LINES = 10000  # Maximum number of lines we need to read from a file.

    # Load the file
    if not is_file_genpath(genpath):
        raise UsageError('Not file genpath: %s' % genpath)
    genpath = genpath[1:]
    if ':' in genpath:  # Looking for a particular key in the file
        subpath, key = genpath.split(':')
    else:
        subpath, key = genpath, None

    target = BundleTarget(bundle_uuid, subpath)
    if target not in target_cache:
        info = None
        try:
            target_info = rest_util.get_target_info(target, 0)
            if target_info['type'] == 'file':
                contents = head_target(target_info['resolved_target'], MAX_LINES)

                if len(contents) == 0:
                    info = ''
                elif all('\t' in x for x in contents):
                    # Tab-separated file (key\tvalue\nkey\tvalue...)
                    info = {}
                    for x in contents:
                        kv = x.strip().split("\t", 1)
                        if len(kv) == 2:
                            info[kv[0]] = kv[1]
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
        except NotFoundError:
            pass

        # Try to interpret the structure of the file by looking inside it.
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
            if info is None:
                break
    return apply_func(post, info)


def resolve_items_into_infos(items):
    """
    Helper function.
    {'bundle_uuid': '...', 'subworksheet_uuid': '...', 'value': '...', 'type': '...')
        -> (bundle_info, subworksheet_info, value_obj, type, id, sort_key)
    """
    # Database only contains the uuid; need to expand to info.
    # We need to do to convert the bundle_uuids into bundle_info dicts.
    # However, we still make O(1) database calls because we use the
    # optimized batch_get_bundles multiget method.
    bundle_uuids = set(i['bundle_uuid'] for i in items if i['bundle_uuid'] is not None)

    bundle_dict = rest_util.get_bundle_infos(bundle_uuids, get_single_host_worksheet=True)

    # Go through the items and substitute the components
    new_items = []
    for i in items:
        bundle_info = (
            bundle_dict.get(i['bundle_uuid'], {'uuid': i['bundle_uuid']})
            if i['bundle_uuid']
            else None
        )
        if bundle_info is not None:
            bundle_info['sort_key'] = i['sort_key']
            bundle_info['id'] = i['id']
        if i['subworksheet_uuid']:
            try:
                subworksheet_info = local.model.get_worksheet(
                    i['subworksheet_uuid'], fetch_items=False
                ).to_dict()
            except UsageError as e:
                # If can't get the subworksheet, it's probably invalid, so just replace it with an error
                # type = worksheet_util.TYPE_MARKUP
                subworksheet_info = {'uuid': i['subworksheet_uuid']}
                # value = 'ERROR: non-existent worksheet %s' % subworksheet_uuid
        else:
            subworksheet_info = None
        value_obj = (
            formatting.string_to_tokens(i['value']) if i['type'] == TYPE_DIRECTIVE else i['value']
        )
        new_items.append(
            (bundle_info, subworksheet_info, value_obj, i['type'], i['id'], i['sort_key'])
        )
    return new_items


def perform_search_query(value_obj):
    """
    Perform a search query and return the resulting raw items.
    Input: directive that is tokenized by formatting.string_to_tokens(),
        such as formatting.string_to_tokens("search 0x .limit=100")
    """
    command = get_command(value_obj)
    is_search = command == 'search'
    is_wsearch = command == 'wsearch'
    if is_search or is_wsearch:
        keywords = value_obj[1:]
        raw_items = []

        if is_search:
            keywords = rest_util.resolve_owner_in_keywords(keywords)
            search_result = local.model.search_bundles(request.user.user_id, keywords)
            if search_result['is_aggregate']:
                # Add None's for the 'id' and 'sort_key' of the tuple, since these
                # items are not really worksheet items.
                raw_items.append(markup_item(str(search_result['result'])) + (None, None))
            else:
                bundle_uuids = search_result['result']
                bundle_infos = rest_util.get_bundle_infos(bundle_uuids)
                for bundle_uuid in bundle_uuids:
                    # Since bundle UUID's are queried first, we can't assume a UUID exists in
                    # the subsequent bundle info query.
                    if bundle_uuid in bundle_infos:
                        raw_items.append(bundle_item(bundle_infos[bundle_uuid]) + (None, None))
        elif is_wsearch:
            worksheet_infos = search_worksheets(keywords)
            for worksheet_info in worksheet_infos:
                raw_items.append(subworksheet_item(worksheet_info) + (None, None))

        return raw_items
    else:
        # Not a search query
        return []


def expand_search_item(raw_item):
    """
    Raw items that include searches must be expanded into more raw items.
    Input: Raw item.
    Output: Array of raw items. If raw item does not need expanding,
    this returns an 1-length array that contains original raw item,
    otherwise it contains the search result. You do not need to call
    resolve_items_into_infos on the returned raw_items.
    """
    (bundle_info, subworksheet_info, value_obj, item_type, id, sort_key) = raw_item
    if item_type == TYPE_DIRECTIVE and get_command(value_obj) in ('search', 'wsearch'):
        return perform_search_query(value_obj)
    else:
        return [raw_item]
