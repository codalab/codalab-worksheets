"""
Legacy REST APIs moved from the codalab-worksheets Django REST server.
"""
import base64
import shutil
from cStringIO import StringIO
from contextlib import closing
from datetime import datetime, timedelta
import json
from oauthlib.common import generate_token
import os
import random
import shlex
import threading
import yaml

from bottle import (
  abort,
  get,
  httplib,
  HTTPResponse,
  local,
  post,
  redirect,
  request,
  response,
)

from codalab.bundles import get_bundle_subclass, PrivateBundle
from codalab.client.json_api_client import JsonApiClient
from codalab.common import UsageError, precondition
from codalab.lib import (
  bundle_cli,
  formatting,
  spec_util,
  worksheet_util,
)
from codalab.lib.codalab_manager import CodaLabManager
from codalab.lib.worksheet_util import ServerWorksheetResolver
from codalab.model.tables import GROUP_OBJECT_PERMISSION_ALL
from codalab.objects.chat_box_qa import ChatBoxQA
from codalab.objects.oauth2 import OAuth2Token
from codalab.objects.permission import permission_str, \
    check_bundles_have_all_permission, check_bundles_have_read_permission
from codalab.rest.util import get_bundle_info, get_bundle_infos, resolve_owner_in_keywords
from codalab.rest.worksheets import (
    update_worksheet_items,
    get_worksheet_info,
    get_worksheet_uuid,
)
from codalab.server.authenticated_plugin import AuthenticatedPlugin


def get_user_token():
    """
    Returns an access token for the authenticated user.
    This function facilitates interactions with the bundle service.
    """
    CLIENT_ID = 'codalab_cli_client'

    if request.user is None:
        return None

    # Try to find an existing token that will work.
    token = local.model.find_oauth2_token(
        CLIENT_ID,
        request.user.user_id,
        datetime.utcnow() + timedelta(minutes=5))
    if token is not None:
        return token.access_token

    # Otherwise, generate a new one.
    token = OAuth2Token(
        local.model,
        access_token=generate_token(),
        refresh_token=None,
        scopes='',
        expires=datetime.utcnow() + timedelta(hours=10),
        client_id=CLIENT_ID,
        user_id=request.user.user_id,
    )
    local.model.save_oauth2_token(token)

    return token.access_token


class BundleService(object):
    """
    Methods for legacy frontend API.
    """

    @staticmethod
    def _rest_url():
        return 'http://{rest_host}:{rest_port}'.format(**local.config['server'])

    @staticmethod
    def get_bundle_info(uuid):
        bundle_info = get_bundle_info(uuid, True, True, True)

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

    @staticmethod
    def get_bundle_infos(uuids, get_children=False, get_host_worksheets=False, get_permissions=False):
        """
        get_children, get_host_worksheets, get_permissions: whether we want to return more detailed information.
        Return map from bundle uuid to info.
        """
        return get_bundle_infos(
            uuids,
            get_children=get_children,
            get_host_worksheets=get_host_worksheets,
            get_permissions=get_permissions)

    # Used by RPC
    def interpret_genpath_table_contents(self, contents):
        return worksheet_util.interpret_genpath_table_contents(self, contents)

    # Used by RPC
    def interpret_search(self, worksheet_uuid, data):
        return worksheet_util.interpret_search(self, worksheet_uuid, data)

    # Used by RPC
    def interpret_wsearch(self, data):
        return worksheet_util.interpret_wsearch(self, data)

    @staticmethod
    def search_worksheets(keywords, worksheet_uuid=None):
        keywords = resolve_owner_in_keywords(keywords)
        results = local.model.search_worksheets(request.user.user_id, keywords)
        BundleService._set_owner_names(results)
        return results

    @staticmethod
    def _set_owner_names(results):
        """
        Helper function: Set owner_name given owner_id of each item in results.
        """
        owners = [local.model.get_user(r['owner_id']) for r in results]
        for r, o in zip(results, owners):
            r['owner_name'] = o.user_name

    def full_worksheet(self, uuid, bundle_uuids=None):
        """
        Return information about a worksheet. Calls
        - get_worksheet_info: get basic info
        - resolve_interpreted_items: get more information about a worksheet.
        In the future, for large worksheets, might want to break this up so
        that we can render something basic.
        """
        worksheet_info = get_worksheet_info(uuid, fetch_items=True, fetch_permission=True, legacy=True)

        # Fetch items.
        worksheet_info['raw'] = worksheet_util.get_worksheet_lines(worksheet_info)

        # Set permissions
        worksheet_info['edit_permission'] = (worksheet_info['permission'] == GROUP_OBJECT_PERMISSION_ALL)
        # Check enable chat box
        worksheet_info['enable_chat'] = local.config.get('enable_chat', False)
        # Format permissions into strings
        worksheet_info['permission_str'] = permission_str(worksheet_info['permission'])
        for group_permission in worksheet_info['group_permissions']:
            group_permission['permission_str'] = permission_str(group_permission['permission'])

        # Go and fetch more information about the worksheet contents by
        # resolving the interpreted items.
        try:
            interpreted_items = worksheet_util.interpret_items(
                                worksheet_util.get_default_schemas(),
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

        worksheet_info['items'] = self.resolve_interpreted_items(interpreted_items['items'])
        worksheet_info['raw_to_interpreted'] = interpreted_items['raw_to_interpreted']
        worksheet_info['interpreted_to_raw'] = interpreted_items['interpreted_to_raw']

        def decode_lines(interpreted):
            # interpreted is None or list of base64 encoded lines
            if interpreted is None:
                return formatting.contents_str(None)
            else:
                return map(base64.b64decode, interpreted)

        # Currently, only certain fields are base64 encoded.
        for item in worksheet_info['items']:
            if item is None:
                continue
            if item['mode'] in ['html', 'contents']:
                item['interpreted'] = decode_lines(item['interpreted'])
            elif item['mode'] == 'table':
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
                    if bundle_info['bundle_type'] != PrivateBundle.BUNDLE_TYPE:
                        target_info = self.get_top_level_contents((bundle_info['uuid'], ''))
                        bundle_info['target_info'] = target_info
                    if isinstance(bundle_info, dict):
                        worksheet_util.format_metadata(bundle_info.get('metadata'))
        if bundle_uuids:
            return {'items': worksheet_info['items']}
        return worksheet_info

    @staticmethod
    def parse_and_update_worksheet(uuid, lines):
        """
        Replace worksheet |uuid| with the raw contents given by |lines|.
        """
        worksheet_info = get_worksheet_info(uuid, fetch_items=True, legacy=True)
        new_items, commands = worksheet_util.parse_worksheet_form(
            lines, ServerWorksheetResolver(local.model, request.user), worksheet_info['uuid'])
        update_worksheet_items(worksheet_info, new_items)
        # Note: commands are ignored

    def get_bundle_file_contents(self, uuid):
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

        info = self.get_top_level_contents((uuid, ''))
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

    def get_top_level_contents(self, target):
        info = self.get_target_info(target, 1)
        if info is not None and info['type'] == 'directory':
            for item in info['contents']:
                item['size_str'] = formatting.size_str(item['size'])
        return info

    @staticmethod
    def search_bundle_uuids(worksheet_uuid, keywords):
        keywords = resolve_owner_in_keywords(keywords)
        return local.model.search_bundle_uuids(request.user.user_id, worksheet_uuid, keywords)

    @staticmethod
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

    @staticmethod
    def check_target_has_read_permission(target):
        check_bundles_have_read_permission(local.model, request.user, [target[0]])

    def get_target_info(self, target, depth):
        """
        Returns information about an individual target inside the bundle, or
        None if the target doesn't exist.
        """
        self.check_target_has_read_permission(target)
        return local.download_manager.get_target_info(target[0], target[1], depth)

    def cat_target(self, target, out):
        """
        Prints the contents of the target file into the file-like object out.
        The caller should ensure that the target is a file.
        """
        self._do_download_file(target, out_fileobj=out)

    def _do_download_file(self, target, out_path=None, out_fileobj=None):
        self.check_target_has_read_permission(target)
        with closing(local.download_manager.stream_file(target[0], target[1], gzipped=False)) as fileobj:
            if out_path is not None:
                with open(out_path, 'wb') as out:
                    shutil.copyfileobj(fileobj, out)
            elif out_fileobj is not None:
                shutil.copyfileobj(fileobj, out_fileobj)

    # Maximum number of bytes to read per line requested
    MAX_BYTES_PER_LINE = 128

    def head_target(self, target, max_num_lines, replace_non_unicode=False, base64_encode=True):
        """
        Return base64 encoded version of the result.

        The caller should ensure that the target is a file.
        """
        self.check_target_has_read_permission(target)
        lines = local.download_manager.summarize_file(
            target[0], target[1],
            max_num_lines, 0, self.MAX_BYTES_PER_LINE, None,
            gzipped=False).splitlines(True)

        if replace_non_unicode:
            lines = map(formatting.verbose_contents_str, lines)

        if base64_encode:
            lines = map(base64.b64encode, lines)

        return lines

    # Default number of lines to pull for each display mode.
    DEFAULT_CONTENTS_MAX_LINES = 10
    DEFAULT_GRAPH_MAX_LINES = 100

    def resolve_interpreted_items(self, interpreted_items):
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
            if item == None:
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
                    contents = worksheet_util.interpret_genpath_table_contents(self, contents)
                    data = (header, contents)
                elif mode == 'contents':
                    try:
                        max_lines = int(properties.get('maxlines', self.DEFAULT_CONTENTS_MAX_LINES))
                    except ValueError:
                        raise UsageError("maxlines must be integer")

                    target_info = self.get_target_info(data, 0)
                    if target_info is not None and target_info['type'] == 'directory':
                        data = [base64.b64encode('<directory>')]
                    elif target_info is not None and target_info['type'] == 'file':
                        data = self.head_target(data, max_lines, replace_non_unicode=True)
                    else:
                        data = None
                elif mode == 'html':
                    target_info = self.get_target_info(data, 0)
                    if target_info is not None and target_info['type'] == 'file':
                        data = self.head_target(data, None)
                    else:
                        data = None
                elif mode == 'image':
                    target_info = self.get_target_info(data, 0)
                    if target_info is not None and target_info['type'] == 'file':
                        result = StringIO()
                        self.cat_target(data, result)
                        data = base64.b64encode(result.getvalue())
                    else:
                        data = None
                elif mode == 'graph':
                    try:
                        max_lines = int(properties.get('maxlines', self.DEFAULT_CONTENTS_MAX_LINES))
                    except ValueError:
                        raise UsageError("maxlines must be integer")

                    # data = list of {'target': ...}
                    # Add a 'points' field that contains the contents of the target.
                    for info in data:
                        target = info['target']
                        target_info = self.get_target_info(target, 0)
                        if target_info is not None and target_info['type'] == 'file':
                            contents = self.head_target(target, max_lines, replace_non_unicode=True, base64_encode=False)
                            # Assume TSV file without header for now, just return each line as a row
                            info['points'] = points = []
                            for line in contents:
                                row = line.split('\t')
                                points.append(row)
                elif mode == 'search':
                    data = worksheet_util.interpret_search(self, None, data)
                elif mode == 'wsearch':
                    data = worksheet_util.interpret_wsearch(self, data)
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

    def update_bundle_metadata(self, uuid, metadata):
        check_bundles_have_all_permission(local.model, request.user, [uuid])
        bundle = local.model.get_bundle(uuid)
        self.validate_user_metadata(bundle, metadata)
        local.model.update_bundle(bundle, {'metadata': metadata})

    def _create_cli(self, worksheet_uuid):
        """
        Create an instance of the CLI.

        The CLI uses JsonApiClient to communicate back to the REST API.
        This is admittedly not ideal since now the REST API is essentially
        making HTTP requests back to itself. Future potential solutions might
        include creating a subclass of JsonApiClient that can reroute HTTP
        requests directly to the appropriate Bottle view functions.
        """
        output_buffer = StringIO()
        rest_client = JsonApiClient(self._rest_url(), lambda: get_user_token())
        manager = CodaLabManager(
            temporary=True,
            config=local.config,
            clients={
                self._rest_url(): rest_client
            })
        manager.set_current_worksheet_uuid(self._rest_url(), worksheet_uuid)
        cli = bundle_cli.BundleCLI(manager, headless=True, stdout=output_buffer, stderr=output_buffer)
        return cli, output_buffer

    def complete_command(self, worksheet_uuid, command):
        """
        Given a command string, return a list of suggestions to complete the last token.
        """
        cli, output_buffer = self._create_cli(worksheet_uuid)

        command = command.lstrip()
        if not command.startswith('cl'):
            command = 'cl ' + command

        return cli.complete_command(command)

    @staticmethod
    def get_command(raw_command_map):
        """
        Return a cli-command corresponding to raw_command_map contents.
        Input:
            raw_command_map: a map containing the info to edit, new_value and the action to perform
        """
        return worksheet_util.get_worksheet_info_edit_command(raw_command_map)

    def general_command(self, worksheet_uuid, command):
        """
        Executes an arbitrary CLI command with |worksheet_uuid| as the current worksheet.
        Basically, all CLI functionality should go through this command.
        The method currently intercepts stdout/stderr and returns it back to the user.
        """
        # Tokenize
        if isinstance(command, basestring):
            # shlex throws ValueError on incorrectly formatted commands
            try:
                args = shlex.split(command)
            except ValueError as e:
                raise UsageError(e.message)
        else:
            args = list(command)

        # Ensure command always starts with 'cl'
        if args[0] == 'cl':
            args = args[1:]

        cli, output_buffer = self._create_cli(worksheet_uuid)
        exception = None
        structured_result = None
        try:
            structured_result = cli.do_command(args)
        except SystemExit:  # as exitcode:
            # argparse sometimes throws SystemExit, we don't want to exit
            pass

        output_str = output_buffer.getvalue()
        output_buffer.close()

        return {
            'structured_result': structured_result,
            'output': output_str,
            'exception': exception
        }

    ###############################################
    # methods related to chat box and chat portal #
    ###############################################

    def add_chat_log_info(self, query_info):
        """
        Add the given chat into the database.
        |query_info| encapsulates all the information of one chat
        Example: query_info = {
            'sender_user_id': 1,
            'recipient_user_id': 2,
            'message': 'Hello this is my message',
            'worksheet_uuid': 0x508cf51e546742beba97ed9a69329838,   // the worksheet the user is browsing when he/she sends this message
            'bundle_uuid': 0x8e66b11ecbda42e2a1f544627acf1418,   // the bundle the user is browsing when he/she sends this message
        }
        Return an auto response, if the chat is directed to the system.
        Otherwise, return an updated chat list of the sender.
        """
        updated_data = local.model.add_chat_log_info(query_info)
        if query_info.get('recipient_user_id') != local.model.system_user_id:
            return updated_data
        else:
            message = query_info.get('message')
            worksheet_uuid = query_info.get('worksheet_uuid')
            bundle_uuid = query_info.get('bundle_uuid')
            bot_response = self.format_message_response(ChatBoxQA.answer(message, worksheet_uuid, bundle_uuid))
            info = {
                'sender_user_id': local.model.system_user_id,
                'recipient_user_id': request.user.user_id,
                'message': bot_response,
                'worksheet_uuid': worksheet_uuid,
                'bundle_uuid': bundle_uuid,
            }
            local.model.add_chat_log_info(info)
            return bot_response

    @staticmethod
    def format_message_response(params):
        """
        Format automatic response
        |params| is None if the system can't process the user's message
        or is not confident enough to give a response.
        Otherwise, |params| is a triple that consists of
        the question that the system is trying to answer,
        the response it has for that question, and the recommended command to run.
        Return the automatic response that will be sent back to the user's chat box.
        """
        if params == None:
            return 'Thank you for your question. Our staff will get back to you as soon as we can.'
        else:
            question, response, command = params
            result = 'This is the question we are trying to answer: ' + question + '\n'
            result += response + '\n'
            result += 'You can try to run the following command: \n'
            result += command
            return result

    @staticmethod
    def get_chat_log_info(query_info):
        '''
        |query_info| specifies the user_id of the user that you are querying about.
        Example: query_info = {
            user_id: 2,   // get the chats sent by and received by the user with user_id 2
            limit: 20,   // get the most recent 20 chats related to this user. This is optional, as by default it will get all the chats.
        }
        Return a list of chats that the user have had given the user_id
        '''
        return local.model.get_chat_log_info(query_info)

    @staticmethod
    def get_user_info(user_id):
        if user_id is None:
            user_id = request.user.user_id
        return local.model.get_user_info(user_id, fetch_extra=False)

    @staticmethod
    def get_faq():
        '''
        Return a list of FAQ item, each of the following format:
        '0': {
            'question': 'how can I upload / add a bundle?'
            'answer': {
                'response': 'You can do cl upload or click Update Bundle.',
                'command': 'cl upload <file_path>'
            }
        }
        '''
        file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../objects/chat_box_qa.yaml')
        with open(file_path, 'r') as stream:
            content = yaml.load(stream)
            return content


@get('/worksheets/sample/')
def get_sample_worksheets():
    '''
    Get worksheets to display on the front page.
    Keep only |worksheet_uuids|.
    '''
    service = BundleService()

    # Select good high-quality worksheets and randomly choose some
    list_worksheets = service.search_worksheets(['tag=paper,software,data'])
    list_worksheets = random.sample(list_worksheets, min(3, len(list_worksheets)))

    # Always put home worksheet in
    list_worksheets = service.search_worksheets(['name=home']) + list_worksheets

    # Reformat
    list_worksheets = [{'uuid': val['uuid'],
                        'display_name': val.get('title') or val['name'],
                        'owner_name': val['owner_name']} for val in list_worksheets]

    response.content_type = 'application/json'
    return json.dumps(list_worksheets)


@get('/worksheets/')
def get_worksheets_landing():
    requested_ws = request.query.get('uuid', request.query.get('name', 'home'))
    uuid = get_worksheet_uuid(None, requested_ws)
    redirect('/worksheets/%s/' % uuid)


@post('/api/worksheets/command/')
def post_worksheets_command():
    # TODO(klopyrev): The Content-Type header is not set correctly in
    # editable_field.jsx, so we can't use request.json.
    data = json.loads(request.body.read())
    service = BundleService()

    if data.get('raw_command', None):
        data['command'] = service.get_command(data['raw_command'])

    if not data.get('worksheet_uuid', None) or not data.get('command', None):
        return HTTPResponse("Must have worksheet uuid and command", status=httplib.BAD_REQUEST)

    # If 'autocomplete' field is set, return a list of completions instead
    if data.get('autocomplete', False):
        return {
            'completions': service.complete_command(data['worksheet_uuid'], data['command'])
        }

    result = service.general_command(data['worksheet_uuid'], data['command'])
    # The return value is a list, so the normal Bottle JSON return-value logic
    # doesn't apply since it handles only dicts.
    response.content_type = 'application/json'
    return json.dumps(result)


@get('/api/worksheets/<uuid:re:%s>/' % spec_util.UUID_STR)
def get_worksheet_content(uuid):
    service = BundleService()
    bundle_uuids = request.query.getall('bundle_uuid')
    return service.full_worksheet(uuid, bundle_uuids)


@post('/api/worksheets/<uuid:re:%s>/' % spec_util.UUID_STR,
      apply=AuthenticatedPlugin())
def post_worksheet_content(uuid):
    data = request.json

    lines = data['lines']

    service = BundleService()
    service.parse_and_update_worksheet(uuid, lines)
    return {}


@get('/api/bundles/content/<uuid:re:%s>/' % spec_util.UUID_STR)
@get('/api/bundles/content/<uuid:re:%s>/<path:path>/' % spec_util.UUID_STR)
def get_bundle_content(uuid, path=''):
    service = BundleService()
    info = None
    bundle_info = service.get_bundle_info(uuid)
    if bundle_info and bundle_info['bundle_type'] != PrivateBundle.BUNDLE_TYPE:
        info = service.get_top_level_contents((uuid, path))
    return info if info is not None else {}


@get('/api/bundles/<uuid:re:%s>/' % spec_util.UUID_STR)
def get_bundle_info_(uuid):
    service = BundleService()
    bundle_info = service.get_bundle_info(uuid)
    if bundle_info is None:
        abort(httplib.NOT_FOUND, 'The bundle is not available')
    if bundle_info['bundle_type'] != PrivateBundle.BUNDLE_TYPE:
        bundle_info.update(service.get_bundle_file_contents(uuid))
    return bundle_info


@post('/api/bundles/<uuid:re:%s>/' % spec_util.UUID_STR)
def post_bundle_info(uuid):
    '''
    Save metadata information for a bundle.
    '''
    service = BundleService()
    bundle_info = service.get_bundle_info(uuid)
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

        service.update_bundle_metadata(uuid, new_metadata)
        bundle_info = service.get_bundle_info(uuid)
        return bundle_info
    else:
        abort(httplib.FORBIDDEN, 'Can\'t save unless you\'re the owner')


@get('/api/chatbox/')
def get_chat_box():
    """
    Return a list of chats that the current user has had
    """
    service = BundleService()
    info = {
        'user_id': request.user.user_id if request.user is not None else None
    }
    return {'chats': service.get_chat_log_info(info)}


@post('/api/chatbox',
      apply=AuthenticatedPlugin())
def post_chat_box():
    """
    Add the chat to the log.
    Return an auto response, if the chat is directed to the system.
    Otherwise, return an updated chat list of the sender.
    """
    service = BundleService()
    recipient_user_id = request.POST.get('recipientUserId', None)
    message = request.POST.get('message', None)
    worksheet_uuid = request.POST.get('worksheetId', -1)
    bundle_uuid = request.POST.get('bundleId', -1)
    info = {
        'sender_user_id': request.user.user_id,
        'recipient_user_id': recipient_user_id,
        'message': message,
        'worksheet_uuid': worksheet_uuid,
        'bundle_uuid': bundle_uuid,
    }
    chats = service.add_chat_log_info(info)
    return {'chats': chats}


@get('/api/users/')
def get_users():
    service = BundleService()
    return {
        'user_info': service.get_user_info(None) if request.user is not None else None,
    }


@get('/api/faq/')
def get_faq():
    """
    Return a list of Frequently Asked Questions.
    Currently disabled. Needs further work.
    """
    service = BundleService()
    return {'faq': service.get_faq()}


@post('/api/rpc')
def execute_rpc_call():
    """
    Temporary interface for making simple RPC calls to LocalBundleClient over
    the REST API, to speed up deprecation of XMLRPC while we migrate to REST.

    RPC calls should be POST requests with a JSON payload:
    {
        'method': <name of the LocalBundleClient method to call>,
        'args': <array of args>,
        'kwargs': <object of kwargs>
    }
    """
    service = BundleService()
    data = request.json
    precondition('method' in data, "RPC call must include `method` key")
    method = data['method']
    args = data.get('args', [])
    kwargs = data.get('kwargs', {})
    precondition(isinstance(args, list), "`args` must be list")
    precondition(isinstance(kwargs, dict), "`kwargs` must be dict")
    precondition(hasattr(service, method), "BundleService.%s not defined" % method)
    return {
        'data': getattr(service, method)(*args, **kwargs)
    }
