"""
Legacy REST APIs moved from the codalab-worksheets Django REST server. 
"""
import base64
from contextlib import closing
from cStringIO import StringIO
from datetime import datetime, timedelta
import json
from oauthlib.common import generate_token
import random
import shlex

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
from codalab.client.local_bundle_client import LocalBundleClient
from codalab.client.remote_bundle_client import RemoteBundleClient
from codalab.common import State, UsageError
from codalab.lib import (
  bundle_cli,
  file_util,
  formatting,
  metadata_util,
  spec_util,
  worksheet_util,
  zip_util,
)
from codalab.lib.codalab_manager import CodaLabManager
from codalab.model.tables import GROUP_OBJECT_PERMISSION_ALL
from codalab.objects.oauth2 import OAuth2Token
from codalab.objects.permission import permission_str
from codalab.server.auth import LocalUserAuthHandler
from codalab.server.authenticated_plugin import AuthenticatedPlugin
from codalab.server.rpc_file_handle import RPCFileHandle


class BundleService(object):
    '''
    Adapts the LocalBundleClient for REST calls.
    '''

    def __init__(self):
        self.client = LocalBundleClient(
            'local', local.bundle_store, local.model,
            local.upload_manager, local.download_manager,
            LocalUserAuthHandler(request.user, local.model), verbose=0)

    def get_bundle_info(self, uuid):
        bundle_info = self.client.get_bundle_info(uuid, True, True, True)

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
        bundle_info['editable_metadata_fields'] = worksheet_util.get_editable_metadata_fields(cls, metadata)

        return bundle_info

    def search_worksheets(self, keywords, worksheet_uuid=None):
        return self.client.search_worksheets(keywords)

    def get_worksheet_uuid(self, spec):
        # generic function sometimes get uuid already just return it.
        if spec_util.UUID_REGEX.match(spec):
            return spec
        else:
            return worksheet_util.get_worksheet_uuid(self.client, None, spec)

    def full_worksheet(self, uuid):
        """
        Return information about a worksheet. Calls
        - get_worksheet_info: get basic info
        - resolve_interpreted_items: get more information about a worksheet.
        In the future, for large worksheets, might want to break this up so
        that we can render something basic.
        """
        worksheet_info = self.client.get_worksheet_info(uuid, True, True)

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

        worksheet_info['items'] = self.client.resolve_interpreted_items(interpreted_items['items'])
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
                    if bundle_info['bundle_type'] != PrivateBundle.BUNDLE_TYPE:
                        target_info = self.get_top_level_contents((bundle_info['uuid'], ''))
                        bundle_info['target_info'] = target_info
                    try:
                        if isinstance(bundle_info, dict):
                            worksheet_util.format_metadata(bundle_info.get('metadata'))
                    except Exception, e:
                        print e
                        import ipdb; ipdb.set_trace()

        return worksheet_info

    def parse_and_update_worksheet(self, uuid, lines):
        """
        Replace worksheet |uuid| with the raw contents given by |lines|.
        """
        worksheet_info = self.client.get_worksheet_info(uuid, True)
        new_items, commands = worksheet_util.parse_worksheet_form(lines, self.client, worksheet_info['uuid'])
        self.client.update_worksheet_items(worksheet_info, new_items)
        # Note: commands are ignored

    def get_bundle_file_contents(self, uuid):
        """
        If bundle is a single file, get file contents.
        Otherwise, get stdout and stderr.
        For each file, return a truncated version.
        """
        def get_summary(info, name):
            if info['type'] == 'file':
                TRUNCATION_TEXT = (
                    '\n'
                    '... Truncated. Click link above to see full file. ...\n'
                    '\n')
                contents = local.download_manager.summarize_file(
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
            info['file_contents'] = get_summary(info, '')
        elif info['type'] == 'directory':
            # Read contents of stdout and stderr.
            info['stdout'] = None
            info['stderr'] = None
            for item in info['contents']:
                name = item['name']
                if name in ['stdout', 'stderr'] and (item['type'] == 'file' or item['type'] == 'link'):
                    info[name] = get_summary(item, name)
        return info

    def get_top_level_contents(self, target):
        info = self.client.get_target_info(target, 1)
        if info is not None and info['type'] == 'directory':
            for item in info['contents']:
                item['size_str'] = formatting.size_str(item['size'])
        return info

    # Create an instance of a CLI.
    def _create_cli(self, worksheet_uuid):
        output_buffer = StringIO()
        manager = CodaLabManager(temporary=True, clients={'local': self.client})
        manager.set_current_worksheet_uuid(self.client, worksheet_uuid)
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

    def get_command(self, raw_command_map):
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
            args = shlex.split(command)
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
            # this should not happen under normal circumstances
            pass
        except BaseException as e:
            exception = str(e)

        output_str = output_buffer.getvalue()
        output_buffer.close()

        return {
            'structured_result': structured_result,
            'output': output_str,
            'exception': exception
        }

    def update_bundle_metadata(self, uuid, new_metadata):
        self.client.update_bundle_metadata(uuid, new_metadata)
        return

    def add_chat_log_info(self, query_info):
        return self.client.add_chat_log_info(query_info)    

    def get_chat_log_info(self, query_info):
        return self.client.get_chat_log_info(query_info)

    def get_user_info(self, user_id):
        return self.client.get_user_info(user_id, True)

    def get_faq(self):
        return self.client.get_faq()

class RemoteBundleService(object):
    '''
    Adapts the RemoteBundleClient for REST calls.
    TODO(klopyrev): This version should eventually go away once the file upload
    logic is cleaned up. See below where this class is used for more information.
    '''
    def __init__(self):
        self.client = RemoteBundleClient(self._cli_url(),
                                         lambda command: self._get_user_token(), verbose=0)

    def _cli_url(self):
        return 'http://' + local.config['server']['host'] + ':' + str(local.config['server']['port'])

    def _get_user_token(self):
        """
        Returns an access token for the user. This function facilitates interactions
        with the bundle service.
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

    def upload_bundle(self, source_file, bundle_type, worksheet_uuid):
        """
        Upload |source_file| (a stream) to |worksheet_uuid|.
        """
        # Construct info for creating the bundle.
        bundle_subclass = get_bundle_subclass(bundle_type) # program or data
        metadata = metadata_util.fill_missing_metadata(bundle_subclass, {}, initial_metadata={'name': source_file.filename, 'description': 'Upload ' + source_file.filename})
        info = {'bundle_type': bundle_type, 'metadata': metadata}

        # Upload it by creating a file handle and copying source_file to it (see RemoteBundleClient.upload_bundle in the CLI).
        remote_file_uuid = self.client.open_temp_file(metadata['name'])
        try:
            with closing(RPCFileHandle(remote_file_uuid, self.client.proxy)) as dest:
                file_util.copy(source_file.file, dest, autoflush=False, print_status='Uploading %s' % metadata['name'])
           
            pack = False  # For now, always unpack (note: do this after set remote_file_uuid, which needs the extension)
            if not pack and zip_util.path_is_archive(metadata['name']):
                metadata['name'] = zip_util.strip_archive_ext(metadata['name'])
           
            # Then tell the client that the uploaded file handle is there.
            new_bundle_uuid = self.client.finish_upload_bundle(
                [remote_file_uuid],
                not pack,  # unpack
                info,
                worksheet_uuid,
                True)  # add_to_worksheet
        except:
            self.client.finalize_file(remote_file_uuid)
            raise
        return new_bundle_uuid


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
    service = BundleService()
    uuid = service.get_worksheet_uuid(requested_ws)
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
    return service.full_worksheet(uuid)


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


@post('/api/bundles/upload/')
def post_bundle_upload():
    # TODO(klopyrev): This file upload logic is not optimal. The upload goes
    # to the remote XML RPC bundle service, just like it did before when this
    # API was implemented in Django. Ideally, this REST server should just store
    # the upload to the bundle store directly. A bunch of logic needs to be
    # cleaned up in order for that to happen.
    service = RemoteBundleService()
    source_file = request.files['file']
    bundle_type = request.POST['bundle_type']
    worksheet_uuid = request.POST['worksheet_uuid']
    new_bundle_uuid =  service.upload_bundle(source_file, bundle_type, worksheet_uuid)
    return {'uuid': new_bundle_uuid}


@get('/api/bundles/<uuid:re:%s>/' % spec_util.UUID_STR)
def get_bundle_info(uuid):
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
        for key in ['data_size', 'created', 'time', 'time_user', 'time_system', 'memory', 'disk_read', 'disk_write', 'exitcode', 'actions', 'started', 'last_updated']:
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
