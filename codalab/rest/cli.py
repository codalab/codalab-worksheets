"""
Web terminal API.
"""
from cStringIO import StringIO
from datetime import datetime, timedelta
import json
from oauthlib.common import generate_token
import shlex

from bottle import (
    httplib,
    HTTPResponse,
    local,
    post,
    request,
    response,
)

from codalab.client.json_api_client import JsonApiClient
from codalab.common import UsageError
from codalab.lib import (
    bundle_cli,
    worksheet_util,
)
from codalab.lib.codalab_manager import CodaLabManager
from codalab.objects.oauth2 import OAuth2Token


@post('/cli/command')
@post('/api/worksheets/command/')  # DEPRECATED
def post_worksheets_command():
    # TODO(klopyrev): The Content-Type header is not set correctly in
    # editable_field.jsx, so we can't use request.json.
    data = json.loads(request.body.read())

    if data.get('raw_command', None):
        data['command'] = worksheet_util.get_worksheet_info_edit_command(data['raw_command'])

    if not data.get('worksheet_uuid', None) or not data.get('command', None):
        return HTTPResponse("Must have worksheet uuid and command", status=httplib.BAD_REQUEST)

    # If 'autocomplete' field is set, return a list of completions instead
    if data.get('autocomplete', False):
        return {
            'completions': complete_command(data['worksheet_uuid'], data['command'])
        }

    result = general_command(data['worksheet_uuid'], data['command'])
    # The return value is a list, so the normal Bottle JSON return-value logic
    # doesn't apply since it handles only dicts.
    response.content_type = 'application/json'
    return json.dumps(result)


def rest_url():
    return 'http://{rest_host}:{rest_port}'.format(**local.config['server'])


def get_user_token():
    """
    Returns an access token for the authenticated user.
    This function facilitates interactions with the bundle service.
    """
    CLIENT_ID = 'codalab_cli_client'

    if not request.user.is_authenticated:
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


def create_cli(worksheet_uuid):
    """
    Create an instance of the CLI.

    The CLI uses JsonApiClient to communicate back to the REST API.
    This is admittedly not ideal since now the REST API is essentially
    making HTTP requests back to itself. Future potential solutions might
    include creating a subclass of JsonApiClient that can reroute HTTP
    requests directly to the appropriate Bottle view functions.
    """
    output_buffer = StringIO()
    rest_client = JsonApiClient(rest_url(), get_user_token)
    manager = CodaLabManager(
        temporary=True,
        config=local.config,
        clients={
            rest_url(): rest_client
        })
    manager.set_current_worksheet_uuid(rest_url(), worksheet_uuid)
    cli = bundle_cli.BundleCLI(manager, headless=True, stdout=output_buffer, stderr=output_buffer)
    return cli, output_buffer


def complete_command(worksheet_uuid, command):
    """
    Given a command string, return a list of suggestions to complete the last token.
    """
    cli, output_buffer = create_cli(worksheet_uuid)

    command = command.lstrip()
    if not command.startswith('cl'):
        command = 'cl ' + command

    return cli.complete_command(command)


def general_command(worksheet_uuid, command):
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

    cli, output_buffer = create_cli(worksheet_uuid)
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
