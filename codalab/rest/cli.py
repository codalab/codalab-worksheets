"""
Web terminal API.
"""
from io import StringIO
from datetime import datetime, timedelta
from oauthlib.common import generate_token
import shlex

from bottle import abort, httplib, local, post, request

from codalab.client.json_api_client import JsonApiClient
from codalab.common import UsageError
from codalab.lib import bundle_cli
from codalab.lib.codalab_manager import CodaLabManager
from codalab.objects.oauth2 import OAuth2Token
from codalab.server.authenticated_plugin import ProtectedPlugin


@post('/cli/command', apply=ProtectedPlugin())
def post_worksheets_command():
    """
    JSON request body:
    ```
    {
        "worksheet_uuid": "0xea72f9b6aa754636a6657ff2b5e005b0",
        "command": "cl run :main.py 'python main.py'",
        "autocomplete": false
    }
    ```

    JSON response body:
    ```
    {
        "structured_result": { ... },
        "output": "..."
    }
    ```
    """
    query = request.json
    if 'worksheet_uuid' not in query:
        abort(httplib.BAD_REQUEST, 'Missing `workhseet_uuid`')
    if 'command' not in query:
        abort(httplib.BAD_REQUEST, 'Missing `command`')

    # If 'autocomplete' field is set, return a list of completions instead
    if query.get('autocomplete', False):
        return {'completions': complete_command(query['worksheet_uuid'], query['command'])}

    return general_command(query['worksheet_uuid'], query['command'])


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
        CLIENT_ID, request.user.user_id, datetime.utcnow() + timedelta(minutes=5)
    )
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
    rest_extra_headers = local.config['server'].get('extra_headers', {})
    rest_client = JsonApiClient(rest_url(), get_user_token, rest_extra_headers)
    manager = CodaLabManager(temporary=True, config=local.config, clients={rest_url(): rest_client})
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
    if isinstance(command, str):
        # shlex throws ValueError on incorrectly formatted commands
        try:
            # see https://docs.python.org/2/library/shlex.html#shlex.shlex.escapedquotes
            # By default, the double quote can be escaped. By setting the
            # escapedquotes property, we are able to escape single quotes as well
            # examples: run '\''
            lexer = shlex.shlex(command, posix=True)
            lexer.escapedquotes = '\'"'
            lexer.whitespace_split = True
            args = list(lexer)
        except ValueError as e:
            raise UsageError(str(e))
    else:
        args = list(command)

    # Ensure command always starts with 'cl'
    if args[0] == 'cl':
        args = args[1:]

    cli, output_buffer = create_cli(worksheet_uuid)
    structured_result = None
    try:
        structured_result = cli.do_command(args)
    except SystemExit:  # as exitcode:
        # argparse sometimes throws SystemExit, we don't want to exit
        pass

    output_str = output_buffer.getvalue()
    output_buffer.close()

    return {'structured_result': structured_result, 'output': output_str}
