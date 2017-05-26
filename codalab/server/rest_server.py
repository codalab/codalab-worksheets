from httplib import INTERNAL_SERVER_ERROR, BAD_REQUEST
import datetime
import json
import os
import re
import sys
import textwrap
import time
import traceback

import bottle
from bottle import (
    abort,
    Bottle,
    default_app,
    get,
    HTTPError,
    HTTPResponse,
    install,
    JSONPlugin,
    local,
    request,
    run,
    static_file,
    uninstall,
)

from codalab.common import exception_to_http_error
from codalab.lib import formatting, server_util
import codalab.rest.account
import codalab.rest.bundle_actions
import codalab.rest.bundles
import codalab.rest.chats
import codalab.rest.cli
import codalab.rest.groups
import codalab.rest.help
import codalab.rest.interpret
import codalab.rest.oauth2
import codalab.rest.titlejs
import codalab.rest.users
import codalab.rest.workers
import codalab.rest.worksheets
from codalab.server.authenticated_plugin import (
    PublicUserPlugin,
    UserVerifiedPlugin,
)
from codalab.server.cookie import CookieAuthenticationPlugin
from codalab.server.json_api_plugin import JsonApiPlugin
from codalab.server.oauth2_provider import oauth2_provider


# Don't log requests to routes matching these regexes.
ROUTES_NOT_LOGGED_REGEXES = [
    re.compile(r'/oauth2/.*'),
    re.compile(r'/workers/.*'),
]


class SaveEnvironmentPlugin(object):
    """Saves environment objects in the local request variable."""
    api = 2
    
    def __init__(self, manager):
        self.manager = manager

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            # Note that the model is created here during the first request to
            # the server. This is intentional to ensure that any MySQL engine
            # objects are created after forking.
            local.model = self.manager.model()
            local.worker_model = self.manager.worker_model()
            local.upload_manager = self.manager.upload_manager()
            local.download_manager = self.manager.download_manager()
            local.bundle_store = self.manager.bundle_store()
            local.config = self.manager.config
            local.emailer = self.manager.emailer
            return callback(*args, **kwargs)

        return wrapper


class CheckJsonPlugin(object):
    """Checks that the input JSON data can be parsed."""
    api = 2
    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            try:
                # TODO(klopyrev): Version 0.13 of bottle incorporates this
                # check. We can get rid of this plugin once that version is
                # released and we upgrade.
                request.json
            except ValueError:
                abort(BAD_REQUEST, 'Invalid JSON')
            return callback(*args, **kwargs)
        return wrapper


class LoggingPlugin(object):
    """Logs successful requests to the events log."""
    api = 2

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            if not self._should_log(route.rule):
                return callback(*args, **kwargs)

            start_time = time.time()

            res = callback(*args, **kwargs)

            # Use explicitly defined route name or 'METHOD /rule'
            command = route.name or (route.method + ' ' + route.rule)
            query_dict = (
                dict(map(lambda k: (k, request.query[k]), request.query)))
            args = [request.path, query_dict]
            # if (route.method == 'POST'
            #     and request.content_type == 'application/json'):
            #     args.append(request.json)

            local.model.update_events_log(
                start_time=start_time,
                user_id=getattr(getattr(local, 'user', None), 'user_id', ''),
                user_name=getattr(getattr(local, 'user', None), 'user_name', ''),
                command=command,
                args=args)

            return res

        return wrapper

    def _should_log(self, rule):
        for regex in ROUTES_NOT_LOGGED_REGEXES:
            if regex.match(rule):
                return False
        return True


class ErrorAdapter(object):
    """Converts known exceptions to HTTP errors."""
    api = 2

    MAX_AUX_INFO_LENGTH = 5000

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            try:
                return callback(*args, **kwargs)
            except Exception as e:
                if isinstance(e, HTTPResponse):
                    raise
                code, message = exception_to_http_error(e)
                if code == INTERNAL_SERVER_ERROR:
                    self.report_exception(e)
                    message = "Unexpected Internal Error (%s). The administrators have been notified." % message
                raise HTTPError(code, message)

        return wrapper

    @staticmethod
    def _censor_passwords(pairs):
        # Return iterator over pairs censoring any values with 'password' in the key
        return ((k, '<censored>') if 'password' in k else (k, v) for k, v in pairs)

    def report_exception(self, exc):
        query = formatting.key_value_list(request.query.allitems())
        forms = formatting.key_value_list(
            self._censor_passwords(request.forms.allitems()) if request.json is None else [])
        body = formatting.verbose_pretty_json(request.json)
        local_vars = formatting.key_value_list(
            self._censor_passwords(server_util.exc_frame_locals().items()))
        aux_info = textwrap.dedent("""\
                    Query params:
                    {0}

                    Form params:
                    {1}

                    JSON body:
                    {2}

                    Local variables:
                    {3}""").format(query, forms, body, local_vars)

        if len(aux_info) > self.MAX_AUX_INFO_LENGTH:
            aux_info = aux_info[:(self.MAX_AUX_INFO_LENGTH / 2)] + \
                       "(...truncated...)" + \
                       aux_info[-(self.MAX_AUX_INFO_LENGTH / 2):]

        message = textwrap.dedent("""\
             Error on request by {0.user}:

             {0.method} {0.path}

             {1}

             {2}""").format(request, aux_info, traceback.format_exc())

        # Both print to console and send email
        print >>sys.stderr, message
        self.send_email(exc, message)

    @server_util.rate_limited(max_calls_per_hour=6)
    def send_email(self, exc, message):
        # Caller is responsible for logging message anyway if desired
        if 'admin_email' not in local.config['server']:
            print >>sys.stderr, 'Warning: No admin_email configured, so no email sent.'
            return

        # Subject should be "ExceptionType: message"
        subject = '%s: %s' % (type(exc).__name__, exc.message)

        # Prepend server name to subject if available
        if 'instance_name' in local.config['server']:
            subject = "[%s] %s" % (local.config['server']['instance_name'], subject)

        local.emailer.send_email(subject=subject,
                                 body=message,
                                 recipient=local.config['server']['admin_email'])


class DatetimeEncoder(json.JSONEncoder):
    """Extend JSON encoder to handle datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def error_handler(response):
    """Simple error handler that doesn't use the Bottle error template."""
    if request.is_ajax:
        return HTTPResponse(body=response.body, status=response.status)
    else:
        return request.app.default_error_handler(response)


@get('/static/<filename:path>')
def send_static(filename):
    return static_file(filename, root='static/')


def dummy_xmlrpc_app():
    app = Bottle()
    return app


def run_rest_server(manager, debug, num_processes, num_threads):
    """Runs the REST server."""
    host = manager.config['server']['rest_host']
    port = manager.config['server']['rest_port']

    install(SaveEnvironmentPlugin(manager))
    install(CheckJsonPlugin())
    install(LoggingPlugin())
    install(oauth2_provider.check_oauth())
    install(CookieAuthenticationPlugin())
    install(UserVerifiedPlugin())
    install(PublicUserPlugin())
    install(ErrorAdapter())

    # Replace default JSON plugin with one that handles datetime objects
    # Note: ErrorAdapter must come before JSONPlugin to catch serialization errors
    uninstall(JSONPlugin())
    install(JSONPlugin(json_dumps=DatetimeEncoder().encode))

    # JsonApiPlugin must come after JSONPlugin, to inspect and modify response
    # dicts before they are serialized into JSON
    install(JsonApiPlugin())

    for code in xrange(100, 600):
        default_app().error(code)(error_handler)

    root_app = Bottle()
    root_app.mount('/rest', default_app())

    # Look for templates in codalab-cli/views
    bottle.TEMPLATE_PATH = [os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'views')]

    # Increase the request body size limit to 8 MiB
    bottle.BaseRequest.MEMFILE_MAX = 8 * 1024 * 1024

    # We use gunicorn to create a server with multiple processes, since in
    # Python a single process uses at most 1 CPU due to the Global Interpreter
    # Lock.
    sys.argv = sys.argv[:1] # Small hack to work around a Gunicorn arg parsing
                            # bug. None of the arguments to cl should go to
                            # Gunicorn.
    run(app=root_app, host=host, port=port, debug=debug, server='gunicorn',
        workers=num_processes, worker_class='gthread', threads=num_threads,
        worker_tmp_dir='/tmp',  # don't use globally set tempdir
        timeout=5 * 60)
