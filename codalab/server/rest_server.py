from httplib import INTERNAL_SERVER_ERROR, BAD_REQUEST
import os
import re
import sys
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
    local,
    request,
    run,
    static_file,
)

from codalab.common import exception_to_http_error
import codalab.rest.account
import codalab.rest.bundle
import codalab.rest.example  # TODO: Delete.
import codalab.rest.legacy
import codalab.rest.oauth2
import codalab.rest.titlejs
import codalab.rest.users
import codalab.rest.worker
from codalab.server.authenticated_plugin import UserVerifiedPlugin
from codalab.server.cookie import CookieAuthenticationPlugin
from codalab.server.oauth2_provider import oauth2_provider


# Don't log requests to routes matching these regexes.
ROUTES_NOT_LOGGED_REGEXES = [
    re.compile(r'/oauth2/.*'),
    re.compile(r'/worker/.*'),
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
            local.launch_new_worker_system = self.manager.launch_new_worker_system()
            local.worker_model = self.manager.worker_model()
            local.upload_manager = self.manager.upload_manager()
            local.download_manager = self.manager.download_manager()
            local.bundle_store = self.manager.bundle_store()
            local.config = self.manager.config
            local.emailer = self.manager.emailer()
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
            
            command = route.method + ' ' + route.rule
            query_dict = (
                dict(map(lambda k: (k, request.query[k]), request.query)))
            args = [request.path, query_dict]
            if (route.method == 'POST'
                and request.content_type == 'application/json'):
                try:
                    args.append(request.json)
                except ValueError:
                    pass

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

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            try:
                return callback(*args, **kwargs)
            except Exception as e:
                if isinstance(e, HTTPResponse):
                    raise
                code, message = exception_to_http_error(e)
                if code == INTERNAL_SERVER_ERROR:
                    traceback.print_exc()
                raise HTTPError(code, message)

        return wrapper



def error_handler(response):
    """Simple error handler that doesn't use the Bottle error template."""
    if request.is_ajax:
        return HTTPResponse(body=response.body, status=response.status)
    else:
        return request.app.default_error_handler(response)


@get('/static/<filename:path>')
def send_static(filename):
    return static_file(filename, root='static/')


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
    install(ErrorAdapter())

    for code in xrange(100, 600):
        default_app().error(code)(error_handler)

    root_app = Bottle()
    root_app.mount('/rest', default_app())

    bottle.TEMPLATE_PATH = [os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'views')]

    # We use gunicorn to create a server with multiple processes, since in
    # Python a single process uses at most 1 CPU due to the Global Interpreter
    # Lock.
    sys.argv = sys.argv[:1] # Small hack to work around a Gunicorn arg parsing
                            # bug. None of the arguments to cl should go to
                            # Gunicorn.
    run(app=root_app, host=host, port=port, debug=debug, server='gunicorn',
        workers=num_processes, worker_class='gthread', threads=num_threads,
        timeout=5 * 60)
