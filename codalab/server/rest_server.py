from httplib import BAD_REQUEST
import sys
import time

from bottle import (
    abort,
    Bottle,
    default_app,
    error,
    get,
    HTTPError,
    HTTPResponse,
    install,
    local,
    request,
    run,
    static_file,
)

import codalab.rest.account
import codalab.rest.example
import codalab.rest.oauth2
import codalab.rest.users
from codalab.server.authenticated_plugin import UserVerifiedPlugin
from codalab.server.cookie import CookieAuthenticationPlugin
from codalab.server.oauth2_provider import oauth2_provider


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
            start_time = time.time()

            res = callback(*args, **kwargs)
            
            command = route.method + ' ' + route.rule
            query_dict = (
                dict(map(lambda k: (k, request.query[k]), request.query)))
            args = [request.path, query_dict]
            if (route.method == 'POST'
                and request.content_type == 'application/json'):
                args.append(request.json)

            local.model.update_events_log(
                start_time=start_time,
                user_id=getattr(getattr(local, 'user', None), 'user_id', ''),
                user_name=getattr(getattr(local, 'user', None), 'user_name', ''),
                command=command,
                args=args)

            return res

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


def run_rest_server(manager, debug, num_processes):
    """Runs the REST server."""
    host = manager.config['server']['rest_host']
    port = manager.config['server']['rest_port']

    install(SaveEnvironmentPlugin(manager))
    install(CheckJsonPlugin())
    install(LoggingPlugin())
    install(oauth2_provider.check_oauth())
    install(CookieAuthenticationPlugin())
    install(UserVerifiedPlugin())

    root_app = Bottle()
    root_app.mount('/rest', default_app())
    
    for code in xrange(100, 600):
        root_app.error(code)(error_handler)

    # We use gunicorn to create a server with multiple processes, since in
    # Python a single process uses at most 1 CPU due to the Global Interpreter
    # Lock.
    # We use gevent so that each of the processes handles each request in a
    # greenlet (a sort of a lightweight thread).
    sys.argv = sys.argv[:1] # Small hack to work around a Gunicorn arg parsing
                            # bug. None of the arguments to cl should go to
                            # Gunicorn.
    run(app=root_app, host=host, port=port, debug=debug, server='gunicorn',
        workers=num_processes, worker_class='gevent' if not debug else 'sync',
        timeout=5 * 60)
