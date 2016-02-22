import time
from httplib import BAD_REQUEST

from bottle import (
    abort,
    install,
    local,
    request,
    run,
    mount,
)


class SaveEnvironmentPlugin(object):
    """Saves environment objects in the local request variable."""
    api = 2
    
    def __init__(self, model, bundle_store, config):
        self.model = model
        self.bundle_store = bundle_store
        self.config = config

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            local.model = self.model
            local.bundle_store = self.bundle_store
            local.config = self.config
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

            # TODO(klopyrev): Log real user ID and name.
            local.model.update_events_log(
                start_time=start_time,
                user_id='',
                user_name='',
                command=command,
                args=args)

            return res

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


def run_rest_server(manager, debug, num_workers):
    """Runs the REST server."""
    host = manager.config['server']['rest_host']
    port = manager.config['server']['rest_port']

    install(SaveEnvironmentPlugin(manager.model(), manager.bundle_store(), manager.config))
    install(CheckJsonPlugin())
    install(LoggingPlugin())

    # Import views
    import codalab.rest.login
    import codalab.rest.users
    from codalab.rest.oauth2 import oauth2_app

    # Mount OAuth2 endpoints
    oauth2_app.install(SaveEnvironmentPlugin(manager.model(), manager.bundle_store(), manager.config))
    mount('/oauth2/', oauth2_app)

    if not debug:
        # We use gunicorn to create a server with multiple processes, since in
        # Python a single process uses at most 1 CPU due to the Global
        # Interpreter Lock.
        # We use gevent so that each of the processes handles each request in a
        # greenlet (a sort of a lightweight thread).
        run(host=host, port=port, debug=False, server='gunicorn',
            workers=num_workers, worker_class='gevent')
    else:
        run(host=host, port=port, debug=True)
