from bottle import abort, install, request, run
from codalab.rest.example import register_example_endpoints
from httplib import BAD_REQUEST
import time


REGISTER_ENDPOINT_FNS = [
    # TODO(klopyrev): Remove example once it is no longer needed.
    register_example_endpoints
]


class LoggingPlugin(object):
    api = 2
    
    def __init__(self, model):
        self.model = model
    
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
            self.model.update_events_log(
                start_time=start_time,
                user_id='',
                user_name='',
                command=command,
                args=args)

            return res

        return wrapper


class AuthenticationPlugin(object):
    api = 2
    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            print('authentication')
            # TODO(sckoo): Add authentication. The information about the user
            # can be put in the "local" thread-local variable in bottle.
            # TODO(sckoo): Update the logging plugin to log real user ID and
            # and name.
            return callback(*args, **kwargs)
        return wrapper


class CheckJsonPlugin(object):
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
    host = manager.config['server']['rest_host']
    port = manager.config['server']['rest_port']

    install(CheckJsonPlugin())
    install(LoggingPlugin(manager.model()))
    authentication_plugin = AuthenticationPlugin()
    
    for register_fn in REGISTER_ENDPOINT_FNS:
        register_fn(manager.model(), manager.bundle_store(),
                    authentication_plugin)

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
