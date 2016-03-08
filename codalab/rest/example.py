from bottle import get, post, request

from codalab.lib import spec_util
from codalab.server.authenticated_plugin import AuthenticatedPlugin


@get('/example/stream_file', apply=AuthenticatedPlugin())
def stream_file():
    # Stream a file back.
    return open(__file__, 'rb')


@post('/example/post_and_get_json/<uuid:re:%s>/' % spec_util.UUID_STR)
def post_json(uuid):
    print(request.json)
    response = {'test': 'test1', 'test2': 5}
    return response


@post('/example/post_file')
def post_file():
    data = request['wsgi.input']
    # You can now stream the input.
    return ''


@get('/example/oauth_protected', apply=AuthenticatedPlugin())
def oauth_protected():
    return "Hi %s." % request.user.user_name
