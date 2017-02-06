import httplib

from bottle import abort, get, request, local, post

from codalab.lib.spec_util import NAME_REGEX
from codalab.lib.server_util import bottle_patch as patch
from codalab.rest.schemas import (
#    AuthenticatedUserSchema,
    USER_READ_ONLY_FIELDS,
    UserSchema,
)
from codalab.server.authenticated_plugin import (
    AuthenticatedPlugin,
    UserVerifiedPlugin,
)

@post('/help/', apply=AuthenticatedPlugin(), skip=UserVerifiedPlugin)
def fetch_help():
    print request.user
    print request.query
    # user = AuthenticatedUserSchema().dump(request.user).data
    # print user

    for param in request.params: print param
    for query in request.query: print query
    print ''
    print '*' * 100
    import pdb; pdb.set_trace()
