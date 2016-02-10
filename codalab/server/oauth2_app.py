"""
Bottle app for the OAuth2 authorization and token endpoints.
"""
from bottle import Bottle

from codalab.server.oauth2_provider import OAuth2Provider


def OAuth2App():
    app = Bottle()

    OAuth2Provider()


    @app.route('/authorize')
    def authorize():
        return 'Hello World'

    return app
