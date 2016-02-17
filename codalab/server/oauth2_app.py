"""
Bottle app for the OAuth2 authorization and token endpoints.
"""
from datetime import datetime, timedelta

from bottle import Bottle, request, template, local

from codalab.server.oauth2_provider import OAuth2Provider
from codalab.objects.oauth2 import OAuth2AuthCode, OAuth2Token


def create_oauth2_app():
    app = Bottle()
    oauth = OAuth2Provider(app)

    @oauth.clientgetter
    def get_client(client_id):
        return local.model.get_oauth2_client(client_id)

    @oauth.grantgetter
    def get_grant(client_id, code):
        return local.model.get_oauth2_auth_code(client_id, code)

    @oauth.grantsetter
    def set_grant(client_id, code, request, *args, **kwargs):
        # Grant expires in 100 seconds
        expires = datetime.utcnow() + timedelta(seconds=100)
        grant = OAuth2AuthCode(
            local.model,
            client_id=client_id,
            code=code['code'],
            redirect_uri=request.redirect_uri,
            scopes=','.join(request.scopes),
            user_id=1337,  # FIXME: get current authenticated user
            expires=expires
        )
        return local.model.save_oauth2_auth_code(grant)

    @oauth.tokengetter
    def get_token(access_token=None, refresh_token=None):
        return local.model.get_oauth2_token(access_token, refresh_token)

    @oauth.tokensetter
    def set_token(token, request, *args, **kwargs):
        # Make sure that every client has only one token connected to a user
        local.model.clear_oauth2_tokens(request.client.client_id, request.user.user_id)

        expires_in = token.get('expires_in')
        expires = datetime.utcnow() + timedelta(seconds=expires_in)

        token = OAuth2Token(
            local.model,
            access_token=token['access_token'],
            refresh_token=token.get('refresh_token', None),
            scopes=token['scope'],
            expires=expires,
            client_id=request.client.client_id,
            user_id=request.user.user_id,
        )

        return local.model.save_oauth2_token(token)

    @oauth.usergetter
    def get_user(username, password, *args, **kwargs):
        user = local.model.get_user(username=username)
        if user.check_password(password):
            return user
        return None

    @app.route('/login', ['GET', 'POST'])
    def do_login():
        pass
        # username = request.forms.get('username')
        # password = request.forms.get('password')
        # if check_login(username, password):
        #     response.set_cookie("account", username, secret='some-secret-key')
        #     return template("<p>Welcome {{name}}! You are now logged in.</p>", name=username)
        # else:
        #     return "<p>Login failed.</p>"

    def require_login(callback):
        def wrapper(*args, **kwargs):
            # check that username is still defined on cookie
            # and check that cookie has not expired
            return callback(*args, **kwargs)

        return wrapper

    # The other route is to write a Plugin and add it to the "apply" param to the authorize view function

    # @require_login
    @app.route('/authorize', ['GET', 'POST'])
    @oauth.authorize_handler
    def authorize(*args, **kwargs):
        if request.method == 'GET':
            client_id = kwargs.get('client_id')
            client = local.model.get_oauth2_client(client_id)
            return "<h1>authorizing for %s</h1>" % client_id
        elif request.method == 'POST':
            confirm = request.forms.get('confirm', 'no')
            return confirm == 'yes'

    @app.route('/token', ['POST'])
    @oauth.token_handler
    def handle_token(): pass

    @app.route('/token', ['POST'])
    @oauth.revoke_handler
    def revoke_token(): pass

    return app
