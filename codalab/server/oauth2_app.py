"""
Bottle app for the OAuth2 authorization and token endpoints.
"""
from datetime import datetime, timedelta

from bottle import Bottle

from codalab.server.oauth2_provider import OAuth2Provider
from codalab.objects.oauth2 import OAuth2AuthCode, OAuth2Client, OAuth2Token


def create_oauth2_app(model):
    app = Bottle()
    oauth = OAuth2Provider(app)

    @oauth.clientgetter
    def get_client(client_id):
        return model.get_oauth2_client(client_id)

    @oauth.grantgetter
    def get_grant(client_id, code):
        return model.get_oauth2_auth_code(client_id, code)

    @oauth.grantsetter
    def set_grant(client_id, code, request, *args, **kwargs):
        # Grant expires in 100 seconds
        expires = datetime.utcnow() + timedelta(seconds=100)
        grant = OAuth2AuthCode(
            model,
            client_id=client_id,
            code=code['code'],
            redirect_uri=request.redirect_uri,
            scopes=','.join(request.scopes),
            user_id=None,  # FIXME: get current authenticated user
            expires=expires
        )
        return model.save_oauth2_auth_code(grant)

    @oauth.tokengetter
    def get_token(access_token=None, refresh_token=None):
        return model.get_oauth2_token(access_token, refresh_token)

    @oauth.tokensetter
    def set_token(token, request, *args, **kwargs):
        # Make sure that every client has only one token connected to a user
        model.clear_oauth2_tokens(request.client.client_id, request.user.user_id)

        expires_in = token.get('expires_in')
        expires = datetime.utcnow() + timedelta(seconds=expires_in)

        token = OAuth2Token(
            model,
            access_token=token['access_token'],
            refresh_token=token['refresh_token'],
            scopes=token['scope'],
            expires=expires,
            client_id=request.client.client_id,
            user_id=request.user.user_id,
        )

        return model.save_oauth2_token(token)

    @oauth.usergetter
    def get_user(username, password, *args, **kwargs):
        user = model.get_user(username)
        if user.check_password(password):
            return user
        return None

    # @app.route('/authorize')
    # @oauth.authorize_handler
    # def authorize():
    #     return 'Hello World'

    @app.route('/token', methods=['POST'])
    @oauth.token_handler
    def handle_token():
        return None

    return app
