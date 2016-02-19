"""
Bottle app for the OAuth2 authorization and token endpoints.
"""
from datetime import datetime, timedelta
from urllib import urlencode

from bottle import Bottle, request, response, template, local, redirect

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

    @app.route('/login', ['GET', 'POST'], name='login')
    def do_login():
        if request.method == 'GET':
            return template("login", error=None)
        elif request.method == 'POST':
            redirect_uri = request.query.get('redirect_uri')
            username = request.forms.get('username')
            password = request.forms.get('password')

            user = local.model.get_user(username=username)
            if user.check_password(password):
                response.set_cookie("user_id", user.user_id, secret='some-secret-key', max_age=3600)  # FIXME generate and store in config, and set expiry date
                if redirect_uri:
                    # FIXME Does this need to be safer?
                    return redirect(redirect_uri)
                else:
                    return "<p>Successfully signed into CodaLab.</p>"
            else:
                return template("login", redirect_uri=redirect_uri, error="Login/password did not match.")

    def require_login(callback):
        def wrapper(*args, **kwargs):
            # check that username is still defined on cookie
            # and check that cookie has not expired
            user_id = request.get_cookie("user_id", secret='some-secret-key')
            if user_id:
                local.user = local.model.get_user(user_id=user_id)
            else:
                # TODO pass all params?
                # Make sure X-Forwarded-Host is set properly if behind reverse-proxy to use request.url
                redirect("%s?%s" % (app.get_url('login'), urlencode({"redirect_uri": request.url})))

            return callback(*args, **kwargs)

        return wrapper

    # The other route is to write a Plugin and add it to the "apply" param to the authorize view function

    @app.route('/authorize', ['GET', 'POST'])
    @require_login
    @oauth.authorize_handler
    def authorize(*args, **kwargs):
        if request.method == 'GET':
            client_id = kwargs.get('client_id')
            redirect_uri = kwargs.get('redirect_uri')
            client = local.model.get_oauth2_client(client_id)
            return template("oauth2_authorize", client=client, redirect_uri=redirect_uri)
        elif request.method == 'POST':
            confirm = request.forms.get('confirm', 'no')
            return confirm == 'yes'

    @app.route('/token', ['POST'])
    @oauth.token_handler
    def handle_token(): pass

    @app.route('/revoke', ['POST'])
    @oauth.revoke_handler
    def revoke_token(): pass

    return app
