"""
Bottle app for the OAuth2 authorization and token endpoints.
"""
from datetime import datetime, timedelta

from bottle import Bottle, request, template, local

from codalab.objects.oauth2 import OAuth2AuthCode, OAuth2Token
from codalab.rest.login import require_login
from codalab.server.oauth2_provider import OAuth2Provider

oauth2_app = Bottle()
oauth2_provider = OAuth2Provider(oauth2_app)


@oauth2_provider.clientgetter
def get_client(client_id):
    return local.model.get_oauth2_client(client_id)


@oauth2_provider.grantgetter
def get_grant(client_id, code):
    return local.model.get_oauth2_auth_code(client_id, code)


@oauth2_provider.grantsetter
def set_grant(client_id, code, _request, *args, **kwargs):
    # Grant expires in 100 seconds
    expires = datetime.utcnow() + timedelta(seconds=100)
    grant = OAuth2AuthCode(
        local.model,
        client_id=client_id,
        code=code['code'],
        redirect_uri=_request.redirect_uri,
        scopes=','.join(_request.scopes),
        user_id=1337,  # FIXME: get current authenticated user
        expires=expires
    )
    return local.model.save_oauth2_auth_code(grant)


@oauth2_provider.tokengetter
def get_token(access_token=None, refresh_token=None):
    return local.model.get_oauth2_token(access_token, refresh_token)


@oauth2_provider.tokensetter
def set_token(token, _request, *args, **kwargs):
    # _request.user only available for "password" grant types,
    # while local.user is available on views with @require_login,
    # i.e. the authorize view
    user = _request.user or local.user

    # Make sure that every client has only one token connected to a user
    local.model.clear_oauth2_tokens(_request.client.client_id, user.user_id)

    expires_in = token.get('expires_in')
    expires = datetime.utcnow() + timedelta(seconds=expires_in)

    token = OAuth2Token(
        local.model,
        access_token=token['access_token'],
        refresh_token=token.get('refresh_token', None),
        scopes=token['scope'],
        expires=expires,
        client_id=_request.client.client_id,
        user_id=user.user_id,
    )

    return local.model.save_oauth2_token(token)


@oauth2_provider.usergetter
def get_user(username, password, *args, **kwargs):
    user = local.model.get_user(username=username)
    if user.check_password(password):
        return user
    return None


@oauth2_app.route('/authorize', ['GET', 'POST'])
@require_login
@oauth2_provider.authorize_handler
def authorize(*args, **kwargs):
    if request.method == 'GET':
        client_id = kwargs.get('client_id')
        redirect_uri = kwargs.get('redirect_uri')
        client = local.model.get_oauth2_client(client_id)
        return template("oauth2_authorize", client=client, redirect_uri=redirect_uri)
    elif request.method == 'POST':
        confirm = request.forms.get('confirm', 'no')
        return confirm == 'yes'


@oauth2_app.post('/token')
@oauth2_provider.token_handler
def handle_token(): pass


@oauth2_app.post('/revoke')
@oauth2_provider.revoke_handler
def revoke_token(): pass

