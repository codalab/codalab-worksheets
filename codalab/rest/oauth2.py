"""
Bottle app for the OAuth2 authorization and token endpoints.

This is where all of the pieces of the OAuth provider puzzle get put together.
We instantiate an OAuth2Provider, which is a generalized class that handles all
the OAuth 2.0 authorization flows, encapsulating them in a few simple function
decorators.

The xxx[getter|setter] decorators register callbacks which the OAuth2Provider
uses to get and set the necessary objects from our model.

The xxx_handler decorators wrap Bottle view functions to process the standard
OAuth 2.0 requests to our endpoints. In some cases, such as the 'token' endpoint,
the decorator handles all of the logic completely. In the case of the 'authorize'
endpoint, we have to implement some additional logic to generate our personalized
view and process our own form fields.

Note that the 'authorize' endpoint also uses our AuthenticatedPlugin, which
ensures that the user is signed in before allowing them to authorize a request.

The 'errors' endpoint is simply the default redirect destination when the
OAuth2Provider encounters an error during authorization, and it displays the
contents of the default query parameters 'error' and 'error_description'.
"""
from datetime import datetime, timedelta

from bottle import request, template, local, route, post, get, default_app

from codalab.objects.oauth2 import OAuth2AuthCode, OAuth2Token
from codalab.server.authenticated_plugin import AuthenticatedPlugin
from codalab.server.oauth2_provider import oauth2_provider


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
        user_id=request.user.user_id,
        expires=expires,
    )
    return local.model.save_oauth2_auth_code(grant)


@oauth2_provider.tokengetter
def get_token(access_token=None, refresh_token=None):
    return local.model.get_oauth2_token(access_token, refresh_token)


@oauth2_provider.tokensetter
def set_token(token, _request, *args, **kwargs):
    # _request.user only available for "password" grant types,
    # while request.user is available on views with @require_login,
    # i.e. the authorize view
    user = _request.user or request.user

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
    if user is not None and user.check_password(password):
        return user
    return None


# Not currently used by CodaLab website nor by the CLI client.
@route('/oauth2/authorize', ['GET', 'POST'], apply=AuthenticatedPlugin())
@oauth2_provider.authorize_handler
def authorize(*args, **kwargs):
    """
    'authorize' endpoint for OAuth2 authorization code flow.
    """
    if request.method == 'GET':
        client_id = kwargs.get('client_id')
        redirect_uri = kwargs.get('redirect_uri')
        client = local.model.get_oauth2_client(client_id)
        return template("oauth2_authorize", client=client, redirect_uri=redirect_uri)
    elif request.method == 'POST':
        # Return True back to the authorize_handler wrapper iff confirmed.
        confirm = request.forms.get('confirm', 'no')
        return confirm == 'yes'


@post('/oauth2/token')
@oauth2_provider.token_handler
def handle_token():
    """
    OAuth2 token endpoint.

    Access token request:

        grant_type
            REQUIRED.  Value MUST be set to "password".

        username
            REQUIRED.  The resource owner username.

        password
            REQUIRED.  The resource owner password.

        scope
            OPTIONAL.  The scope of the access request. (UNUSED)

    Example successful response:

        HTTP/1.1 200 OK
        Content-Type: application/json;charset=UTF-8
        Cache-Control: no-store
        Pragma: no-cache

        {
          "access_token":"2YotnFZFEjr1zCsicMWpAA",
          "token_type":"example",
          "expires_in":3600,
          "refresh_token":"tGzv3JOkF0XG5Qx2TlKWIA",
          "example_parameter":"example_value"
        }
    """
    pass


@post('/oauth2/revoke')
@oauth2_provider.revoke_handler
def revoke_token():
    """Revoke OAuth2 token."""
    pass


@get('/oauth2/errors', name='oauth2_errors')
def show_errors():
    return template('oauth2_errors', **request.query)


default_app().config['OAUTH2_PROVIDER_ERROR_ENDPOINT'] = 'oauth2_errors'
