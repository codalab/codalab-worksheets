# Skeleton for an OAuth 2 Web Application Server which is an OAuth
# provider configured for Authorization Code, Refresh Token grants and
# for dispensing Bearer Tokens.

from oauthlib.oauth2 import RequestValidator
from sqlalchemy import (
    and_,
    or_,
    not_,
    select,
    union,
    desc,
    func,
)
from sqlalchemy.sql.expression import (
    literal,
    true,
)

from codalab.model.tables import (
    user as cl_user,
    oauth2_client,
    oauth2_access_token,
    oauth2_refresh_token,
    oauth2_auth_code,
)


class OAuthDelegate(RequestValidator):
    """
    Maps various OAuth 2.0 validation and persistence methods to our SQL backing store.
    """

    def __init__(self, engine):
        self.engine = engine

    def validate_client_id(self, client_id, request, *args, **kwargs):
        # TODO
        # Not needed by Resource Owner Password Credentials Grant
        # Simple validity check, does client exist? Not banned?
        raise NotImplementedError

    def validate_redirect_uri(self, client_id, redirect_uri, request, *args, **kwargs):
        # TODO
        # Not needed by Resource Owner Password Credentials Grant
        # Is the client allowed to use the supplied redirect_uri? i.e. has
        # the client previously registered this EXACT redirect uri.
        raise NotImplementedError

    def get_default_redirect_uri(self, client_id, request, *args, **kwargs):
        # TODO
        # Not needed by Resource Owner Password Credentials Grant
        # The redirect used if none has been supplied.
        # Prefer your clients to pre register a redirect uri rather than
        # supplying one on each authorization request.
        raise NotImplementedError

    def validate_scopes(self, client_id, scopes, client, request, *args, **kwargs):
        # TODO
        # Not needed by Resource Owner Password Credentials Grant
        # Is the client allowed to access the requested scopes?
        raise NotImplementedError

    def get_default_scopes(self, client_id, request, *args, **kwargs):
        # Scopes a client will authorize for if none are supplied in the
        # authorization request.
        return []

    def validate_response_type(self, client_id, response_type, client, request, *args, **kwargs):
        # Clients should only be allowed to use one type of response type, the
        # one associated with their one allowed grant type.
        with self.engine.begin() as connection:
            client = connection.execute(select([
                oauth2_client.c.response_type
            ]).where(
                oauth2_client.c.id == client_id
            ).limit(1)).fetchone()

        if client is None:
            raise Exception("Client %s doesn't exist" % client_id)

        return client[oauth2_client.c.response_type] == response_type

    def validate_user(self, username, password, client, request, *args, **kwargs):
        """
        Ensure the username and password is valid.

        OBS! The validation should also set the user attribute of the request to a valid resource
        owner, i.e. request.user = username or similar. If not set you will be unable to associate
        a token with a user in the persistance method used (commonly, save_bearer_token).

        :param username: Unicode username
        :param password: Unicode password
        :param client: Client object set by you, see authenticate_client.
        :param request: The HTTP Request (oauthlib.common.Request)
        :return: True or False
        """
        pass

    # Post-authorization

    def save_authorization_code(self, client_id, code, request, *args, **kwargs):
        # TODO
        # Not needed by Resource Owner Password Credentials Grant
        # Remember to associate it with request.scopes, request.redirect_uri
        # request.client, request.state and request.user (the last is passed in
        # post_authorization credentials, i.e. { 'user': request.user}.
        raise NotImplementedError

    # Token request

    def authenticate_client(self, request, *args, **kwargs):
        # Whichever authentication method suits you, HTTP Basic might work
        pass

    def authenticate_client_id(self, client_id, request, *args, **kwargs):
        # TODO
        # Not needed by Resource Owner Password Credentials Grant
        # Don't allow public (non-authenticated) clients
        return False

    def validate_code(self, client_id, code, client, request, *args, **kwargs):
        # TODO
        # Not needed by Resource Owner Password Credentials Grant
        # Validate the code belongs to the client. Add associated scopes,
        # state and user to request.scopes and request.user.
        raise NotImplementedError

    def confirm_redirect_uri(self, client_id, code, redirect_uri, client, *args, **kwargs):
        # TODO
        # Not needed by Resource Owner Password Credentials Grant
        # You did save the redirect uri with the authorization code right?
        raise NotImplementedError

    def validate_grant_type(self, client_id, grant_type, client, request, *args, **kwargs):
        # Clients should only be allowed to use one type of grant.
        # In this case, it must be "authorization_code" or "refresh_token"
        pass

    def save_bearer_token(self, token, request, *args, **kwargs):
        # Remember to associate it with request.scopes, request.user and
        # request.client. The two former will be set when you validate
        # the authorization code. Don't forget to save both the
        # access_token and the refresh_token and set expiration for the
        # access_token to now + expires_in seconds.
        pass

    def invalidate_authorization_code(self, client_id, code, request, *args, **kwargs):
        # TODO
        # Not needed by Resource Owner Password Credentials Grant
        # Authorization codes are use once, invalidate it when a Bearer token
        # has been acquired.
        raise NotImplementedError

    # Protected resource request

    def validate_bearer_token(self, token, scopes, request):
        # Remember to check expiration and scope membership
        pass

    # Token refresh request

    def get_original_scopes(self, refresh_token, request, *args, **kwargs):
        # TODO
        # Not needed by Resource Owner Password Credentials Grant
        # Obtain the token associated with the given refresh_token and
        # return its scopes, these will be passed on to the refreshed
        # access token if the client did not specify a scope during the
        # request.
        raise NotImplementedError




