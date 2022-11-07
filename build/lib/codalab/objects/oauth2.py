"""
ORM-style objects for the OAuth2 Provider.

Unfortunately, using the ORMObject is too limiting at the moment.
It clobbers DateTimes, and doesn't allow saving column values to different attribute names.

They must implement the interfaces defined here:
https://flask-oauthlib.readthedocs.org/en/latest/oauth2.html
"""
from codalab.common import UsageError


class OAuth2Client(object):
    def __init__(self, model, **kwargs):
        self.model = model
        self.id = kwargs['id'] if 'id' in kwargs else None
        try:
            self.client_id = kwargs['client_id']
            self.client_secret = kwargs['secret']
            self.name = kwargs['name']
            self.user_id = kwargs['user_id']
            self.grant_type = kwargs['grant_type']
            self.response_type = kwargs['response_type']
            self.redirect_uris = (
                kwargs['redirect_uris'].split(',') if kwargs['redirect_uris'] else []
            )
            self.default_scopes = kwargs['scopes'].split(',') if kwargs['scopes'] else []
        except KeyError as e:
            raise UsageError("Missing column %r" % e.args[0])

        self.default_redirect_uri = self.redirect_uris[0] if len(self.redirect_uris) > 0 else None

    @property
    def allowed_grant_types(self):
        return [self.grant_type, "refresh_token"]

    @property
    def allowed_response_types(self):
        return [self.response_type]

    @property
    def client_type(self):
        # Assume all clients are public for now
        return 'public'

    @property
    def user(self):
        return self.model.get_user(user_id=self.user_id)

    @property
    def columns(self):
        return {
            'client_id': self.client_id,
            'secret': self.client_secret,
            'name': self.name,
            'user_id': self.user_id,
            'grant_type': self.grant_type,
            'response_type': self.response_type,
            'redirect_uris': ','.join(self.redirect_uris),
            'scopes': ','.join(self.default_scopes),
        }


class OAuth2AuthCode(object):
    """
    This does not have to be stored in the database, and can be stored in some ephemeral cache in the future.
    """

    def __init__(self, model, **kwargs):
        self.model = model
        self.id = kwargs['id'] if 'id' in kwargs else None
        try:
            self.client_id = kwargs['client_id']
            self.code = kwargs['code']
            self.user_id = kwargs['user_id']
            self.scopes = kwargs['scopes'].split(',') if kwargs['scopes'] else []
            self.expires = kwargs['expires']
            self.redirect_uri = kwargs['redirect_uri']
        except KeyError as e:
            raise UsageError("Missing column %r" % e.args[0])

    @property
    def user(self):
        return self.model.get_user(self.user_id)

    def delete(self):
        self.model.delete_oauth2_auth_code(self.id)

    @property
    def columns(self):
        return {
            'client_id': self.client_id,
            'code': self.code,
            'user_id': self.user_id,
            'scopes': ','.join(self.scopes),
            'expires': self.expires,
            'redirect_uri': self.redirect_uri,
        }


class OAuth2Token(object):
    def __init__(self, model, **kwargs):
        self.model = model
        self.token_type = 'Bearer'
        self.id = kwargs['id'] if 'id' in kwargs else None
        try:
            self.client_id = kwargs['client_id']
            self.user_id = kwargs['user_id']
            self.scopes = kwargs['scopes'].split(',') if kwargs['scopes'] else []
            self.access_token = kwargs['access_token']
            self.refresh_token = kwargs['refresh_token']
            self.expires = kwargs['expires']
        except KeyError as e:
            raise UsageError("Missing column %r" % e.args[0])

    @property
    def user(self):
        return self.model.get_user(user_id=self.user_id)

    def delete(self):
        self.model.delete_oauth2_token(self.id)

    @property
    def columns(self):
        return {
            'client_id': self.client_id,
            'user_id': self.user_id,
            'access_token': self.access_token,
            'refresh_token': self.refresh_token,
            'scopes': ','.join(self.scopes),
            'expires': self.expires,
        }
