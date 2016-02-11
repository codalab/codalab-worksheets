"""
Bottle app for the OAuth2 authorization and token endpoints.
"""
from bottle import Bottle

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
    oauth2_token,
    oauth2_auth_code,
)

from codalab.server.oauth2_provider import OAuth2Provider


def OAuth2App(engine):
    app = Bottle()

    oauth = OAuth2Provider(app)

    @oauth.clientgetter
    def get_client(client_id):
        with engine.begin() as connection:
            return connection.execute(select([
                oauth2_client
            ]).where(
                oauth2_client.c.id == client_id
            ).limit(1)).fetchone()

    @oauth.grantgetter
    def get_grant(client_id, code):
        with engine.begin() as connection:
            return connection.execute(select([
                oauth2_auth_code
            ]).where(
                and_(oauth2_auth_code.c.client_id == client_id, oauth2_auth_code.c.code == code)
            ).limit(1)).fetchone()


    @app.route('/authorize')
    @oauth.authorize_handler
    def authorize():
        return 'Hello World'

    return app
