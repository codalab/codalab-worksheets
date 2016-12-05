# Oauth2 API

&larr; [Back to Table of Contents](index.md)
## `GET /oauth2/authorize`

&#039;authorize&#039; endpoint for OAuth2 authorization code flow.

## `POST /oauth2/authorize`

&#039;authorize&#039; endpoint for OAuth2 authorization code flow.

## `POST /oauth2/token`
## `POST /oauth2/revoke`
Provide secure services using OAuth2.
    The server should provide an authorize handler and a token handler,

    But before the handlers are implemented, the server should provide
    some getters for the validation.
    There are two usage modes. One is binding the Bottle app instance:

        app = Bottle()
        oauth = OAuth2Provider(app)

    The second possibility is to bind the Bottle app later:

        oauth = OAuth2Provider()

        def create_app():
            app = Bottle()
            oauth.app = app
            return app

    Configure :meth:`tokengetter` and :meth:`tokensetter` to get and
    set tokens. Configure :meth:`grantgetter` and :meth:`grantsetter`
    to get and set grant tokens. Configure :meth:`clientgetter` to
    get the client.

    Configure :meth:`usergetter` if you need password credential
    authorization.

    With everything ready, implement the authorization workflow:

        * :meth:`authorize_handler` for consumer to confirm the grant
        * :meth:`token_handler` for client to exchange access token

    And now you can protect the resource with scopes::

        @app.route(&#039;/api/user&#039;)
        @oauth.check_oauth(&#039;email&#039;, &#039;username&#039;)
        def user():
            return jsonify(request.user)

## `GET /oauth2/errors`

&larr; [Back to Table of Contents](index.md)
