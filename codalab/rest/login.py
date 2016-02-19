"""
Login views.
"""
from urllib import urlencode

from bottle import request, response, template, local, redirect, route, default_app, get, post


@get('/logout', name='logout')
def do_logout():
    response.delete_cookie("user_id")
    redirect_uri = request.query.get('redirect_uri')
    if redirect_uri:
        # FIXME Does this need to be safer?
        return redirect(redirect_uri)
    else:
        return "<p>Successfully signed out from CodaLab.</p>"


@get('/login', name='login')
def show_login():
    return template("login", error=None)


@post('/login')
def do_login():
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


# The other way to do this is to write a Plugin and add it to the "apply" param to the authorize view function
def require_login(callback):
    def wrapper(*args, **kwargs):
        """Check that user is defined on session cookie"""
        user_id = request.get_cookie("user_id", secret='some-secret-key')
        if user_id:
            local.user = local.model.get_user(user_id=user_id)
        else:
            # Make sure X-Forwarded-Host is set properly if behind reverse-proxy to use request.url
            redirect("%s?%s" % ('/login', urlencode({"redirect_uri": request.url})))

        return callback(*args, **kwargs)

    return wrapper
