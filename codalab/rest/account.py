"""
Login and signup views.
Handles create new user accounts and authenticating users.
"""
from bottle import request, response, template, local, redirect, default_app, get, post

from codalab.lib import spec_util
from codalab.lib.server_util import redirect_with_query
from codalab.objects.user import User
from codalab.common import UsageError
from codalab.server.authenticated_plugin import AuthenticatedPlugin, UserVerifiedPlugin
from codalab.server.cookie import LoginCookie


def send_verification_key(username, email, key):
    # Send verification key to given email address
    hostname = request.get_header('Host')
    local.emailer.send_email(
        subject="Verify your new CodaLab account",
        body=template('email_verification_body', user=username, current_site=hostname, key=key),
        recipient=email,
    )


@get('/account/logout', name='logout', skip=UserVerifiedPlugin)
def do_logout():
    LoginCookie.clear()
    redirect_uri = request.query.get('redirect_uri')
    return redirect(redirect_uri)


@post('/account/login')
def do_login():
    success_uri = request.forms.get('success_uri')
    error_uri = request.forms.get('error_uri')
    username = request.forms.get('username')
    password = request.forms.get('password')

    user = local.model.get_user(username=username)
    if not (user and user.check_password(password)):
        return redirect_with_query(error_uri, {
            "error": "Login/password did not match.",
            "next": success_uri,
        })

    # Save cookie in client
    cookie = LoginCookie(user.user_id, max_age=30 * 24 * 60 * 60)
    cookie.save()

    # Redirect client to next page
    if success_uri:
        return redirect(success_uri)
    else:
        return redirect('/')


@post('/account/signup')
def do_signup():
    if request.user:
        return redirect(default_app().get_url('success', message="You are already logged into your account."))

    success_uri = request.forms.get('success_uri')
    error_uri = request.forms.get('error_uri')
    username = request.forms.get('username')
    password = request.forms.get('password')
    email = request.forms.get('email')

    errors = []
    if request.forms.get('confirm_password') != password:
        errors.append("Passwords do not match.")

    if not spec_util.NAME_REGEX.match(username):
        errors.append("Username must only contain letter, digits, hyphens, underscores, and periods.")

    try:
        User.validate_password(password)
    except UsageError as e:
        errors.append(e.message)

    # Only do a basic validation of email -- the only guaranteed way to check
    # whether an email address is valid is by sending an actual email.
    if not spec_util.BASIC_EMAIL_REGEX.match(email):
        errors.append("Email address is invalid.")

    if local.model.user_exists(username, email):
        errors.append("User with this username or email already exists.")

    if errors:
        return redirect_with_query(error_uri, {
            "error": " ".join(errors),
            "next": success_uri,
            "email": email,
            "username": username,
        })

    # Create unverified user
    _, verification_key = local.model.add_user(username, email, password)

    # Send key
    send_verification_key(username, email, verification_key)

    # Redirect to success page
    return redirect_with_query(success_uri, {
        "email": email
    })


@get('/account/verify/<key>', skip=UserVerifiedPlugin)
def do_verify(key):
    if local.model.verify_user(key):
        return redirect('/account/verify/success')
    else:
        return redirect('/account/verify/error')


@get('/account/resend', name='resend_key', skip=UserVerifiedPlugin)
def resend_key():
    if request.user.is_verified:
        return redirect('/account/verify/success')
    key = local.model.get_verification_key(request.user.user_id)
    send_verification_key(request.user.user_name, request.user.email, key)
    return redirect_with_query('/account/signup/success', {
        "email": request.user.email,
    })


@get('/account/whoami', apply=AuthenticatedPlugin(), skip=UserVerifiedPlugin)
def whoami():
    info = request.user.to_dict()
    del info['password']
    return info


@get('/account/css', skip=UserVerifiedPlugin)
def css():
    response.content_type = 'text/css'
    if request.user is None:
        return template('user_not_authenticated_css')
    else:
        return template('user_authenticated_css', username=request.user.user_name)
