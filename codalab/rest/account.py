"""
Login and signup views.
Handles create new user accounts and authenticating users.
"""


from bottle import request, response, template, local, redirect, default_app, get, post

from codalab.lib import spec_util
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

    # Redirect to success page
    return redirect(default_app().get_url(
        'success_no_verify',
        message="Thank you for signing up for a CodaLab account! "
                "A link to verify your account has been sent to %s." % email)
    )


@get('/account/logout', name='logout', skip=UserVerifiedPlugin)
def do_logout():
    LoginCookie.clear()
    redirect_uri = request.query.get('redirect_uri')
    if redirect_uri:
        return redirect(redirect_uri)
    else:
        return redirect(default_app().get_url('login'))


@get('/account/login', name='login')
def show_login():
    if request.user:
        return redirect(default_app().get_url('success', message="You are already signed into CodaLab."))
    return template("login")


@post('/account/login')
def do_login():
    redirect_uri = request.query.get('redirect_uri')
    username = request.forms.get('username')
    password = request.forms.get('password')

    user = local.model.get_user(username=username)
    if not (user and user.check_password(password)):
        return template("login", errors=["Login/password did not match."])

    # Save cookie in client
    cookie = LoginCookie(user.user_id, max_age=3600)
    cookie.save()

    # Redirect client to next page
    if redirect_uri:
        return redirect(redirect_uri)
    else:
        return redirect(default_app().get_url('success', message="Successfully signed into CodaLab."))


@get('/account/success', name='success')
@get('/account/success_no_verify', name='success_no_verify', skip=UserVerifiedPlugin)
def show_success():
    title = request.query.get('title', '') or request.params.get('title', '') or 'Success!'
    message = request.query.get('message', '') or request.params.get('message', '')
    return template('success', message=message, title=title)


@get('/account/signup', name='signup')
def show_signup():
    if request.user:
        return redirect(default_app().get_url('success', message="You are already logged into your account."))

    return template('signup')


@post('/account/signup')
def do_signup():
    if request.user:
        return redirect(default_app().get_url('success', message="You are already logged into your account."))

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
        return template('signup', errors=errors)

    # Create unverified user
    _, verification_key = local.model.add_user(username, email, password)

    return send_verification_key(username, email, verification_key)


@get('/account/verify/<key>', skip=UserVerifiedPlugin)
def do_verify(key):
    if local.model.verify_user(key):
        return redirect(default_app().get_url('success', message="Account verified!"))
    else:
        return "Invalid or expired verification key."


@get('/account/resend', name='resend_key', skip=UserVerifiedPlugin)
def resend_key():
    if request.user.is_verified:
        return redirect(default_app().get_url('success', message="Your account has already been verified."))
    key = local.model.get_verification_key(request.user.user_id)
    return send_verification_key(request.user.user_name, request.user.email, key)


@get('/account/whoami', apply=AuthenticatedPlugin(), skip=UserVerifiedPlugin)
def whoami():
    info = request.user.to_dict()
    del info['password']
    return info

