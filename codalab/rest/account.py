"""
Login and signup views.
Handles create new user accounts and authenticating users.
"""
from bottle import request, response, template, local, redirect, default_app, get, post

from codalab.lib import crypt_util, spec_util
from codalab.lib.server_util import redirect_with_query
from codalab.lib.spec_util import NAME_REGEX
from codalab.common import UsageError
from codalab.objects.user import User
from codalab.server.authenticated_plugin import AuthenticatedPlugin, UserVerifiedPlugin
from codalab.server.cookie import LoginCookie


def send_verification_key(username, email, key):
    # Send verification key to given email address
    hostname = request.get_header('X-Forwarded-Host') or request.get_header('Host')
    scheme = request.get_header('X-Forwarded-Proto')
    local.emailer.send_email(
        subject="Verify your CodaLab account",
        body=template(
            'email_verification_body', user=username, scheme=scheme, hostname=hostname, key=key
        ),
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
        return redirect_with_query(
            error_uri, {"error": "Login/password did not match.", "next": success_uri}
        )

    # Update last login
    local.model.update_user_last_login(user.user_id)

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
    if request.user.is_authenticated:
        return redirect(
            default_app().get_url('success', message="You are already logged into your account.")
        )

    success_uri = request.forms.get('success_uri')
    error_uri = request.forms.get('error_uri')
    username = request.forms.get('username')
    email = request.forms.get('email')
    first_name = request.forms.get('first_name')
    last_name = request.forms.get('last_name')
    password = request.forms.get('password')
    affiliation = request.forms.get('affiliation')

    errors = []
    if request.user.is_authenticated:
        errors.append(
            "You are already logged in as %s, please log out before "
            "creating a new account." % request.user.user_name
        )

    if request.forms.get('confirm_password') != password:
        errors.append("Passwords do not match.")

    if not spec_util.NAME_REGEX.match(username):
        errors.append(
            "Username must only contain letter, digits, hyphens, underscores, and periods."
        )

    try:
        User.validate_password(password)
    except UsageError as e:
        errors.append(str(e))

    # Only do a basic validation of email -- the only guaranteed way to check
    # whether an email address is valid is by sending an actual email.
    if not spec_util.BASIC_EMAIL_REGEX.match(email):
        errors.append("Email address is invalid.")

    if local.model.user_exists(username, email):
        errors.append("User with this username or email already exists.")

    if not NAME_REGEX.match(username):
        errors.append("Username characters must be alphanumeric, underscores, periods, or dashes.")

    if errors:
        return redirect_with_query(
            error_uri,
            {
                'error': ' '.join(errors),
                'next': success_uri,
                'email': email,
                'username': username,
                'first_name': first_name,
                'last_name': last_name,
                'affiliation': affiliation,
            },
        )

    # If user leaves it blank, empty string is obtained - make it of NoneType.
    if not affiliation:
        affiliation = None

    # Create unverified user
    _, verification_key = local.model.add_user(
        username, email, first_name, last_name, password, affiliation
    )

    # Send key
    send_verification_key(username, email, verification_key)

    # Redirect to success page
    return redirect_with_query(success_uri, {'email': email})


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
    return redirect_with_query('/account/signup/success', {'email': request.user.email})


@get('/account/css', skip=UserVerifiedPlugin)
def css():
    response.content_type = 'text/css'
    if request.user.is_authenticated:
        return template('user_authenticated_css', username=request.user.user_name)
    else:
        return template('user_not_authenticated_css')


@get('/account/reset', apply=AuthenticatedPlugin())
def request_reset_get():
    """
    Password reset endpoint for authenticated users.
    """
    # Generate reset code
    reset_code = local.model.new_user_reset_code(request.user.user_id)

    # Send code
    hostname = request.get_header('X-Forwarded-Host') or request.get_header('Host')
    scheme = request.get_header('X-Forwarded-Proto')
    user_name = request.user.first_name or request.user.user_name
    local.emailer.send_email(
        subject="CodaLab password reset link",
        body=template(
            'password_reset_body', user=user_name, scheme=scheme, hostname=hostname, code=reset_code
        ),
        recipient=request.user.email,
    )

    # Redirect to success page
    return redirect('/account/reset/sent')


@post('/account/reset')
def request_reset_post():
    """
    Password reset form POST endpoint.
    """
    email = request.forms.get('email')
    user = local.model.get_user(username=email)
    if user is None:
        # Redirect back to form page
        return redirect_with_query(
            '/account/reset', {'error': "User with email %s not found." % email}
        )

    # Generate reset code
    reset_code = local.model.new_user_reset_code(user.user_id)

    # Send code
    hostname = request.get_header('X-Forwarded-Host') or request.get_header('Host')
    scheme = request.get_header('X-Forwarded-Proto')
    user_name = user.first_name or user.user_name
    local.emailer.send_email(
        subject="CodaLab password reset link",
        body=template(
            'password_reset_body', user=user_name, scheme=scheme, hostname=hostname, code=reset_code
        ),
        recipient=email,
    )

    # Redirect to success page
    return redirect('/account/reset/sent')


@get('/account/reset/verify/<code>')
def verify_reset_code(code):
    """
    Target endpoint for password reset code links.
    Does an initial verification of the reset code and redirects to the
    frontend page with the appropriate parameters.
    """
    if local.model.get_reset_code_user_id(code, delete=False) is not None:
        redirect_with_query('/account/reset/verified', {'code_valid': True, 'code': code})
    else:
        redirect_with_query('/account/reset/verified', {'code_valid': False})


@post('/account/reset/finalize')
def reset_password():
    """
    Final password reset form POST endpoint.
    """
    code = request.forms.get('code')
    password = request.forms.get('password')
    confirm_password = request.forms.get('confirm_password')

    # Validate password
    if confirm_password != password:
        return redirect_with_query(
            '/account/reset/verified',
            {'code_valid': True, 'code': code, 'error': "Passwords do not match."},
        )
    try:
        User.validate_password(password)
    except UsageError as e:
        return redirect_with_query(
            '/account/reset/verified', {'code_valid': True, 'code': code, 'error': str(e)}
        )

    # Verify reset code again and get user_id
    user_id = local.model.get_reset_code_user_id(code, delete=True)
    if user_id is None:
        return redirect_with_query('/account/reset/verified', {'code_valid': False})

    # Update user password
    user_info = local.model.get_user_info(user_id)
    user_info['password'] = (User.encode_password(password, crypt_util.get_random_string()),)
    local.model.update_user_info(user_info)

    return redirect('/account/reset/complete')


@post('/account/changeemail', apply=AuthenticatedPlugin(), skip=UserVerifiedPlugin)
def request_change_email():
    """
    Email change form POST endpoint.
    """
    email = request.forms.get('email').strip()

    if email == request.user.email:
        return redirect_with_query(
            '/account/changeemail', {'error': "Your email address is already %s." % email}
        )

    if not spec_util.BASIC_EMAIL_REGEX.match(email):
        return redirect_with_query('/account/changeemail', {'error': "Invalid email address."})

    if local.model.user_exists(None, email):
        return redirect_with_query(
            '/account/changeemail', {'error': "User with this email already exists."}
        )

    local.model.update_user_info(
        {'user_id': request.user.user_id, 'email': email, 'is_verified': False}
    )

    key = local.model.get_verification_key(request.user.user_id)
    send_verification_key(request.user.user_name, request.user.email, key)

    return redirect('/account/changeemail/sent')
