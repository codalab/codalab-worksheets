# Account API
&larr; [Back to Table of Contents](index.md)
# `GET /account/logout`
# `POST /account/login`
# `POST /account/signup`
# `GET /account/verify/<key>`
# `GET /account/resend`
# `GET /account/css`
# `GET /account/reset`

Password reset endpoint for authenticated users.

# `POST /account/reset`

Password reset form POST endpoint.

# `GET /account/reset/verify/<code>`

Target endpoint for password reset code links.
Does an initial verification of the reset code and redirects to the
frontend page with the appropriate parameters.

# `POST /account/reset/finalize`

Final password reset form POST endpoint.

# `POST /account/changeemail`

Email change form POST endpoint.

&larr; [Back to Table of Contents](index.md)
