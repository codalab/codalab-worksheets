# Users API
&larr; [Back to Table of Contents](index.md)
## `GET /users/<user_spec>`
Fetch a single user.
## `GET /users`

Fetch list of users, filterable by username and email.

Takes the following query parameters:
    filter[user_name]=name1,name2,...
    filter[email]=email1,email2,...

Fetches all users that match any of these usernames or emails.

## `PATCH /users`

Update arbitrary users.

This operation is reserved for the root user. Other users can update their
information through the /user &quot;authenticated user&quot; API.
Follows the bulk-update convention in the CodaLab API, but currently only
allows one update at a time.

&larr; [Back to Table of Contents](index.md)
