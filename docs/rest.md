# REST API Reference

This reference and the REST API itself is still under heavy development and is
subject to change at any time. Feedback through our GitHub issues is appreciated!

## Table of Contents
- [Resource Object Schemas](#resource-object-schemas)
- [API Endpoints](#api-endpoints)
  - [Bundle Permissions API](#bundle-permissions-api)
  - [Oauth2 API](#oauth2-api)
  - [Users API](#users-api)
  - [Worksheet Items API](#worksheet-items-api)
  - [Workers API](#workers-api)
  - [Bundles API](#bundles-api)
  - [User API](#user-api)
  - [Groups API](#groups-api)
  - [Bundle Actions API](#bundle-actions-api)
  - [Worksheets API](#worksheets-api)
  - [Worksheet Permissions API](#worksheet-permissions-api)

# Resource Object Schemas
## worksheet-items


Name | Type
--- | ---
    `subworksheet` | Relationship
    `sort_key` | Integer
    `worksheet` | Relationship
    `bundle` | Relationship
    `value` | String
    `type` | String
    `id` | Integer
## users


Name | Type
--- | ---
    `first_name` | String
    `last_name` | String
    `time_quota` | Integer
    `url` | Url
    `disk_used` | Integer
    `time_used` | Integer
    `email` | String
    `disk_quota` | Integer
    `affiliation` | String
    `last_login` | LocalDateTime
    `user_name` | String
    `id` | String
    `date_joined` | LocalDateTime
## bundles


Name | Type
--- | ---
    `host_worksheets` | List
    `data_hash` | String
    `uuid` | String
    `permission` | Integer
    `group_permissions` | Relationship
    `args` | String
    `id` | String
    `state` | String
    `dependencies` | BundleDependencySchema
    `command` | String
    `owner` | Relationship
    `bundle_type` | String
    `children` | Relationship
    `permission_spec` | PermissionSpec
    `metadata` | Dict
## worksheet-permissions


Name | Type
--- | ---
    `group` | Relationship
    `permission` | Integer
    `worksheet` | Relationship
    `group_name` | String
    `id` | Integer
    `permission_spec` | PermissionSpec
## bundle-permissions


Name | Type
--- | ---
    `group` | Relationship
    `permission` | Integer
    `bundle` | Relationship
    `group_name` | String
    `id` | Integer
    `permission_spec` | PermissionSpec
## BundleDependencySchema


Plain (non-JSONAPI) Marshmallow schema for a single bundle dependency.
Not defining this as a separate resource with Relationships because we only
create a set of dependencies once at bundle creation.


Name | Type
--- | ---
    `parent_name` | Method
    `child_uuid` | String
    `parent_uuid` | String
    `child_path` | String
    `parent_path` | String
## bundle-actions


Name | Type
--- | ---
    `type` | String
    `uuid` | String
    `subpath` | String
    `string` | String
    `id` | Integer
## worksheets


Name | Type
--- | ---
    `name` | String
    `last_item_id` | Integer
    `tags` | List
    `frozen` | DateTime
    `group_permissions` | Relationship
    `title` | String
    `items` | Relationship
    `owner` | Relationship
    `permission` | Integer
    `id` | String
    `permission_spec` | PermissionSpec
    `uuid` | String
## groups


Name | Type
--- | ---
    `name` | String
    `user_defined` | Boolean
    `admins` | Relationship
    `members` | Relationship
    `owner` | Relationship
    `id` | String
## users


Name | Type
--- | ---
    `first_name` | String
    `last_name` | String
    `url` | Url
    `affiliation` | String
    `user_name` | String
    `id` | String
    `date_joined` | LocalDateTime

&uarr; [Back to Top](#table-of-contents)
# API Endpoints
## Bundle Permissions API
### `POST /bundle-permissions`

Bulk set bundle permissions.


&uarr; [Back to Top](#table-of-contents)
## Oauth2 API
### `GET /oauth2/authorize`

&#039;authorize&#039; endpoint for OAuth2 authorization code flow.

### `POST /oauth2/authorize`

&#039;authorize&#039; endpoint for OAuth2 authorization code flow.

### `POST /oauth2/token`
### `POST /oauth2/revoke`
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

### `GET /oauth2/errors`

&uarr; [Back to Top](#table-of-contents)
## Users API
### `GET /users/<user_spec>`
Fetch a single user.
### `GET /users`

Fetch list of users, filterable by username and email.

Takes the following query parameters:
    filter[user_name]=name1,name2,...
    filter[email]=email1,email2,...

Fetches all users that match any of these usernames or emails.

### `PATCH /users`

Update arbitrary users.

This operation is reserved for the root user. Other users can update their
information through the /user &quot;authenticated user&quot; API.
Follows the bulk-update convention in the CodaLab API, but currently only
allows one update at a time.


&uarr; [Back to Top](#table-of-contents)
## Worksheet Items API
### `POST /worksheet-items`

Bulk add worksheet items.

|replace| - Replace existing items in host worksheets. Default is False.


&uarr; [Back to Top](#table-of-contents)
## Workers API
### `POST /workers/<worker_id>/checkin`

Checks in with the bundle service, storing information about the worker.
Waits for a message for the worker for WAIT_TIME_SECS seconds. Returns the
message or None if there isn&#039;t one.

### `POST /workers/<worker_id>/checkout`

Checks out from the bundle service, cleaning up any state related to the
worker.

### `POST /workers/<worker_id>/reply/<socket_id:int>`

Replies with a single JSON message to the given socket ID.

### `POST /workers/<worker_id>/reply_data/<socket_id:int>`

Replies with a stream of data to the given socket ID. This reply mechanism
works through 2 messages sent by this method: the first message is a header
message containing metadata. The second message streams the actual data in.

The contents of the first message are parsed from the header_message query
parameter, which should be in JSON format.

The contents of the second message go in the body of the HTTP request.

### `POST /workers/<worker_id>/start_bundle/<uuid:re:0x[0-9a-f]{32}>`

Checks whether the bundle is still assigned to run on the worker with the
given ID. If so, reports that it&#039;s starting to run and returns True.
Otherwise, returns False, meaning the worker shouldn&#039;t run the bundle.

### `PUT /workers/<worker_id>/update_bundle_metadata/<uuid:re:0x[0-9a-f]{32}>`

Updates metadata related to a running bundle.

### `POST /workers/<worker_id>/finalize_bundle/<uuid:re:0x[0-9a-f]{32}>`

Reports that the bundle has finished running.

### `GET /workers/code.tar.gz`

Returns .tar.gz archive containing the code of the worker.


&uarr; [Back to Top](#table-of-contents)
## Bundles API
### `GET /bundles/<uuid:re:0x[0-9a-f]{32}>`
### `GET /bundles`

Fetch bundles by bundle specs OR search keywords.

### `POST /bundles`

Bulk create bundles.

|worksheet_uuid| - The parent worksheet of the bundle, add to this worksheet
                   if not detached or shadowing another bundle. Also used
                   to inherit permissions.
|shadow| - the uuid of the bundle to shadow
|detached| - True (&#039;1&#039;) if should not add new bundle to any worksheet,
             or False (&#039;0&#039;) otherwise. Default is False.

### `PATCH /bundles`

Bulk update bundles.

### `DELETE /bundles`

Delete the bundles specified.
If |force|, allow deletion of bundles that have descendants or that appear across multiple worksheets.
If |recursive|, add all bundles downstream too.
If |data-only|, only remove from the bundle store, not the bundle metadata.
If |dry-run|, just return list of bundles that would be deleted, but do not actually delete.

### `GET /bundles/<uuid:re:0x[0-9a-f]{32}>/contents/info/<path:path>`
### `GET /bundles/<uuid:re:0x[0-9a-f]{32}>/contents/info/`
### `GET /bundles/<uuid:re:0x[0-9a-f]{32}>/contents/blob/<path:path>`

API to download the contents of a bundle or a subpath within a bundle.

For directories this method always returns a tarred and gzipped archive of
the directory.

For files, if the request has an Accept-Encoding header containing gzip,
then the returned file is gzipped.

### `GET /bundles/<uuid:re:0x[0-9a-f]{32}>/contents/blob/`

API to download the contents of a bundle or a subpath within a bundle.

For directories this method always returns a tarred and gzipped archive of
the directory.

For files, if the request has an Accept-Encoding header containing gzip,
then the returned file is gzipped.

### `PUT /bundles/<uuid:re:0x[0-9a-f]{32}>/contents/blob/`

Update the contents of the given running or uploading bundle.

Query parameters:
    urls - comma-separated list of URLs from which to fetch data to fill the
           bundle, using this option will ignore any uploaded file data
    git - (optional) 1 if URL should be interpreted as git repos to clone
          or 0 otherwise, default is 0
OR
    filename - (optional) filename of the uploaded file, used to indicate
               whether or not it is an archive, default is &#039;contents&#039;

Query parameters that are always available:
    unpack - (optional) 1 if the uploaded file should be unpacked if it is
             an archive, or 0 otherwise, default is 1
    simplify - (optional) 1 if the uploaded file should be &#039;simplified&#039; if
               it is an archive, or 0 otherwise, default is 1
               (See UploadManager for full explanation of &#039;simplification&#039;)
    finalize - (optional) 1 if this should be considered the final version
               of the bundle contents and thus mark the bundle as &#039;ready&#039;
               when upload is complete and &#039;failed&#039; if upload fails, or 0 if
               should allow future updates, default is 0


&uarr; [Back to Top](#table-of-contents)
## User API
### `GET /user`
Fetch authenticated user.
### `PATCH /user`
Update one or multiple fields of the authenticated user.

&uarr; [Back to Top](#table-of-contents)
## Groups API
### `GET /groups/<group_spec>`
Fetch a single group.
### `GET /groups`
Fetch list of groups readable by the authenticated user.
### `DELETE /groups`
Delete groups.
### `POST /groups`
Create a group.
### `POST /groups/<group_spec>/relationships/admins`
### `POST /groups/<group_spec>/relationships/members`
### `DELETE /groups/<group_spec>/relationships/members`
### `DELETE /groups/<group_spec>/relationships/admins`

&uarr; [Back to Top](#table-of-contents)
## Bundle Actions API
### `POST /bundle-actions`

Sends the message to the worker to do the bundle action, and adds the
action string to the bundle metadata.


&uarr; [Back to Top](#table-of-contents)
## Worksheets API
### `GET /worksheets/<uuid:re:0x[0-9a-f]{32}>`
### `GET /worksheets`

Fetch bundles by bundle specs OR search keywords.

### `POST /worksheets`
### `POST /worksheets/<uuid:re:0x[0-9a-f]{32}>/raw`
### `PATCH /worksheets`

Bulk update worksheets metadata.

### `DELETE /worksheets`

Delete the bundles specified.
If |force|, allow deletion of bundles that have descendants or that appear across multiple worksheets.
If |recursive|, add all bundles downstream too.
If |data-only|, only remove from the bundle store, not the bundle metadata.
If |dry-run|, just return list of bundles that would be deleted, but do not actually delete.

### `GET /worksheets/sample/`

Get worksheets to display on the front page.
Keep only |worksheet_uuids|.

### `GET /worksheets/`

&uarr; [Back to Top](#table-of-contents)
## Worksheet Permissions API
### `POST /worksheet-permissions`

Bulk set worksheet permissions.


&uarr; [Back to Top](#table-of-contents)
