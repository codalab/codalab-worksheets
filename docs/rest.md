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
  - [Help API](#help-api)

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
    `notifications` | Integer
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

A bundle permission created on a bundle-group pair will replace any
existing permissions on the same bundle-group pair.


&uarr; [Back to Top](#table-of-contents)
## Oauth2 API
### `GET /oauth2/authorize`

'authorize' endpoint for OAuth2 authorization code flow.

### `POST /oauth2/authorize`

'authorize' endpoint for OAuth2 authorization code flow.

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

        @app.route('/api/user')
        @oauth.check_oauth('email', 'username')
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
information through the /user "authenticated user" API.
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
message or None if there isn't one.

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
given ID. If so, reports that it's starting to run and returns True.
Otherwise, returns False, meaning the worker shouldn't run the bundle.

### `PUT /workers/<worker_id>/update_bundle_metadata/<uuid:re:0x[0-9a-f]{32}>`

Updates metadata related to a running bundle.

### `POST /workers/<worker_id>/finalize_bundle/<uuid:re:0x[0-9a-f]{32}>`

Reports that the bundle has finished running.

### `GET /workers/code.tar.gz`

Returns .tar.gz archive containing the code of the worker.


&uarr; [Back to Top](#table-of-contents)
## Bundles API
### `GET /bundles/<uuid:re:0x[0-9a-f]{32}>`

Fetch bundle by UUID.

### `GET /bundles`

Fetch bundles by bundle `specs` OR search `keywords`. Behavior is undefined
when both `specs` and `keywords` are provided.

Query parameters:

 - `worksheet`: UUID of the base worksheet. Required when fetching by specs.
 - `specs`: Bundle spec of bundle to fetch. May be provided multiples times
    to fetch multiple bundle specs. A bundle spec is either:
    1. a UUID (8 or 32 hex characters with a preceding '0x')
    2. a bundle name referring to the last bundle with that name on the
       given base worksheet
    3. or a reverse index of the form `^N` referring to the Nth-to-last
       bundle on the given base worksheet.
 - `keywords`: Search keyword. May be provided multiples times for multiple
    keywords. Bare keywords match the names and descriptions of bundles.
    Examples of other special keyword forms:
    - `name=<name>            ` : More targeted search of using metadata fields.
    - `size=.sort             ` : Sort by a particular field.
    - `size=.sort-            ` : Sort by a particular field in reverse.
    - `size=.sum              ` : Compute total of a particular field.
    - `.mine                  ` : Match only bundles I own.
    - `.floating              ` : Match bundles that aren't on any worksheet.
    - `.count                 ` : Count the number of bundles.
    - `.limit=10              ` : Limit the number of results to the top 10.

When aggregation keywords such as `.count` are used, the resulting value
is returned as:
```
{
    "meta": {
        "results": <value>
    }
}
```


### `POST /bundles`

Bulk create bundles.

Query parameters:
- `worksheet`: UUID of the parent worksheet of the new bundle, add to
  this worksheet if not detached or shadowing another bundle. The new
  bundle also inherits permissions from this worksheet.
- `shadow`: UUID of the bundle to "shadow" (the new bundle will be added
  as an item immediately after this bundle in its parent worksheet).
- `detached`: 1 if should not add new bundle to any worksheet,
  or 0 otherwise. Default is 0.
- `wait_for_upload`: 1 if the bundle state should be initialized to
  "uploading" regardless of the bundle type, or 0 otherwise. Used when
  copying bundles from another CodaLab instance, this prevents these new
  bundles from being executed by the BundleManager. Default is 0.

### `PATCH /bundles`

Bulk update bundles.

### `DELETE /bundles`

Delete the bundles specified.

Query parameters:
 - `force`: 1 to allow deletion of bundles that have descendants or that
   appear across multiple worksheets, or 0 to throw an error if any of the
   specified bundles have multiple references. Default is 0.
 - `recursive`: 1 to remove all bundles downstream too, or 0 otherwise.
   Default is 0.
 - `data-only`: 1 to only remove contents of the bundle(s) from the bundle
   store and leave the bundle metadata intact, or 0 to remove both the
   bundle contents and the bundle metadata. Default is 0.
 - `dry-run`: 1 to just return list of bundles that would be deleted with
   the given parameters without actually deleting them, or 0 to perform
   the deletion. Default is 0.

### `GET /bundles/<uuid:re:0x[0-9a-f]{32}>/contents/info/<path:path>`

Fetch the bundle contents metadata.

The `<path>` refers to a path to a file or a directory inside the directory
structure of the bundle contents.

Query parameters:
- `depth`: recursively fetch subdirectory info up to this depth.
  Default is 0.

Response format:
```
{
  "data": {
      "name": "<name of file or directory>",
      "link": "<string representing target if file is a symbolic link>",
      "type": "<file|directory>",
      "size": <size of file in bytes>,
      "contents": {
        "name": ...,
        <contents of the directory represented recursively with the same schema>
      },
      "perm", <unix permission integer>
  }
}
```

### `GET /bundles/<uuid:re:0x[0-9a-f]{32}>/contents/info/`

Fetch the bundle contents metadata.

The `<path>` refers to a path to a file or a directory inside the directory
structure of the bundle contents.

Query parameters:
- `depth`: recursively fetch subdirectory info up to this depth.
  Default is 0.

Response format:
```
{
  "data": {
      "name": "<name of file or directory>",
      "link": "<string representing target if file is a symbolic link>",
      "type": "<file|directory>",
      "size": <size of file in bytes>,
      "contents": {
        "name": ...,
        <contents of the directory represented recursively with the same schema>
      },
      "perm", <unix permission integer>
  }
}
```

### `GET /bundles/<uuid:re:0x[0-9a-f]{32}>/contents/blob/<path:path>`

API to download the contents of a bundle or a subpath within a bundle.

For directories this method always returns a tarred and gzipped archive of
the directory.

For files, if the request has an Accept-Encoding header containing gzip,
then the returned file is gzipped.

HTTP headers:
- `Range: bytes=<start>-<end>`: fetch bytes from the range
  `[<start>, <end>)`.

Query parameters:
- `head`: number of lines to fetch from the beginning of the file.
  Default is 0, meaning to fetch the entire file.
- `tail`: number of lines to fetch from the end of the file.
  Default is 0, meaning to fetch the entire file.
- `max_line_length`: maximum number of characters to fetch from each line,
  if either `head` or `tail` is specified. Default is 128.
- 

### `GET /bundles/<uuid:re:0x[0-9a-f]{32}>/contents/blob/`

API to download the contents of a bundle or a subpath within a bundle.

For directories this method always returns a tarred and gzipped archive of
the directory.

For files, if the request has an Accept-Encoding header containing gzip,
then the returned file is gzipped.

HTTP headers:
- `Range: bytes=<start>-<end>`: fetch bytes from the range
  `[<start>, <end>)`.

Query parameters:
- `head`: number of lines to fetch from the beginning of the file.
  Default is 0, meaning to fetch the entire file.
- `tail`: number of lines to fetch from the end of the file.
  Default is 0, meaning to fetch the entire file.
- `max_line_length`: maximum number of characters to fetch from each line,
  if either `head` or `tail` is specified. Default is 128.
- 

### `PUT /bundles/<uuid:re:0x[0-9a-f]{32}>/contents/blob/`

Update the contents of the given running or uploading bundle.

Query parameters:
    urls - comma-separated list of URLs from which to fetch data to fill the
           bundle, using this option will ignore any uploaded file data
    git - (optional) 1 if URL should be interpreted as git repos to clone
          or 0 otherwise, default is 0
OR
    filename - (optional) filename of the uploaded file, used to indicate
               whether or not it is an archive, default is 'contents'

Query parameters that are always available:
    unpack - (optional) 1 if the uploaded file should be unpacked if it is
             an archive, or 0 otherwise, default is 1
    simplify - (optional) 1 if the uploaded file should be 'simplified' if
               it is an archive, or 0 otherwise, default is 1
               (See UploadManager for full explanation of 'simplification')
    finalize_on_failure - (optional) True ('1') if bundle state should be set
                          to 'failed' in the case of a failure during upload,
                          or False ('0') if the bundle state should not
                          change on failure. Default is False.
    state_on_success - (optional) Update the bundle state to this state if
                       the upload completes successfully. Must be either
                       'ready' or 'failed'. Default is 'ready'.


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
## Help API
### `POST /help/`

&uarr; [Back to Top](#table-of-contents)
