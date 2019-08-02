

The CodaLab REST API is still under heavy development, and interfaces may change at any time.

# Authentication

We use [OAuth 2.0](https://oauth.net/2/) for authentication and authorization.

All API requests to protected resources must include a valid OAuth bearer token as a HTTP header:
```
GET /rest/bundles/0x491808200c7e4e2798530d5cf9bdfdd HTTP/1.1
Content-Type: application/json
Authorization: Bearer WM0vteAcGpPhXEGtKmhCGjgVIOvYbB
```

Unauthorized requests will receive a `403 Forbidden` HTTP response.


## Resource Owner Password Credentials Grant

The easiest way to get an OAuth token is by requesting a token directly using a username and password.

### `POST /rest/oauth2/token`
```
Content-Type: application/x-www-form-urlencoded
Authorization: Basic Y29kYWxhYl9jbGlfY2xpZW50Og==
```

| Form Parameter    | Description |
| :---         |      :---      |
| `grant_type` | REQUIRED. Value MUST be set to `password`.                      | 
| `username`   | REQUIRED. The resource owner username or email address.         | 
| `password`   | REQUIRED. The resource owner password.                          | 
| `scope`      | OPTIONAL. Defaults to `default`, which provides full access to the user's resources. No other scopes are currently supported, though we may introduce them in the future.   | 

Example response:
```
{
    "access_token": "NT4xa9noJkBQMxwoL8AikZ6wuGDlUQ",
    "token_type": "Bearer",
    "expires_in": 3600,
    "refresh_token": "HWTsWyqTXmVJuran04OhyXua3k8VlL",
    "scope": "default"
}
```

# Resources

We follow the [JSON API](jsonapi.org) v1.0 specification for the most part, with additional support for bulk operations based on an [unofficial Bulk extension](https://github.com/json-api/json-api/blob/9c7a03dbc37f80f6ca81b16d444c960e96dd7a57/extensions/bulk/index.md).

Some complete examples will be provided to illustrate how this translated into full JSON objects.

Boolean query parameters should be specified as `1` for true and `0` for false.

## Bundles

### Get bundle info
`GET /rest/bundles/<uuid>`

### Search bundles
`GET /rest/bundles/`

| Query Parameter    | Description |
| :---         |      :---      |
| `keywords`   | MULTIPLE. Keywords for a search query, in the same form as in `cl search`.                 | 
| `specs  `    | MULTIPLE. Bundle specs to search for.         | 
| `worksheet`  | OPTIONAL. ID of the parent worksheet for resolving bundle specs.                    | 
| `depth`      | OPTIONAL. Include all descendants of the found bundles down by this depth. | 

### Create bundles
`POST /rest/bundles`

### Update bundles
`PATCH /rest/bundles`

### Delete bundles
`DELETE /rest/bundles`

| Query Parameter    | Description |
| :---         |      :---      |
| `force=1`   | OPTIONAL. Allow deletion of bundles that have descendants or that appear across multiple worksheets.       | 
| `recursive=1`    | OPTIONAL. Delete all bundles downstream of the specified bundles too.         | 
| `data-only=1`  | OPTIONAL. Only delete the bundle contents from the bundle store, but keep the bundle metadata. |
| `dry-run=1`      | OPTIONAL. Just return list of bundles that would be deleted, but do not actually delete. | 

Request Example:
```
{
    "data": [
        {"type": "bundles", "id": "0x491808200c7e4e2798530d5cf9bdfdd1"},
        {"type": "bundles", "id": "0xfbf4b487726248bcbe349932ca6981a3"},
     ]
}
```

Response Example:
```
{
    "meta": {
        "version": "0.2.0",
        "ids": ["0x491808200c7e4e2798530d5cf9bdfdd1", "0xfbf4b487726248bcbe349932ca6981a3"]
    }
}
```

## Bundle Contents

### Fetch Contents Metadata (non-JSON API)
`GET /rest/bundles/<uuid>/contents/info/`

| Query Parameter    | Description |
| :---         |      :---      |
| `depth`   | OPTIONAL. Depth to traverse directory tree.   | 

Example request:
```
GET /rest/bundles/0x97e9d4bdbecd4a969a7f9d41e2f5dd9c/contents/info/?depth=1
```

Example response body:
```
{
    "meta": {
        "version": "0.2.0"
    },
    "data": {
        "contents": [
            {
                "type": "file",
                "name": "a.txt",
                "perm": 420,
                "size": 64
            },
            {
                "type": "file",
                "name": "b.txt",
                "perm": 420,
                "size": 22
            },
        ],
        "type": "directory",
        "name": "0x97e9d4bdbecd4a969a7f9d41e2f5dd9c",
        "perm": 493,
        "size": 408
    }
}
```

### Fetch Contents Blob (non-JSON API)
`GET /rest/bundles/<uuid>/contents/blob/`

Returns the raw data stream containing the contents of the bundle. If the bundle is a directory, the contents will be a tarred and gzipped archive.

### Upload Contents Blob (non-JSON API)
`PUT /rest/bundles/<uuid>/contents/blob/`

Set or replace the contents of the bundle with the file uploaded in the request body. Supports chunked encoding. Directories should be uploaded as tarred+gzipped or zip archives.

## Bundle Permissions

### Set bundle permissions
`POST /rest/bundle-permissions`

## Bundle Actions

### Queue bundle action
`POST /rest/bundle-actions`


## Worksheets

### Fetch worksheet info
`GET /rest/worksheets/<uuid>`

### Update worksheet with raw lines
`POST /rest/worksheets/<uuid>/raw`

### Search worksheets
`GET /rest/worksheets/`

### Create worksheets
`POST /rest/worksheets/`

### Update worksheet metadata
`PATCH /rest/worksheets/`

### Delete worksheets
`DELETE /rest/worksheets/`

### Add worksheet items
`POST /rest/worksheets-items`

### Set worksheet permissions
`POST /rest/worksheets-permissions`

## Users

### Get authenticated user
`GET /rest/user`

### Update authenticated user
`PATCH /rest/user`

### Lookup user
`GET /rest/users/<user_spec>`

### Lookup users
`GET /rest/users`

## Groups

### Lookup group
`GET /rest/groups/<group_spec>`

### Lookup groups
`GET /rest/groups`

Fetch all groups accessible by the authenticated user.

### Delete group
`DELETE /rest/groups/<group_spec>`

### Create group
`POST /rest/groups`

### Add admin to group
`POST /rest/groups/<group_spec>/relationships/admins`

### Add normal member to group
`POST /rest/groups/<group_spec>/relationships/members`

### Delete members from group
`DELETE /rest/groups/<group_spec>/relationships/members`