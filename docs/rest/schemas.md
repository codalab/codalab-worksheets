&larr; [Back to Table of Contents](index.md)
# worksheet-items


Name | Type
--- | ---
`subworksheet` | Relationship
`sort_key` | Integer
`worksheet` | Relationship
`bundle` | Relationship
`value` | String
`type` | String
`id` | Integer
# users


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
# bundles


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
`dependencies` | Nested
`command` | String
`owner` | Relationship
`bundle_type` | String
`children` | Relationship
`permission_spec` | PermissionSpec
`metadata` | Dict
# worksheet-permissions


Name | Type
--- | ---
`group` | Relationship
`permission` | Integer
`worksheet` | Relationship
`group_name` | String
`id` | Integer
`permission_spec` | PermissionSpec
# bundle-permissions


Name | Type
--- | ---
`group` | Relationship
`permission` | Integer
`bundle` | Relationship
`group_name` | String
`id` | Integer
`permission_spec` | PermissionSpec
# BundleDependencySchema


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
# bundle-actions


Name | Type
--- | ---
`type` | String
`uuid` | String
`subpath` | String
`string` | String
`id` | Integer
# worksheets


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
# groups


Name | Type
--- | ---
`name` | String
`user_defined` | Boolean
`admins` | Relationship
`members` | Relationship
`owner` | Relationship
`id` | String
# users


Name | Type
--- | ---
`first_name` | String
`last_name` | String
`url` | Url
`affiliation` | String
`user_name` | String
`id` | String
`date_joined` | LocalDateTime
&larr; [Back to Table of Contents](index.md)
