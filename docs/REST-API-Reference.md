# REST API Reference

_version 0.5.22_

This reference and the REST API itself is still under heavy development and is
subject to change at any time. Feedback through our GitHub issues is appreciated!

## Table of Contents
- [Introduction](#introduction)
- [Resource Object Schemas](#resource-object-schemas)
- [API Endpoints](#api-endpoints)

# Introduction
We use the JSON API v1.0 specification with the Bulk extension.
- http://jsonapi.org/format/
- https://github.com/json-api/json-api/blob/9c7a03dbc37f80f6ca81b16d444c960e96dd7a57/extensions/bulk/index.md

The following specification will not provide the verbose JSON formats of the API requests and responses, as those can be
found in the JSON API specification. Instead:
- Each resource type below (worksheets, bundles, etc.) specifies a list of
  attributes with their respective data types and semantics, along with a list
  of relationships.
- Each API call specifies the HTTP request method, the endpoint URI, HTTP
  parameters with their respective data types and semantics, description of the
  API call.

## How we use JSON API

Using the JSON API specification allows us to avoid redesigning the wheel when designing our request and response
formats. This also means that we will not specify all the details of the API (for example, Content-Type headers, the
fact that POST requests should contain a single resource object, etc.) in this document at this time, while we may
choose to continue copying in more details as we go in the design and implementation process. However, since there are
many optional features of the JSON API specification, we will document on a best-effort basis the ways in which we will
use the specification that are specific to our API, as well as which parts of the specification we use, and which parts
we do not.

## Top-level JSON structure

Every JSON request or response will have at its root a JSON object containing either a “data” field or an “error” field,
but not both. Thus the presence of an “error” field will unambiguously indicate an error state.

Response documents may also contain a top-level "meta" field, containing additional constructed data that are not
strictly resource objects, such as summations in a search query.

Response documents may also contain a top-level "included" field, discussed below.

## Primary Data

The JSON API standard specifies that the “data” field will contain either a [resource object]
(http://jsonapi.org/format/#document-resource-objects) or an array of resource objects, depending on the nature of the
request. More specfically, if the client is fetching a single specific resource (e.g. GET /bundles/0x1d09b495), the
“data” field will have a single JSON object at its root. If the client intends to fetch a variable number of resources,
then the “data” field will have at its root an array of zero or more JSON objects.

The structure of a JSON response with a single resource object will typically look like this:

```
{
  "data": {
    "type": "bundles",
    "id": "0x1d09b495410249f89dee4465cd21d499",
    "attributes": {
      // ... this bundle's attributes
    },
    "relationships": {
      // ... this bundle's relationships
    }
  }
}
```

Note that we use UUIDs as the "id" of a resource when available (i.e. for worksheets, bundles, and groups) and some
other unique key for those resources to which we have not prescribed a UUID scheme.

For each of the resource types available in the Worksheets API, we define the schema for its **attributes**, as well as
list what **relationships** each instance may have defined. Relationships are analogous to relationships in relational
databases and ORMs—some may be to-one (such as the "owner" of a bundle) and some may be to-many (such as the
"permissions" of a bundle).

We will use the following subset of the relationship object schema for our Worksheets API (Orderly schema):

```
object {
  object {
    string related;   // URL to GET the related resource
  } links;
  object {            // used to identify resource in includes
    string type;      // type of the related resource
    string id;        // id of the related resource
  } data?;
}
```

## Query Parameters

The client may provide additional parameters for requests as query parameters in the request URL. Available parameters
will be listed under each API route. In general:
- Boolean query parameters are encoded as 1 for "true" and 0 for "false".
- Some query parameters take multiple values, which can be passed by simply listing
  the parameter multiple times in the query, e.g. `GET /bundles?keywords=hello&keywords=world`

## Includes

The client will often want to fetch data for resources related to the primary resource(s) in the same request: for
example, client may want to fetch a worksheet along with all of its items, as well as data about the bundles and
worksheets referenced in the items.

Currently, most of the API endpoints will include related resources automatically.
For example, fetching a "worksheet" will also include the "bundles" referenced
by the worksheet in the response. These related resource objects will be
included in an array in the top-level "included" field of the response object.

## Non-JSON API Endpoints

The JSON API specification is not all-encompassing, and there are some cases in our API that fall outside of the
specification. We will indicate this explicitly where it applies, and provide an alternative schema for the JSON format
where necessary.

## Authorization and Authentication

The Bundle Service also serves as an OAuth2 Provider.

All requests to protected resources on the Worksheets API must include a valid
OAuth bearer token in the HTTP headers:

    Authorization: Bearer xxxxtokenxxxx

If the token is expired, does not authorize the application to access the
target resource, or is otherwise invalid, the Bundle Service will respond with
a `401 Unauthorized` or `403 Forbidden` status.

# Resource Object Schemas

## users


Name | Type
--- | ---
`id` | String
`user_name` | String
`first_name` | String
`last_name` | String
`affiliation` | String
`url` | Url
`date_joined` | LocalDateTime
`email` | String
`notifications` | Integer
`time_quota` | Integer
`parallel_run_quota` | Integer
`time_used` | Integer
`disk_quota` | Integer
`disk_used` | Integer
`last_login` | LocalDateTime
`is_verified` | Boolean
`has_access` | Boolean

## users


Name | Type
--- | ---
`id` | String
`user_name` | String
`first_name` | String
`last_name` | String
`affiliation` | String
`url` | Url
`date_joined` | LocalDateTime
`email` | String
`notifications` | Integer
`time_quota` | Integer
`parallel_run_quota` | Integer
`time_used` | Integer
`disk_quota` | Integer
`disk_used` | Integer
`last_login` | LocalDateTime

## bundle-actions


Name | Type
--- | ---
`id` | Integer
`uuid` | String
`type` | String
`subpath` | String
`string` | String

## BundleDependencySchema


Plain (non-JSONAPI) Marshmallow schema for a single bundle dependency.
Not defining this as a separate resource with Relationships because we only
create a set of dependencies once at bundle creation.


Name | Type
--- | ---
`child_uuid` | String
`child_path` | String
`parent_uuid` | String
`parent_path` | String
`parent_name` | Method

## bundle-permissions


Name | Type
--- | ---
`id` | CompatibleInteger
`bundle` | Relationship([bundles](#bundles))
`group` | Relationship([groups](#groups))
`group_name` | String
`permission` | Integer
`permission_spec` | PermissionSpec

## bundles


Name | Type
--- | ---
`id` | String
`uuid` | String
`bundle_type` | String
`command` | String
`data_hash` | String
`state` | String
`owner` | Relationship([users](#users))
`is_anonymous` | Boolean
`metadata` | Dict
`dependencies` | [BundleDependencySchema](#BundleDependencySchema)
`children` | Relationship([bundles](#bundles))
`group_permissions` | Relationship([bundle-permissions](#bundle-permissions))
`host_worksheets` | Relationship([worksheets](#worksheets))
`args` | String
`permission` | Integer
`permission_spec` | PermissionSpec

## groups


Name | Type
--- | ---
`id` | String
`name` | String
`user_defined` | Boolean
`owner` | Relationship([users](#users))
`admins` | Relationship([users](#users))
`members` | Relationship([users](#users))

## users


Name | Type
--- | ---
`id` | String
`user_name` | String
`first_name` | String
`last_name` | String
`affiliation` | String
`url` | Url
`date_joined` | LocalDateTime

## worksheet-items


Name | Type
--- | ---
`id` | CompatibleInteger
`worksheet` | Relationship([worksheets](#worksheets))
`subworksheet` | Relationship([worksheets](#worksheets))
`bundle` | Relationship([bundles](#bundles))
`value` | String
`type` | String
`sort_key` | Integer

## worksheet-permissions


Name | Type
--- | ---
`id` | CompatibleInteger
`worksheet` | Relationship([worksheets](#worksheets))
`group` | Relationship([groups](#groups))
`group_name` | String
`permission` | Integer
`permission_spec` | PermissionSpec

## worksheets


Name | Type
--- | ---
`id` | String
`uuid` | String
`name` | String
`owner` | Relationship([users](#users))
`title` | String
`frozen` | DateTime
`is_anonymous` | Boolean
`tags` | List
`group_permissions` | Relationship([worksheet-permissions](#worksheet-permissions))
`items` | Relationship([worksheet-items](#worksheet-items))
`last_item_id` | Integer
`permission` | Integer
`permission_spec` | PermissionSpec

&uarr; [Back to Top](#table-of-contents)
# API Endpoints
