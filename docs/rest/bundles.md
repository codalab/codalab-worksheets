# Bundles API
&larr; [Back to Table of Contents](index.md)
## `GET /bundles/<uuid:re:0x[0-9a-f]{32}>`
## `GET /bundles`

Fetch bundles by bundle specs OR search keywords.

## `POST /bundles`

Bulk create bundles.

|worksheet_uuid| - The parent worksheet of the bundle, add to this worksheet
                   if not detached or shadowing another bundle. Also used
                   to inherit permissions.
|shadow| - the uuid of the bundle to shadow
|detached| - True (&#039;1&#039;) if should not add new bundle to any worksheet,
             or False (&#039;0&#039;) otherwise. Default is False.

## `PATCH /bundles`

Bulk update bundles.

## `DELETE /bundles`

Delete the bundles specified.
If |force|, allow deletion of bundles that have descendants or that appear across multiple worksheets.
If |recursive|, add all bundles downstream too.
If |data-only|, only remove from the bundle store, not the bundle metadata.
If |dry-run|, just return list of bundles that would be deleted, but do not actually delete.

## `GET /bundles/<uuid:re:0x[0-9a-f]{32}>/contents/info/<path:path>`
## `GET /bundles/<uuid:re:0x[0-9a-f]{32}>/contents/info/`
## `GET /bundles/<uuid:re:0x[0-9a-f]{32}>/contents/blob/<path:path>`

API to download the contents of a bundle or a subpath within a bundle.

For directories this method always returns a tarred and gzipped archive of
the directory.

For files, if the request has an Accept-Encoding header containing gzip,
then the returned file is gzipped.

## `GET /bundles/<uuid:re:0x[0-9a-f]{32}>/contents/blob/`

API to download the contents of a bundle or a subpath within a bundle.

For directories this method always returns a tarred and gzipped archive of
the directory.

For files, if the request has an Accept-Encoding header containing gzip,
then the returned file is gzipped.

## `PUT /bundles/<uuid:re:0x[0-9a-f]{32}>/contents/blob/`

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

&larr; [Back to Table of Contents](index.md)
