# Worksheets API
&larr; [Back to Table of Contents](index.md)
## `GET /worksheets/<uuid:re:0x[0-9a-f]{32}>`
## `GET /worksheets`

Fetch bundles by bundle specs OR search keywords.

## `POST /worksheets`
## `POST /worksheets/<uuid:re:0x[0-9a-f]{32}>/raw`
## `PATCH /worksheets`

Bulk update worksheets metadata.

## `DELETE /worksheets`

Delete the bundles specified.
If |force|, allow deletion of bundles that have descendants or that appear across multiple worksheets.
If |recursive|, add all bundles downstream too.
If |data-only|, only remove from the bundle store, not the bundle metadata.
If |dry-run|, just return list of bundles that would be deleted, but do not actually delete.

## `GET /worksheets/sample/`

Get worksheets to display on the front page.
Keep only |worksheet_uuids|.

## `GET /worksheets/`
&larr; [Back to Table of Contents](index.md)
