# Workers API
&larr; [Back to Table of Contents](index.md)
## `POST /workers/<worker_id>/checkin`

Checks in with the bundle service, storing information about the worker.
Waits for a message for the worker for WAIT_TIME_SECS seconds. Returns the
message or None if there isn&#039;t one.

## `POST /workers/<worker_id>/checkout`

Checks out from the bundle service, cleaning up any state related to the
worker.

## `POST /workers/<worker_id>/reply/<socket_id:int>`

Replies with a single JSON message to the given socket ID.

## `POST /workers/<worker_id>/reply_data/<socket_id:int>`

Replies with a stream of data to the given socket ID. This reply mechanism
works through 2 messages sent by this method: the first message is a header
message containing metadata. The second message streams the actual data in.

The contents of the first message are parsed from the header_message query
parameter, which should be in JSON format.

The contents of the second message go in the body of the HTTP request.

## `POST /workers/<worker_id>/start_bundle/<uuid:re:0x[0-9a-f]{32}>`

Checks whether the bundle is still assigned to run on the worker with the
given ID. If so, reports that it&#039;s starting to run and returns True.
Otherwise, returns False, meaning the worker shouldn&#039;t run the bundle.

## `PUT /workers/<worker_id>/update_bundle_metadata/<uuid:re:0x[0-9a-f]{32}>`

Updates metadata related to a running bundle.

## `POST /workers/<worker_id>/finalize_bundle/<uuid:re:0x[0-9a-f]{32}>`

Reports that the bundle has finished running.

## `GET /workers/code.tar.gz`

Returns .tar.gz archive containing the code of the worker.

&larr; [Back to Table of Contents](index.md)
