from contextlib import closing
import httplib
import json

from bottle import abort, local, post, request

from codalab.server.authenticated_plugin import AuthenticatedPlugin


@post('/worker/<worker_id>/checkin',
      apply=AuthenticatedPlugin())
def checkin(worker_id):
    """
    Checks in with the bundle service, storing information about the worker.
    Waits for a message for the worker for WAIT_TIME_SECS seconds. Returns the
    message or None if there isn't one.
    """
    WAIT_TIME_SECS = 2.0

    socket_id = local.worker_model.worker_checkin(
        request.user.user_id, worker_id,
        request.json['slots'], request.json['dependency_uuids'])
    with closing(local.worker_model.start_listening(socket_id)) as sock:
        return local.worker_model.get_json_message(sock, WAIT_TIME_SECS)


@post('/worker/<worker_id>/checkout',
      apply=AuthenticatedPlugin())
def checkout(worker_id):
    """
    Checks out from the bundle service, cleaning up any state related to the
    worker.
    """
    local.worker_model.worker_cleanup(request.user.user_id, worker_id)


def check_reply_permission(worker_id, socket_id):
    """
    Checks if the authenticated user running a worker with the given ID can
    reply to messages on the given socket ID.
    """
    if not local.worker_model.has_reply_permission(request.user.user_id, worker_id, socket_id):
        abort(httplib.FORBIDDEN, 'Not your socket ID!')


@post('/worker/<worker_id>/reply/<socket_id:int>',
      apply=AuthenticatedPlugin())
def reply(worker_id, socket_id):
    """
    Replies with a single JSON message to the given socket ID.
    """
    check_reply_permission(worker_id, socket_id)
    local.worker_model.send_json_message(socket_id, request.json, 60, autoretry=False)


@post('/worker/<worker_id>/reply_data/<socket_id:int>',
      apply=AuthenticatedPlugin())
def reply_data(worker_id, socket_id):
    """
    Replies with a stream of data to the given socket ID. This reply mechanism
    works through 2 messages sent by this method: the first message is a header
    message containing metadata. The second message streams the actual data in.

    The contents of the first message are parsed from the header_message query
    parameter, which should be in JSON format.

    The contents of the second message go in the body of the HTTP request.
    """
    if not 'header_message' in request.query:
        abort(httplib.BAD_REQUEST, 'Missing header message.')

    try:
        header_message = json.loads(request.query.header_message)
    except ValueError:
        abort(httplib.BAD_REQUEST, 'Header message should be in JSON format.') 

    check_reply_permission(worker_id, socket_id)
    local.worker_model.send_json_message(socket_id, header_message, 60, autoretry=False)
    local.worker_model.send_stream(socket_id, request['wsgi.input'], 60)
