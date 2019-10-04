from __future__ import (
    absolute_import,
)  # Without this line "from worker.worker import VERSION" doesn't work.
from contextlib import closing
import http.client
import json
import os
import subprocess
from datetime import datetime

from bottle import abort, get, local, post, put, request, response

from codalab.lib import spec_util
from codalab.objects.permission import check_bundle_have_run_permission
from codalab.server.authenticated_plugin import AuthenticatedPlugin


@post("/workers/<worker_id>/checkin", name="worker_checkin", apply=AuthenticatedPlugin())
def checkin(worker_id):
    """
    Checks in with the bundle service, storing information about the worker.
    Waits for a message for the worker for WAIT_TIME_SECS seconds. Returns the
    message or None if there isn't one.
    """
    WAIT_TIME_SECS = 3.0

    # Old workers might not have all the fields, so allow subsets to be
    # missing.
    socket_id = local.worker_model.worker_checkin(
        request.user.user_id,
        worker_id,
        request.json.get("tag"),
        request.json.get("cpus"),
        request.json.get("gpus"),
        request.json.get("memory_bytes"),
        request.json.get("free_disk_bytes"),
        request.json["dependencies"],
    )

    for uuid, run in request.json["runs"].items():
        try:
            bundle = local.model.get_bundle(uuid)
            local.model.bundle_checkin(bundle, run, request.user.user_id, worker_id)
        except Exception:
            pass

    with closing(local.worker_model.start_listening(socket_id)) as sock:
        return local.worker_model.get_json_message(sock, WAIT_TIME_SECS)


def check_reply_permission(worker_id, socket_id):
    """
    Checks if the authenticated user running a worker with the given ID can
    reply to messages on the given socket ID.
    """
    if not local.worker_model.has_reply_permission(request.user.user_id, worker_id, socket_id):
        abort(http.client.FORBIDDEN, "Not your socket ID!")


@post(
    "/workers/<worker_id>/reply/<socket_id:int>",
    name="worker_reply_json",
    apply=AuthenticatedPlugin(),
)
def reply(worker_id, socket_id):
    """
    Replies with a single JSON message to the given socket ID.
    """
    check_reply_permission(worker_id, socket_id)
    local.worker_model.send_json_message(socket_id, request.json, 60, autoretry=False)


@post(
    "/workers/<worker_id>/reply_data/<socket_id:int>",
    name="worker_reply_blob",
    apply=AuthenticatedPlugin(),
)
def reply_data(worker_id, socket_id):
    """
    Replies with a stream of data to the given socket ID. This reply mechanism
    works through 2 messages sent by this method: the first message is a header
    message containing metadata. The second message streams the actual data in.

    The contents of the first message are parsed from the header_message query
    parameter, which should be in JSON format.

    The contents of the second message go in the body of the HTTP request.
    """
    if "header_message" not in request.query:
        abort(http.client.BAD_REQUEST, "Missing header message.")

    try:
        header_message = json.loads(request.query.header_message)
    except ValueError:
        abort(http.client.BAD_REQUEST, "Header message should be in JSON format.")

    check_reply_permission(worker_id, socket_id)
    local.worker_model.send_json_message(socket_id, header_message, 60, autoretry=False)
    local.worker_model.send_stream(socket_id, request["wsgi.input"], 60)


def check_run_permission(bundle):
    """
    Checks whether the current user can run the bundle.
    """
    if not check_bundle_have_run_permission(local.model, request.user.user_id, bundle):
        abort(http.client.FORBIDDEN, "User does not have permission to run bundle.")


@post(
    "/workers/<worker_id>/start_bundle/<uuid:re:%s>" % spec_util.UUID_STR,
    name="worker_start_bundle",
    apply=AuthenticatedPlugin(),
)
def start_bundle(worker_id, uuid):
    """
    Checks whether the bundle is still assigned to run on the worker with the
    given worker_id. If so, reports that it's starting to run and returns True.
    Otherwise, returns False, meaning the worker shouldn't run the bundle.
    """
    bundle = local.model.get_bundle(uuid)
    check_run_permission(bundle)
    response.content_type = "application/json"
    if local.model.transition_bundle_preparing(
        bundle,
        request.user.user_id,
        worker_id,
        start_time=request.json["start_time"],
        remote=request.json["hostname"],
    ):
        print("Started bundle %s" % uuid)
        return json.dumps(True)
    return json.dumps(False)


@get("/workers/info", name="workers_info", apply=AuthenticatedPlugin())
def workers_info():
    if request.user.user_id != local.model.root_user_id:
        abort(http.client.UNAUTHORIZED, "User is not root user")

    data = local.worker_model.get_workers()

    # edit entries in data to make them suitable for human reading
    for worker in data:
        # checkin_time: seconds since epoch
        worker["checkin_time"] = int(
            (worker["checkin_time"] - datetime.utcfromtimestamp(0)).total_seconds()
        )
        del worker["dependencies"]

        running_bundles = local.model.batch_get_bundles(uuid=worker["run_uuids"])
        worker["cpus_in_use"] = sum(bundle.metadata.request_cpus for bundle in running_bundles)
        worker["gpus_in_use"] = sum(bundle.metadata.request_gpus for bundle in running_bundles)

    return {"data": data}
