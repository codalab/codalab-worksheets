from __future__ import absolute_import  # Without this line "from worker.worker import VERSION" doesn't work.
from contextlib import closing
import httplib
import json
import os
import subprocess
import time

from bottle import abort, get, local, post, put, request, response

from codalab.lib import spec_util
from codalab.objects.permission import check_bundle_have_run_permission
from codalab.server.authenticated_plugin import AuthenticatedPlugin
from codalabworker.worker import VERSION


@post('/workers/<worker_id>/checkin',
      name='worker_checkin', apply=AuthenticatedPlugin())
def checkin(worker_id):
    """
    Checks in with the bundle service, storing information about the worker.
    Waits for a message for the worker for WAIT_TIME_SECS seconds. Returns the
    message or None if there isn't one.
    """
    WAIT_TIME_SECS = 5.0

    torque_worker = ('torque' in local.config['workers'] and
                     request.user.user_id == local.model.root_user_id)
    if (not torque_worker and
        request.json['version'] != VERSION and
        not request.json['will_upgrade']):
        return {'type': 'upgrade'}

    socket_id = local.worker_model.worker_checkin(
        request.user.user_id, worker_id, request.json['tag'],
        request.json['slots'], request.json['cpus'], request.json['gpus'], request.json['memory_bytes'],
        request.json['dependencies'])
    with closing(local.worker_model.start_listening(socket_id)) as sock:
        return local.worker_model.get_json_message(sock, WAIT_TIME_SECS)


@post('/workers/<worker_id>/checkout',
      name='worker_checkout', apply=AuthenticatedPlugin())
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


@post('/workers/<worker_id>/reply/<socket_id:int>',
      name='worker_reply_json', apply=AuthenticatedPlugin())
def reply(worker_id, socket_id):
    """
    Replies with a single JSON message to the given socket ID.
    """
    check_reply_permission(worker_id, socket_id)
    local.worker_model.send_json_message(socket_id, request.json, 60, autoretry=False)


@post('/workers/<worker_id>/reply_data/<socket_id:int>',
      name='worker_reply_blob', apply=AuthenticatedPlugin())
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


def check_run_permission(bundle):
    """
    Checks whether the current user can run the bundle.
    """
    if not check_bundle_have_run_permission(local.model, request.user.user_id, bundle):
        abort(httplib.FORBIDDEN, 'User does not have permission to run bundle.')


@post('/workers/<worker_id>/start_bundle/<uuid:re:%s>' % spec_util.UUID_STR,
      name='worker_start_bundle', apply=AuthenticatedPlugin())
def start_bundle(worker_id, uuid):
    """
    Checks whether the bundle is still assigned to run on the worker with the
    given ID. If so, reports that it's starting to run and returns True.
    Otherwise, returns False, meaning the worker shouldn't run the bundle.
    """
    bundle = local.model.get_bundle(uuid)
    check_run_permission(bundle)
    response.content_type = 'application/json'
    if local.model.start_bundle(bundle, request.user.user_id, worker_id,
                                request.json['hostname'],
                                request.json['start_time']):
        print 'Started bundle %s' % uuid
        return json.dumps(True)
    return json.dumps(False)


@put('/workers/<worker_id>/update_bundle_metadata/<uuid:re:%s>' % spec_util.UUID_STR,
     name='worker_update_bundle_metadata', apply=AuthenticatedPlugin())
def update_bundle_metadata(worker_id, uuid):
    """
    Updates metadata related to a running bundle.
    """
    bundle = local.model.get_bundle(uuid)
    check_run_permission(bundle)
    allowed_keys = set(['run_status', 'time', 'time_user', 'time_system', 'memory', 'memory_max', 'data_size', 'last_updated', 'docker_image'])
    metadata_update = {}
    for key, value in request.json.iteritems():
        if key in allowed_keys:
            metadata_update[key] = value
    local.model.update_bundle(bundle, {'metadata': metadata_update})


@post('/workers/<worker_id>/finalize_bundle/<uuid:re:%s>' % spec_util.UUID_STR,
      name='worker_finalize_bundle', apply=AuthenticatedPlugin())
def finalize_bundle(worker_id, uuid):
    """
    Reports that the bundle has finished running.
    """
    bundle = local.model.get_bundle(uuid)
    check_run_permission(bundle)

    if (local.worker_model.shared_file_system and
        request.user.user_id == local.model.root_user_id):
        # On a shared file system, the worker doesn't upload the contents, so
        # we need to run a metadata update here. With no shared file system
        # it happens in update_bundle_contents.

        # On the NFS file system the contents of directories are cached, so the
        # new directory that has been created for the bundle might not appear
        # right away. We use this loop to check for it. There might be some
        # inconsistencies in the actual contents due to newly created files not
        # be seen. Although, that's very unlikely to give a large difference
        # in the final bundle size.
        bundle_location = local.bundle_store.get_bundle_location(uuid)
        for _ in xrange(120):
            if os.path.exists(bundle_location):
                break
            else:
                time.sleep(1)
        # If the directory still doesn't exist after 2 minutes, the following
        # call will return an error.

        local.upload_manager.update_metadata_and_save(bundle, new_bundle=False)

    print 'Finalized bundle %s' % uuid
    local.model.finalize_bundle(bundle, request.user.user_id,
                                request.json['exitcode'],
                                request.json['failure_message'])


@get('/workers/code.tar.gz', name='worker_download_code')
def code():
    """
    Returns .tar.gz archive containing the code of the worker.
    """
    response.set_header('Content-Disposition', 'filename="code.tar.gz"')
    response.set_header('Content-Type', 'application/gzip')
    codalab_cli = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    code_dir = os.path.join(codalab_cli, 'worker', 'codalabworker')
    args = ['tar', 'czf', '-', '-C', code_dir]
    for filename in os.listdir(code_dir):
        if filename.endswith('.py') or filename.endswith('.sh'):
            args.append(filename)
    proc = subprocess.Popen(args, stdout=subprocess.PIPE)
    result = proc.stdout.read()
    return result
