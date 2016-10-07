from bottle import local, post, request
from marshmallow import validate
from marshmallow_jsonapi import Schema, fields

from codalab.common import State, UsageError, precondition
from codalab.lib.bundle_action import BundleAction
from codalab.lib.spec_util import validate_child_path, validate_uuid
from codalab.objects.permission import check_bundles_have_all_permission
from codalab.server.authenticated_plugin import AuthenticatedPlugin


class BundleActionSchema(Schema):
    id = fields.Integer(dump_only=True, default=None)
    uuid = fields.String(validate=validate_uuid)
    type = fields.String(validate=validate.OneOf({BundleAction.KILL, BundleAction.WRITE}))
    subpath = fields.String(validate=validate_child_path)
    string = fields.String()

    class Meta:
        type_ = 'bundle-actions'


@post('/bundle-actions', apply=AuthenticatedPlugin())
def create_bundle_actions():
    """
    Sends the message to the worker to do the bundle action, and adds the
    action string to the bundle metadata.
    """
    actions = BundleActionSchema(
        strict=True, many=True,
    ).load(request.json).data

    check_bundles_have_all_permission(local.model, request.user, [a['uuid'] for a in actions])

    for action in actions:
        bundle = local.model.get_bundle(action['uuid'])
        if bundle.state != State.RUNNING:
            raise UsageError('Cannot execute this action on a bundle that is not running.')

        worker = local.worker_model.get_bundle_worker(action['uuid'])
        precondition(
            local.worker_model.send_json_message(worker['socket_id'], action, 60),
            'Unable to reach worker.')

        new_actions = getattr(bundle.metadata, 'actions', []) + [BundleAction.as_string(action)]
        db_update = {'metadata': {'actions': new_actions}}
        local.model.update_bundle(bundle, db_update)

    return BundleActionSchema(many=True).dump(actions).data
